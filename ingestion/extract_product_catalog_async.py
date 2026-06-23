"""
ENTERPRISE PIPELINE: extract_product_info.py
Architecture: Producer-Consumer, MongoDB Aggregation, Async File I/O
Complies strictly with "Product information collection (8 hours)" task.
"""

import asyncio
import json
import logging
import os
import random
import signal
import time
from typing import Optional, Dict

import aiofiles
from curl_cffi.requests import AsyncSession
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne, IndexModel, ASCENDING
from pymongo.errors import BulkWriteError
from pydantic import BaseModel, Field, field_validator, ValidationError
from selectolax.parser import HTMLParser

# ==========================================
# 1. SETTINGS & CONFIGURATIONS
# ==========================================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018/?authSource=admin")
DB_NAME = "countly" 
OUTPUT_FILE = "product_data_extracted.jsonl"

CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", "5"))
MAX_RETRIES = 3
CIRCUIT_BREAKER_LIMIT = 15 # Shutdown if 15 consecutive 403s happen

BROWSER_PROFILES = ["chrome110", "chrome120", "edge99", "safari15_5"]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

shutdown_event = asyncio.Event()
consecutive_fails = 0

# ==========================================
# 2. STRICT SCHEMA VALIDATION
# ==========================================
class ProductSchema(BaseModel):
    """Validate data. Drop it if it is missing important fields."""
    product_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    price: float = Field(default=0.0)
    currency: str = Field(default="USD")
    url: str = Field(min_length=10)
    in_stock: bool = Field(default=False)

    @field_validator("price", mode="before")
    def clean_price(cls, v):
        if isinstance(v, (int, float)): return float(v)
        if isinstance(v, str):
            clean_str = "".join(c for c in v if c.isdigit() or c == ".")
            return float(clean_str) if clean_str else 0.0
        return 0.0

    @field_validator("name")
    def clean_name(cls, v):
        return " ".join(v.strip().split())

# ==========================================
# 3. QUEUE BUILDER (MONGODB AGGREGATION)
# ==========================================
async def build_queue_from_events(db):
    """
    Follows assignment requirements:
    1. Filter data from collections -> retrieve product_id and current_url / referrer_url
    2. Get ONLY ONE active product information for each distinct product_id.
    """
    logging.info("Step 1: Building distinct product queue from event collections...")
    
    group_1_collections = [
        "view_product_detail", "select_product_option", 
        "select_product_option_quality", "add_to_cart_action", 
        "product_detail_recommendation_visible", "product_detail_recommendation_noticed"
    ]
    
    # Create indexes for the Queue
    await db.scrape_queue.create_index([("status", 1), ("next_retry", 1), ("retry_count", 1)])
    await db.scrape_queue.create_index("product_id", unique=True)

    for coll in group_1_collections:
        logging.info(f"Scanning and creating indexes for collection: {coll}...")
        # Prevent CPU spike (Full Collection Scan) by ensuring indexes exist before aggregating
        await db[coll].create_index([("product_id", ASCENDING), ("current_url", ASCENDING)], background=True)
        await db[coll].create_index([("viewing_product_id", ASCENDING)], background=True)

        pipeline = [
            {"$project": {
                "pid": {"$ifNull": ["$product_id", "$viewing_product_id"]},
                "url": "$current_url"
            }},
            {"$match": {"pid": {"$exists": True, "$ne": None}, "url": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$pid", "url": {"$first": "$url"}}},
            {"$project": {
                "_id": 0, "product_id": "$_id", "url": 1, 
                "status": "PENDING", "retry_count": 0, "next_retry": 0
            }},
            {"$merge": {"into": "scrape_queue", "on": "product_id", "whenMatched": "keepExisting", "whenNotMatched": "insert"}}
        ]
        try:
            await db[coll].aggregate(pipeline).to_list(None)
        except Exception as e:
            logging.error(f"Cannot aggregate {coll}: {e}")

    logging.info("Scanning collection: product_view_all_recommend_clicked...")
    await db["product_view_all_recommend_clicked"].create_index([("viewing_product_id", ASCENDING), ("referrer_url", ASCENDING)], background=True)
    
    pipeline_2 = [
        {"$project": {"pid": "$viewing_product_id", "url": "$referrer_url"}},
        {"$match": {"pid": {"$exists": True, "$ne": None}, "url": {"$exists": True, "$ne": None}}},
        {"$group": {"_id": "$pid", "url": {"$first": "$url"}}},
        {"$project": {
            "_id": 0, "product_id": "$_id", "url": 1, 
            "status": "PENDING", "retry_count": 0, "next_retry": 0
        }},
        {"$merge": {"into": "scrape_queue", "on": "product_id", "whenMatched": "keepExisting", "whenNotMatched": "insert"}}
    ]
    try:
        await db["product_view_all_recommend_clicked"].aggregate(pipeline_2).to_list(None)
    except Exception as e:
        logging.error(f"Cannot aggregate recommendation collection: {e}")

    total_distinct = await db.scrape_queue.count_documents({})
    logging.info(f"Queue build complete! Found {total_distinct} distinct products.")

# ==========================================
# 4. CPU-BOUND HTML PARSER
# ==========================================
def extract_product_data(html_text: str, source_url: str, product_id: str) -> Optional[dict]:
    """Parse HTML quickly and clean data using Pydantic."""
    try:
        tree = HTMLParser(html_text)
        
        # Check for Akamai Soft Block
        title = tree.css_first("title")
        if title and ("Access Denied" in title.text() or "Security" in title.text()):
            return {"error": "AKAMAI_SOFT_BLOCK"}

        for node in tree.css('script[type="application/ld+json"]'):
            content = node.text(strip=True)
            if not content: continue
            try:
                data = json.loads(content)
                items = data if isinstance(data, list) else data.get("@graph", [data])
                
                for item in items:
                    if isinstance(item, dict) and item.get("@type") == "Product":
                        offers = item.get("offers", {})
                        raw_data = {
                            "product_id": product_id,
                            "name": item.get("name", ""),
                            "price": offers.get("price", 0.0),
                            "currency": offers.get("priceCurrency", "USD"),
                            "url": item.get("url", source_url),
                            "in_stock": "InStock" in offers.get("availability", "")
                        }
                        clean_product = ProductSchema(**raw_data)
                        return {"data": clean_product.model_dump()}
            except (json.JSONDecodeError, ValidationError) as e:
                logging.debug(f"Data format error for {source_url}: {e}")
                continue
    except Exception as e:
        logging.error(f"HTML parsing failed: {e}")
        
    return {"error": "NO_JSON_LD"}

# ==========================================
# 5. ASYNC WORKER (CRAWLER)
# ==========================================
async def crawler_worker(task_queue: asyncio.Queue, result_queue: asyncio.Queue):
    """Worker fetching data using random profiles and dynamic timeouts."""
    global consecutive_fails
    
    session_requests = 0
    current_profile = random.choice(BROWSER_PROFILES)
    session = AsyncSession(impersonate=current_profile)

    while not shutdown_event.is_set():
        try:
            task = await task_queue.get()
        except asyncio.CancelledError:
            break

        if session_requests > 300:
            session.close()
            current_profile = random.choice(BROWSER_PROFILES)
            session = AsyncSession(impersonate=current_profile)
            session_requests = 0

        url = task["url"]
        
        try:
            # Human-like pacing
            jitter = random.uniform(2.0, 5.0)
            await asyncio.sleep(jitter)
            timeout_val = random.uniform(15.0, 30.0)
            
            response = await session.get(url, timeout=timeout_val)
            session_requests += 1

            if response.status_code == 200:
                consecutive_fails = 0 
                extract_result = await asyncio.to_thread(
                    extract_product_data, response.text, url, task["product_id"]
                )
                
                if "data" in extract_result:
                    await result_queue.put({"status": "SUCCESS", "task": task, "data": extract_result["data"]})
                else:
                    if extract_result.get("error") == "AKAMAI_SOFT_BLOCK":
                        consecutive_fails += 1
                        await result_queue.put({"status": "ANTI_BOT", "task": task})
                    else:
                        await result_queue.put({"status": "SCHEMA_ERROR", "task": task, "error": "Missing JSON-LD"})

            elif response.status_code in [403, 429]:
                consecutive_fails += 1
                logging.warning(f"Access blocked (HTTP {response.status_code}) at {url}")
                await result_queue.put({"status": "ANTI_BOT", "task": task})
                
                if consecutive_fails >= CIRCUIT_BREAKER_LIMIT:
                    logging.critical("CRITICAL: Akamai blocked our IP completely. Stopping the system!")
                    shutdown_event.set()

            elif response.status_code == 404:
                await result_queue.put({"status": "DEAD_LINK", "task": task})
            else:
                await result_queue.put({"status": "HTTP_ERROR", "task": task, "error": f"HTTP {response.status_code}"})

        except Exception as e:
            await result_queue.put({"status": "NETWORK_ERROR", "task": task, "error": str(e)})
            
        finally:
            task_queue.task_done()

    session.close()

# ==========================================
# 6. PIPELINE ORCHESTRATOR & FILE WRITER
# ==========================================
async def result_processor(result_queue: asyncio.Queue, db, total_tasks: int):
    """Processes results: writes to file (Single open) and updates MongoDB in bulk."""
    processed_count = 0
    success_count = 0
    fail_count = 0
    batch_ops = []
    
    # FIX: Open file exactly once per run to save Disk IOPS
    async with aiofiles.open(OUTPUT_FILE, "a", encoding="utf-8") as file_writer:
        
        while not shutdown_event.is_set() or not result_queue.empty():
            try:
                res = await asyncio.wait_for(result_queue.get(), timeout=2.0)
                processed_count += 1
                task = res["task"]
                status = res["status"]
                
                if status == "SUCCESS":
                    success_count += 1
                    # Append data line and flush to disk safely
                    await file_writer.write(json.dumps(res["data"], ensure_ascii=False) + "\n")
                    await file_writer.flush()
                    
                    batch_ops.append(UpdateOne({"_id": task["_id"]}, {"$set": {"status": "COMPLETED"}}))
                
                elif status == "DEAD_LINK":
                    fail_count += 1
                    batch_ops.append(UpdateOne({"_id": task["_id"]}, {"$set": {"status": "DEAD_LINK"}}))
                    
                else:
                    fail_count += 1
                    retries = task.get("retry_count", 0) + 1
                    if retries < MAX_RETRIES:
                        backoff_delay = (2 ** retries) * 15 + random.uniform(1.0, 5.0)
                        next_retry = time.time() + backoff_delay
                        batch_ops.append(UpdateOne(
                            {"_id": task["_id"]}, 
                            {"$set": {"retry_count": retries, "next_retry": next_retry, "last_error": res.get("error", status)}}
                        ))
                    else:
                        batch_ops.append(UpdateOne(
                            {"_id": task["_id"]}, 
                            {"$set": {"status": "FAILED_DLQ", "retry_count": retries}}
                        ))
                
                result_queue.task_done()

                if len(batch_ops) >= 50:
                    try:
                        await db.scrape_queue.bulk_write(batch_ops, ordered=False)
                    except BulkWriteError as bwe:
                        logging.error(f"Bulk write error (non-fatal): {bwe.details}")
                    batch_ops.clear()
                    
                    percent = (processed_count / total_tasks) * 100 if total_tasks > 0 else 0
                    logging.info(f"Progress: {processed_count}/{total_tasks} ({percent:.1f}%) | Success: {success_count} | Fail: {fail_count}")

            except asyncio.TimeoutError:
                if batch_ops:
                    try:
                        await db.scrape_queue.bulk_write(batch_ops, ordered=False)
                    except BulkWriteError:
                        pass
                    batch_ops.clear()

async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown_event.set)
        except NotImplementedError:
            pass 

    # FIX: Connection Pool limits to prevent Socket Exhaustion
    client = AsyncIOMotorClient(MONGO_URI, maxPoolSize=50, minPoolSize=10, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]

    await build_queue_from_events(db)

    task_queue = asyncio.Queue(maxsize=100)
    result_queue = asyncio.Queue()
    
    total_urls = await db.scrape_queue.count_documents({"status": "PENDING"})
    if total_urls == 0:
        logging.info("No PENDING tasks left. Everything is complete.")
        client.close()
        return

    logging.info(f"Starting async crawler with {CONCURRENCY_LIMIT} workers...")

    workers = [asyncio.create_task(crawler_worker(task_queue, result_queue)) for _ in range(CONCURRENCY_LIMIT)]
    processor = asyncio.create_task(result_processor(result_queue, db, total_urls))

    batch_count = 0

    while not shutdown_event.is_set():
        
        # =================================================================================
        # [MODIFY HERE FOR PRODUCTION] 
        # Add a '#' at the beginning of the next 3 lines to disable TEST MODE. 
        # Once commented out, the script will run until all IDs are completely fetched.
        # =================================================================================
        if batch_count >= 1:
            logging.info("TEST MODE: Finished the first batch of 50 IDs. Shutting down safely for inspection.")
            break

        cursor = db.scrape_queue.find(
            {"status": "PENDING", "next_retry": {"$lte": time.time()}}
        ).sort([("retry_count", 1), ("next_retry", 1)]).limit(50)
        
        tasks = await cursor.to_list(length=50)
        
        if not tasks:
            logging.info("Waiting for tasks to reach retry time...")
            await asyncio.sleep(10)
            continue
            
        for t in tasks:
            if shutdown_event.is_set(): break
            await db.scrape_queue.update_one({"_id": t["_id"]}, {"$set": {"status": "PROCESSING"}})
            await task_queue.put(t)
            
        batch_count += 1
            
        await asyncio.sleep(random.uniform(5.0, 15.0))

    logging.info("Shutdown signal received. Waiting for tasks to finish...")
    for w in workers: w.cancel()
    processor.cancel()
    client.close()
    logging.info("System closed safely.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        shutdown_event.set()