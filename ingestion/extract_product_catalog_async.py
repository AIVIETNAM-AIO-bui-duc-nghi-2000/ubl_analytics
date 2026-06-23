"""
PRODUCTION PIPELINE: extract_product_catalog_async.py
Description: Asynchronous web scraper to get product data using JSON-LD.
Features: 
- Deduplication via MongoDB 'scrape_queue'
- Asynchronous requests (curl_cffi) with concurrency limits (Semaphore)
- Akamai & Cloudflare Bypass via TLS Fingerprint Impersonation
- JSON-LD Schema Extraction (No CSS Selectors)
- Thread-safe file writing & Centralized Logging
"""

import os
import json
import time
import random
import asyncio
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler

from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne, ASCENDING
# Use curl_cffi instead of aiohttp to bypass Akamai/Cloudflare TLS fingerprinting
from curl_cffi.requests import AsyncSession

# ==========================================
# SYSTEM SETTINGS & CONFIGURATION
# ==========================================
# MongoDB connection via SSH tunnel on port 27018
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin_glamira:Th%40tIsMySecret2026%21@localhost:27018/?authSource=admin")
DB_NAME = "countly"
OUTPUT_FILE = "raw_product_info.jsonl"

# SAFE STEALTH CONFIGURATION (Scenario 2)
CONCURRENCY_LIMIT = 7  
MAX_RETRIES = 2 

# Domain Waterfall Strategy
ENGLISH_DOMAINS = [
    # Core English
    "www.glamira.com", "www.glamira.co.uk", "www.glamira.com.au", "www.glamira.ca", "www.glamira.ie", "www.glamira.co.nz",
    # Asia & Middle East
    "www.glamira.sg", "www.glamira.hk", "www.glamira.in", "www.glamira.com.ph", "www.glamira.com.my", 
    "www.glamira.ae", "www.glamira.co.id", "www.glamira.co.th", "www.glamira.com.kw",
    # Africa & Americas
    "www.glamira.co.za", "www.glamira.africa", "www.glamira.com.br", "www.glamira.cl", "www.glamira.com.mx"
]

# Lock to stop data errors when multiple tasks write to the file
file_write_lock = asyncio.Lock()

# ==========================================
# DYNAMIC DAILY LOGGING
# ==========================================
today_str = datetime.now().strftime("%Y-%m-%d")
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", today_str)
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("Extract_Product_Catalog")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Show log messages in the terminal window
console = logging.StreamHandler()
console.setFormatter(formatter)
logger.addHandler(console)

# Save logs to a file. Max size is 10MB, keep 5 old files.
file_h = RotatingFileHandler(
    os.path.join(LOG_DIR, "crawler_process.log"), 
    maxBytes=10*1024*1024, 
    backupCount=5, 
    encoding="utf-8"
)
file_h.setFormatter(formatter)
logger.addHandler(file_h)

# ==========================================
# PHASE 1: DATA DEDUPLICATION & QUEUE BUILDER
# ==========================================
def build_scrape_queue():
    """
    Reads the master summary collection, extracts unique product IDs,
    and saves them to the scrape queue to avoid duplicate work.
    """
    logger.info("PHASE 1: Building Target Queue from Master Summary Collection...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Create indexes for fast search and queue management
    db["scrape_queue"].create_index([("product_id", ASCENDING)], unique=True)
    db["scrape_queue"].create_index([("status", ASCENDING)])
    
    unique_ids = set()
    
    # Scan the master summary collection using a memory-safe projection
    logger.info("Scanning summary collection. This can take a few minutes...")
    cursor = db["summary"].find(
        {}, 
        {
            "product_id": 1, 
            "viewing_product_id": 1, 
            "segmentation.product_id": 1, 
            "segmentation.viewing_product_id": 1, 
            "_id": 0
        }
    )
    
    for doc in cursor:
        pid = doc.get("product_id") or doc.get("viewing_product_id")
        
        if not pid and "segmentation" in doc and isinstance(doc["segmentation"], dict):
            seg = doc["segmentation"]
            pid = seg.get("product_id") or seg.get("viewing_product_id")
            
        if pid:
            unique_ids.add(str(pid))
                
    logger.info(f"DEDUPLICATION COMPLETE: Found {len(unique_ids)} DISTINCT product IDs.")
    
    # Save unique IDs to the queue table safely
    bulk_ops = []
    for pid in unique_ids:
        doc = {"product_id": pid, "status": "PENDING", "retry_count": 0}
        bulk_ops.append(UpdateOne({"product_id": pid}, {"$setOnInsert": doc}, upsert=True))
        
        if len(bulk_ops) >= 5000:
            db["scrape_queue"].bulk_write(bulk_ops, ordered=False)
            bulk_ops = []
            
    if bulk_ops:
        db["scrape_queue"].bulk_write(bulk_ops, ordered=False)
        
    client.close()

# ==========================================
# PHASE 2: DATA EXTRACTION (JSON-LD)
# ==========================================
def extract_json_ld(html_text: str) -> dict:
    soup = BeautifulSoup(html_text, "html.parser")
    scripts = soup.find_all("script", type="application/ld+json")
    
    for script in scripts:
        if not script.string:
            continue
        try:
            data = json.loads(script.string.strip())
            if isinstance(data, list):
                for item in data:
                    if item.get("@type") == "Product":
                        return item
            elif isinstance(data, dict):
                if "@graph" in data:
                    for item in data["@graph"]:
                        if item.get("@type") == "Product":
                            return item
                elif data.get("@type") == "Product":
                    return data
        except json.JSONDecodeError:
            continue
    return {}

# ==========================================
# PHASE 3: ASYNC WORKER (CURL_CFFI CORE)
# ==========================================
async def fetch_product_data(pid: str, session: AsyncSession, semaphore: asyncio.Semaphore):
    """
    Worker function using curl_cffi to match browser TLS signatures.
    """
    async with semaphore:
        # Politeness Jitter
        await asyncio.sleep(random.uniform(3.0, 7.0))
        
        # Standard browser headers (User-Agent will be injected automatically by impersonate setting)
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Cache-Control": "max-age=0"
        }
        
        for domain in ENGLISH_DOMAINS:
            target_url = f"https://{domain}/search/?q={pid}"
            
            try:
                # impersonate="chrome" tricks Akamai into seeing a real Windows Chrome browser at TLS layer
                response = await session.get(target_url, headers=headers, timeout=10, impersonate="chrome")
                
                # curl_cffi uses status_code attribute instead of status
                if response.status_code == 200:
                    # response.text is a property string, no await needed
                    html = response.text
                    product_data = extract_json_ld(html)
                    
                    if product_data:
                        product_data["internal_product_id"] = pid
                        product_data["source_url"] = target_url
                        product_data["scraped_at"] = time.time()
                        return "SUCCESS", pid, product_data
                    else:
                        return "EXTRACTION_FAILED", pid, None
                        
                elif response.status_code in [403, 429]:
                    logger.warning(f"[{pid}] Anti-Bot triggered on {domain}. Falling back to next domain...")
                    continue
                    
                elif response.status_code == 404:
                    continue
                        
            except Exception:
                continue
                
        return "NOT_FOUND", pid, None

# ==========================================
# PHASE 4: ASYNC ORCHESTRATOR
# ==========================================
async def run_pipeline():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    cursor = db["scrape_queue"].find({
        "status": "PENDING", 
        "retry_count": {"$lt": MAX_RETRIES}
    })
    pending_ids = [doc["product_id"] for doc in cursor]
    
    if not pending_ids:
        logger.info("Scraping Queue is empty. Nothing to process.")
        client.close()
        return

    logger.info(f"IGNITING ASYNC ENGINE. Targets: {len(pending_ids)} | Concurrency Limit: {CONCURRENCY_LIMIT}")
    
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    success_count = 0
    fail_count = 0
    
    # Initialize curl_cffi AsyncSession instead of aiohttp ClientSession
    async with AsyncSession() as session:
        tasks = [fetch_product_data(pid, session, semaphore) for pid in pending_ids]
        
        for future in asyncio.as_completed(tasks):
            status, pid, data = await future
            
            if status == "SUCCESS":
                success_count += 1
                async with file_write_lock:
                    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                        f.write(json.dumps(data, ensure_ascii=False) + "\n")
                        
                db["scrape_queue"].update_one(
                    {"product_id": pid}, 
                    {"$set": {"status": "DONE"}}
                )
                logger.info(f"--> [HIT] Saved data for {pid}")
                
            else:
                fail_count += 1
                db["scrape_queue"].update_one(
                    {"product_id": pid}, 
                    {"$set": {"status": "FAILED"}, "$inc": {"retry_count": 1}}
                )
                logger.warning(f"--> [MISS] {status} for {pid}. Target dead or no JSON-LD.")

    logger.info(f"PIPELINE COMPLETE | Success: {success_count} | Failed/Missing: {fail_count}")
    client.close()

if __name__ == "__main__":
    start_time = time.time()
    
    try:
        # Step 1: Prep DB
        build_scrape_queue()
        # Step 2: Run Engine
        asyncio.run(run_pipeline())
    except KeyboardInterrupt:
        logger.warning("Pipeline manually stopped by user. Progress is saved in DB.")
        
    logger.info(f"Total Execution Time: {time.time() - start_time:.2f} seconds.")