"""
Resilient IP Location Pipeline.
Features:
1. Multi-threaded processing.
2. State-Driven Recovery (Crash-proof): Uses 'status' flag to remember progress.
3. Infinite Retry (Network-proof): Automatically reconnects if network drops for hours.
4. Dynamic Daily Logs: Creates separate log folders for each day.
"""

import datetime
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler

import IP2Location
from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.errors import (
    BulkWriteError,
    ConnectionFailure,
    ServerSelectionTimeoutError,
)

# ==========================================
# SYSTEM SETTINGS
# ==========================================
DEFAULT_URI = (
    "mongodb://admin_glamira:Th%40tIsMySecret2026%21@localhost:27018/?authSource=admin"
)
MONGO_URI = os.getenv("MONGO_URI", DEFAULT_URI)

DB_NAME = "countly"
BIN_FILE = "IP-COUNTRY-REGION-CITY.BIN"
BATCH_SIZE = 2500
MAX_WORKERS = 8

# ==========================================
# DYNAMIC DAILY LOGGING CONFIGURATION
# ==========================================
# Get today's date (e.g., '2026-06-22')
today_str = datetime.datetime.now().strftime("%Y-%m-%d")

# Create path: ../logs/2026-06-22/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logs", today_str)
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "ip_processor.log")

# Setup custom logger
logger = logging.getLogger("IP_Pipeline")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Handler 1: Terminal output
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Handler 2: File output (Rotating)
file_handler = RotatingFileHandler(
    LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ==========================================
# INITIALIZATION
# ==========================================
logger.info("Loading IP database into memory...")
try:
    ip_query = IP2Location.IP2Location(BIN_FILE)
except Exception as e:
    logger.critical(f"Cannot load BIN file. Error: {e}")
    exit(1)

# Initialize database connection
global_client = MongoClient(MONGO_URI, maxPoolSize=50, serverSelectionTimeoutMS=5000)
db = global_client[DB_NAME]


def setup_database():
    """
    Ensure required indexes exist for fast upserts and filtering.
    """
    logger.info("Checking database indexes...")
    # Index for target collection to make upserts fast
    db["ip_locations"].create_index([("ip", ASCENDING)], unique=True, background=True)
    # Index for source collection to make filtering by status fast
    db["unique_ips"].create_index([("status", ASCENDING)], background=True)
    # Ensure source collection has an index on 'ip' for fast updates
    db["unique_ips"].create_index([("ip", ASCENDING)], background=True)
    logger.info("Database is ready.")


def process_batch(ip_list):
    """
    Worker function: Decodes IPs, saves them, and marks them as DONE.
    """
    # TỬ HUYỆT ĐÃ ĐƯỢC VÁ: Mỗi Worker tự khởi tạo một bộ đọc file riêng biệt
    # Tuyệt đối không dùng chung biến global để tránh Xung đột đa luồng (Race Condition)
    local_ip_query = IP2Location.IP2Location(BIN_FILE)

    insert_ops = []

    for ip in ip_list:
        try:
            rec = local_ip_query.get_all(ip)

            # Bỏ qua nếu đọc ra None hoặc IP rác
            if not rec or not hasattr(rec, "country_short"):
                continue

            doc = {
                "ip": ip,
                "country_code": rec.country_short,
                "country_name": rec.country_long,
                "city": rec.city,
                "processed_at": time.time(),
            }
            insert_ops.append(UpdateOne({"ip": ip}, {"$set": doc}, upsert=True))
        except Exception as e:
            logger.error(f"Failed to decode IP [{ip}]: {e}")

    # Step 1: Save decoded data to the target collection
    if insert_ops:
        try:
            db["ip_locations"].bulk_write(insert_ops, ordered=False)
        except BulkWriteError as bwe:
            logger.error(f"Bulk write error details: {bwe.details}")
        except Exception as e:
            logger.error(f"Target DB write error: {e}")
            return 0

    # Step 2: Crash-Proof State Tracking
    try:
        db["unique_ips"].update_many(
            {"ip": {"$in": ip_list}}, {"$set": {"status": "DONE"}}
        )
    except Exception as e:
        logger.error(f"Source DB update error: {e}")

    return len(ip_list)


def chunk_generator(cursor, batch_size):
    """Yields batches of IP addresses safely."""
    batch = []
    for record in cursor:
        ip = record.get("ip")
        if ip:
            batch.append(ip)

        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


def execute_pipeline():
    """
    Runs the multi-threaded pipeline. Returns True if finished entirely.
    """
    # Only fetch IPs that are NOT marked as DONE.
    query = {"status": {"$ne": "DONE"}}

    # Check if there is anything left to process
    remaining = db["unique_ips"].count_documents(query)
    if remaining == 0:
        logger.info("No new IPs to process. Everything is DONE.")
        return True

    logger.info(f"Found {remaining} IPs waiting to be processed.")

    cursor = db["unique_ips"].find(query, {"ip": 1, "_id": 0}, no_cursor_timeout=True)

    futures = []
    processed_in_this_run = 0

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for batch in chunk_generator(cursor, BATCH_SIZE):
                futures.append(executor.submit(process_batch, batch))

                # RAM Control
                if len(futures) >= MAX_WORKERS * 2:
                    for future in as_completed(futures):
                        processed_in_this_run += future.result()
                    logger.info(
                        f"--> Batch complete. {processed_in_this_run} processed in this session."
                    )
                    futures.clear()

            # Wait for remaining tasks
            for future in as_completed(futures):
                processed_in_this_run += future.result()

            if processed_in_this_run > 0:
                logger.info(
                    f"--> Batch complete. {processed_in_this_run} processed in this session."
                )

    finally:
        cursor.close()

    return True


def main():
    """
    The orchestrator with Infinite Network Retry mechanism.
    """
    start_time = time.time()

    try:
        setup_database()
    except Exception as e:
        logger.critical(f"Cannot connect to DB at startup. Error: {e}")
        exit(1)

    logger.info("--- PIPELINE STARTED ---")

    # Infinite Retry Loop (Network Resilience)
    while True:
        try:
            # Attempt to run the full pipeline
            is_finished = execute_pipeline()

            if is_finished:
                break  # Exit the infinite loop when all data is processed

        except (ConnectionFailure, ServerSelectionTimeoutError) as net_error:
            # Network drops (even for an hour) will be caught here.
            logger.warning(
                f"Network connection lost. Waiting 60 seconds... Error: {net_error}"
            )
            time.sleep(60)  # Sleep and automatically retry

        except Exception as unknown_error:
            # Catch other random crashes (e.g., cursor killed by MongoDB server)
            logger.error(
                f"Pipeline crashed unexpectedly: {unknown_error}. Retrying in 10 seconds..."
            )
            time.sleep(10)

    # Cleanup
    global_client.close()
    run_time = time.time() - start_time
    logger.info(f"--- PIPELINE FULLY COMPLETED in {run_time:.2f} seconds ---")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning(
            "Pipeline forcefully stopped by user (Ctrl+C). Progress was saved safely."
        )
