from pymongo import MongoClient

db = MongoClient("mongodb://35.247.155.208:27017/")["countly"]
print(f"PENDING: {db.scrape_queue.count_documents({'status': 'PENDING'})}")
print(f"COMPLETED: {db.scrape_queue.count_documents({'status': 'COMPLETED'})}")
print(f"DEAD_LINK: {db.scrape_queue.count_documents({'status': 'DEAD_LINK'})}")
print(f"FAILED_SCHEMA: {db.scrape_queue.count_documents({'status': 'FAILED_SCHEMA'})}")
print(f"ANTI_BOT: {db.scrape_queue.count_documents({'status': 'ANTI_BOT'})}")
