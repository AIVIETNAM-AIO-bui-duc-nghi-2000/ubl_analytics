import json
from pymongo import MongoClient

# Kết nối qua đường hầm SSH
client = MongoClient("mongodb://admin_glamira:Th%40tIsMySecret2026%21@localhost:27018/?authSource=admin")
db = client["countly"]

print("\n[PEEK IN] ĐANG LẤY 1 DÒNG DỮ LIỆU MẪU TỪ BẢNG SUMMARY...")

# Lấy duy nhất 1 bản ghi đầu tiên có dữ liệu sự kiện
sample_document = db["summary"].find_one({"key": {"$exists": True}})

if sample_document:
    print(json.dumps(sample_document, indent=4, default=str))
else:
    # Nếu không tìm thấy theo key, lấy đại 1 dòng bất kỳ
    any_document = db["summary"].find_one()
    print(json.dumps(any_document, indent=4, default=str))