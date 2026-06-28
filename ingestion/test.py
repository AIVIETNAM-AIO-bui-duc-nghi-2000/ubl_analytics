from pymongo import MongoClient

# Thay thế bằng EXTERNAL IP của VM của bạn từ GCP Console
VM_EXTERNAL_IP = "35.247.155.208"
try:
    # Kết nối với timeout ngắn để phát hiện lỗi nhanh nếu bị chặn
    client = MongoClient(
        f"mongodb://{VM_EXTERNAL_IP}:27017/", serverSelectionTimeoutMS=5000
    )

    # Ép một lệnh gọi tới server
    client.server_info()
    print("Đã kết nối thành công tới MongoDB trên GCP!")

except Exception as e:
    print(f"Kết nối thất bại: {e}")
