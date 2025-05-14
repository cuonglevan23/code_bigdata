from pymongo import MongoClient


def get_mongo_client(uri="mongodb://localhost:27017/"):
    return MongoClient(uri)


def inspect_fields(db_name="crime_data", collection_name="crimes", sample_size=100):
    client = get_mongo_client()
    db = client[db_name]
    collection = db[collection_name]

    # Lấy một số bản ghi để kiểm tra các trường
    sample_docs = collection.find().limit(sample_size)

    # Duyệt để thu thập tất cả các trường có mặt trong dữ liệu
    all_fields = set()
    for doc in sample_docs:
        all_fields.update(doc.keys())

    print(f"Các trường có mặt trong collection '{collection_name}':")
    for field in sorted(all_fields):
        print(f" - {field}")


if __name__ == "__main__":
    inspect_fields()
