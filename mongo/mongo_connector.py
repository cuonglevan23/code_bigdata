from pymongo import MongoClient
import pandas as pd
from models.crime import Crime  # Import models

def get_mongo_client(uri="mongodb://localhost:27017/"):
    return MongoClient(uri)

def insert_data_to_mongo(data, db_name="crime_data", collection_name="crimes"):
    client = get_mongo_client()
    db = client[db_name]
    collection = db[collection_name]
    collection.insert_many(data)
    print(f"Đã chèn {len(data)} bản ghi vào MongoDB.")

def load_and_insert_data(csv_path):
    df = pd.read_csv(csv_path, on_bad_lines='skip')
    print(f"Dữ liệu đã được tải thành công với {len(df)} dòng.")

    crime_dicts = []
    for _, row in df.iterrows():
        crime = Crime.from_series(row)
        if crime:
            crime_dicts.append(crime.to_dict())

    insert_data_to_mongo(crime_dicts)
