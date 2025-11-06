import csv
import os
from db_utils import get_connection
from dotenv import load_dotenv
from upload_to_s3 import upload_to_s3

load_dotenv()

def load_products(cursor, update_timestamp):
    table = "Products"
    columns_str = os.getenv("PRODUCTS_COLUMNS")
    db_link = os.getenv("DB_LINK")

    if not columns_str:
        raise ValueError("PRODUCTS_COLUMNS not set in .env")
    if not db_link:
        raise ValueError("DB_LINK not set in .env")

    columns = [col.strip() for col in columns_str.split(",")]
    print(f'SELECT {columns_str} FROM {table.upper()}@{db_link}')

    cursor.execute(f'SELECT {columns_str} FROM {table.upper()}@{db_link}')
    rows = cursor.fetchall()

    filename = f"{table}_{update_timestamp}.csv"
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(rows)
    print("load_products  completed")
    return filename

def export_products():
    update_timestamps = os.getenv("UPDATE_TIMESTAMPS")
    bucket = os.getenv("BUCKET")
    table = "Products"

    if not update_timestamps:
        raise ValueError("UPDATE_TIMESTAMPS not set in .env")
    if not bucket:
        raise ValueError("BUCKET not set in .env")

    update_timestamps = [d.strip() for d in update_timestamps.split(",") if d.strip()]    
    

    for update_timestamp in update_timestamps:
        conn, cursor = get_connection(update_timestamp)
        filename = load_products(cursor, update_timestamp)
        s3_key = f"{table}/{update_timestamp}/{filename}"
        print("s3 key is",s3_key)
        upload_to_s3(filename, bucket, s3_key)
        print("upload completed")
        os.remove(filename)

        cursor.close()
        conn.close()



if __name__ == "__main__":
    export_products()
