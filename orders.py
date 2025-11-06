import csv
import os
from db_utils import get_connection
from dotenv import load_dotenv
from upload_to_s3 import upload_to_s3

load_dotenv()

def load_orders(cursor, update_timestamp):
    table = "Orders"
    columns_str = os.getenv("ORDERS_COLUMNS")
    db_link = os.getenv("DB_LINK")

    if not columns_str:
        raise ValueError("ORDERS_COLUMNS not set in .env")
    columns = [col.strip() for col in columns_str.split(",")]

    print(f"Fetching data for table '{table}' with timestamp '{update_timestamp}'...")
    print(f'SELECT {columns_str} FROM {table.upper()}@{db_link}')

    cursor.execute(f'SELECT {columns_str} FROM {table.upper()}@{db_link}')
    rows = cursor.fetchall()

    filename = f"{table}_{update_timestamp}.csv"
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(rows)
    
    return filename

def export_orders():
    table="Orders"
    update_timestamps = os.getenv("UPDATE_TIMESTAMPS")
    bucket = os.getenv("BUCKET")

    if not update_timestamps:
        raise ValueError("UPDATE_TIMESTAMPS not set in .env")

    if not bucket:
        raise ValueError("BUCKET not set in .env")

    print("Starting export process for Customers table...")

    update_timestamps = [d.strip() for d in update_timestamps.split(",") if d.strip()]    
    
    for update_timestamp in update_timestamps:
        print(f"\n Exporting data for timestamp: {update_timestamp}")
        conn, cursor = get_connection(update_timestamp)
        print(f"Database connection established for {update_timestamp}.")
        try:
            filename = load_orders(cursor, update_timestamp)
            s3_key = f"{table}/{update_timestamp}/{filename}"
            print(f"Uploading {filename} to S3 bucket '{bucket}' with key '{s3_key}'...")
            try:
                upload_to_s3(filename, bucket, s3_key)
                print(f"Uploaded to s3://{bucket}/{s3_key}")
            except Exception as upload_err:
                print(f"Error uploading {filename}: {upload_err}")
                continue

            os.remove(filename)
            print(f"Deleted local file: {filename}")

        except Exception as db_err:
            print(f"Database error for {update_timestamp}: {db_err}")

        finally:
            if cursor:
                cursor.close()
                print("Database cursor closed.")
            if conn:
                conn.close()
                print("Database connection closed.")

    print(f"\nExport process completed for all update timestamps in {table}.")



if __name__ == "__main__":
    export_orders()
