import csv
import os
from db_utils import get_connection
from dotenv import load_dotenv
from upload_to_s3 import upload_to_s3

load_dotenv()

def load_customers(cursor, update_timestamp):
    table = "Customers"
    columns_str = os.getenv("CUSTOMERS_COLUMNS")
    db_link = os.getenv("DB_LINK")

    if not columns_str:
        raise ValueError("CUSTOMERS_COLUMNS not set in .env")

    if not db_link:
        raise ValueError("DB_LINK not set in .env")

    columns = [col.strip() for col in columns_str.split(",")]

    print(f"Fetching data for table '{table}' with timestamp '{update_timestamp}'...")
    print(f'SELECT {columns_str} FROM {table.upper()}@{db_link}')
    
    cursor.execute(f'SELECT {columns_str} FROM {table.upper()}@{db_link}')
    rows = cursor.fetchall()
    print(f"Fetched {len(rows)} rows from the database.")

    filename = f"{table}_{update_timestamp}.csv"
    print(f"Writing data to file: {filename}")
    with open(filename, 'w', newline='',encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(rows)
    print(f"File {filename} written successfully.")
    return filename


def export_customers():
    update_timestamps = os.getenv("UPDATE_TIMESTAMPS")
    bucket = os.getenv("BUCKET")
    table = "Customers"

    if not update_timestamps:
        raise ValueError("UPDATE_TIMESTAMPS not set in .env")

    if not bucket:
        raise ValueError("BUCKET not set in .env")


    print("Starting export process for Customers table...")

    update_timestamps = [d.strip() for d in update_timestamps.split(",") if d.strip()]    
    
    for update_timestamp in update_timestamps:
        print(f"\nProcessing update timestamp: {update_timestamp}")
        conn, cursor = None, None

        try:
            conn, cursor = get_connection(update_timestamp)
            print(f"Database connection established for {update_timestamp}.")

            filename = load_customers(cursor, update_timestamp)

            s3_key = f"{table}/{update_timestamp}/{filename}"
            print(f"Uploading {filename} to S3 bucket '{bucket}' with key '{s3_key}'...")
            upload_to_s3(filename, bucket, s3_key)
            print(f"File uploaded to S3 successfully: s3://{bucket}/{s3_key}")

            os.remove(filename)
            print(f"Local file deleted: {filename}")

        except Exception as e:
            print(f"Error processing {update_timestamp}: {e}")

        finally:
            if cursor:
                cursor.close()
                print("Database cursor closed.")
            if conn:
                conn.close()
                print("Database connection closed.")

    print(f"\nExport process completed for all update timestamps in {table}.")


if __name__ == "__main__":
    export_customers()
