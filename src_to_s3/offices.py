import csv
import os
import io
from .db_utils import get_db_connection
from dotenv import load_dotenv
from .upload_to_s3 import upload_to_s3

load_dotenv()

def load_offices(cursor, update_timestamp):
    table = "Offices"
    columns_str = os.getenv("OFFICES_COLUMNS")
    db_link = os.getenv("DB_LINK")
    if not columns_str:
        raise ValueError("OFFICES_COLUMNS not set in .env")
    columns = [col.strip() for col in columns_str.split(",")]

    select_cols = ', '.join(f'"{col}"' for col in columns)
    cursor.execute(f'SELECT {select_cols} FROM {table.upper()}@{db_link}')
    rows = cursor.fetchall()

    filename = f"{table}_{update_timestamp}.csv"
    print(f"Writing data to file: {filename}")

    # --- Clean UTF-8-safe writing section ---
    def clean_text(value):
        """Convert all strings safely to UTF-8 without invalid characters."""
        if value is None:
            return ""
        if isinstance(value, str):
            # Re-encode from Windows-1252 (CP1252) → UTF-8 safely
            return value.encode("windows-1252", errors="ignore").decode("utf-8", errors="ignore")
        return str(value)

    # Write the data safely to a temporary CSV file
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        for row in rows:
            safe_row = [clean_text(col) for col in row]
            writer.writerow(safe_row)

    print(f"File {filename} written successfully (UTF-8 cleaned).")
    return filename

def export_offices(update_timestamp = None):
    # update_timestamps = os.getenv("UPDATE_TIMESTAMPS")
    bucket = os.getenv("BUCKET")
    table = "Offices"
    
    if not update_timestamp:
        raise ValueError("UPDATE_TIMESTAMPS not set in .env")

    if not bucket:
        raise ValueError("BUCKET not set in .env")

    # update_timestamps = [d.strip() for d in update_timestamps.split(",") if d.strip()]    
    
    try:
        conn, cursor = get_db_connection(update_timestamp)
        print(f"Database connection established for {update_timestamp}.")

        filename = load_offices(cursor, update_timestamp)

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
    return


if __name__ == "__main__":
    export_offices()