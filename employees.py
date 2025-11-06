import csv
import os
from db_utils import get_connection
from dotenv import load_dotenv
from upload_to_s3 import upload_to_s3

load_dotenv()

def load_employees(cursor, update_timestamp):
    table = "Employees"
    columns_str = os.getenv("EMPLOYEES_COLUMNS")
    db_link = os.getenv("DB_LINK")
    
    if not columns_str:
        raise ValueError("EMPLOYEES_COLUMNS not set in .env")

    columns = [col.strip() for col in columns_str.split(",")]
    select_cols = ', '.join(f'"{col}"' for col in columns)

    cursor.execute(f'SELECT {select_cols} FROM {table.upper()}@{db_link}')
    rows = cursor.fetchall()

    filename = f"{table}_{update_timestamp}.csv"
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(rows)

    return filename

def export_employees():
    update_timestamps = os.getenv("UPDATE_TIMESTAMPS")
    bucket = os.getenv("BUCKET")

    if not update_timestamps:
        raise ValueError("UPDATE_TIMESTAMPS not set in .env")

    if not bucket:
        raise ValueError("BUCKET not set in .env")

    update_timestamps = [d.strip() for d in update_timestamps.split(",") if d.strip()]    
    

    for update_timestamp in update_timestamps:
        conn, cursor = get_connection(update_timestamp)
        table = "Employees"

        filename = load_employees(cursor, update_timestamp)
        s3_key = f"{table}/{update_timestamp}/{filename}"

        upload_to_s3(filename, bucket, s3_key)
        os.remove(filename)

        cursor.close()
        conn.close()
    return




if __name__ == "__main__":
    export_employees()
