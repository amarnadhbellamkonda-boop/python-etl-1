import redshift_connector
import os
import datetime
from dotenv import load_dotenv

load_dotenv()

def get_redshift_connection():
    try:
        conn = redshift_connector.connect(
            host=os.getenv("REDSHIFT_HOST"),
            database=os.getenv("REDSHIFT_DB"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASSWORD"),
            port=os.getenv("REDSHIFT_PORT"),
        )
        cursor = conn.cursor()
        print("Redshift connection established.")
        return conn,cursor
    except Exception as e:
        print("Error in connecting red shift:", e)

def get_latest_etl_batch():
    try:
        etl_metadata_schema = os.getenv("ETL_METADATA_SCHEMA")
        conn, cursor = get_redshift_connection()
        cursor.execute(f"SELECT * FROM {etl_metadata_schema}.batch_control;")
        rows = cursor.fetchall()
        latest_date = rows[0][1]
        if isinstance(latest_date, (datetime.date, datetime.datetime)):
            latest_date = latest_date.strftime("%Y-%m-%d")
        else:
            latest_date = str(latest_date)
        print(f"Latest ETL batch date fetched: {latest_date}")
        return rows[0][0],latest_date
    except Exception as e:
        print(f"Error fetching latest ETL batch date: {e}")
        return None
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
        print("Redshift connection closed.")

if __name__ == "__main__":
    latest_date = get_latest_etl_batch()
    print("Latest ETL batch date:", latest_date)