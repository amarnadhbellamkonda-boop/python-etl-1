import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch

def load_orders(conn=None, cur=None, latest_date=None):
    if cur is None or conn is None:
        conn, cur = get_redshift_connection()
        
    if latest_date is None:
        latest_batch,latest_date = get_latest_etl_batch()
    
    iam_role = os.getenv("IAM")
    s3_bucket = os.getenv("BUCKET")
    dev_stage = os.getenv("DEV_STAGE_SCHEMA")

    if not s3_bucket:   
        raise ValueError("BUCKET not set in .env")
    
    if not iam_role:
        raise ValueError("IAM_ROLE not set in .env")

    try:
        table = "Orders"
        redshift_table = f"{dev_stage}.{table.lower()}"
        s3_key = f"{table}/{latest_date}/{table}_{latest_date}.csv"
        truncate_query = f"TRUNCATE TABLE {redshift_table};"
        print(f"Truncating table {redshift_table}...")
        cur.execute(truncate_query)
        conn.commit()
        print(f"Table {redshift_table} truncated successfully.")

        copy_query = f"""
            COPY {redshift_table}
            FROM 's3://{s3_bucket}/{s3_key}'
            IAM_ROLE '{iam_role}'
            FORMAT AS CSV
            IGNOREHEADER 1
            DELIMITER ','
            TIMEFORMAT 'auto'
            DATEFORMAT 'auto'
            BLANKSASNULL
            EMPTYASNULL
            TRUNCATECOLUMNS
            ACCEPTINVCHARS AS '?'
            MAXERROR 100;
        """
        
        print(f"Running COPY command for table {table}...")
        cur.execute(copy_query)
        if conn:
            conn.commit()
        print(f"✅ Data copied successfully for table {table} from S3 to Redshift!")
    except Exception as e:
        print(f"Error during COPY command for table {table}: {e}")   
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()   

if __name__ == "__main__":
    load_orders()