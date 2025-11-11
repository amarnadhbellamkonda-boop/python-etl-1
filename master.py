import os
from dotenv import load_dotenv
from utils import get_latest_etl_batch_date
from src_to_s3.main import run_src_to_s3
from s3_to_stg.main import run_s3_to_stg

load_dotenv()

def run_etl_process():
    try:
        latest_date = get_latest_etl_batch_date()
        run_src_to_s3(latest_date)
        print("Source to S3 ETL completed.")
        run_s3_to_stg(latest_date)
        print("S3 to dev_stage completed.")
    except Exception as e:
        print("Error in master etl process: ",e)
    
if __name__ == "__main__":
    run_etl_process()