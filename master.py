import os
from dotenv import load_dotenv
from utils import get_latest_etl_batch
from src_to_s3.main import run_src_to_s3
from s3_to_stg.main import run_s3_to_stg
from stg_to_dw.main import run_dw_etl

load_dotenv()

def run_etl_process():
    try:
        latest_batch,latest_date = get_latest_etl_batch()
        run_src_to_s3(latest_date)
        print("Source to S3 ETL completed.")
        run_s3_to_stg(latest_date)
        print("S3 to dev_stage completed.")
        run_dw_etl(latest_batch,latest_date)
        print("dev_stage to dev_dw ETL completed.")
    except Exception as e:
        print("Error in master etl process: ",e)
    
if __name__ == "__main__":
    run_etl_process()