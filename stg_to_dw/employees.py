import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch

load_dotenv()

def load_employees_into_dw(ETL_BATCH_NO=None, ETL_BATCH_DATE=None):

    conn, cur = get_redshift_connection()

    if ETL_BATCH_NO is None or ETL_BATCH_DATE is None:
        ETL_BATCH_NO, ETL_BATCH_DATE = get_latest_etl_batch()

    dev_stage = os.getenv("DEV_STAGE_SCHEMA")     # e.g., j25Amarnadh_prodstage
    dev_dw = os.getenv("DEV_DW_SCHEMA")           # e.g., j25Amarnadh_proddw
    table = "employees"

    if not dev_stage:
        raise ValueError("DEV_STAGE_SCHEMA not set in .env")

    if not dev_dw:
        raise ValueError("DEV_DW_SCHEMA not set in .env")

    # --------------------------------------------
    # 1️⃣ INSERT NEW ROWS
    # --------------------------------------------
    insert_query = f"""
INSERT INTO {dev_dw}.employees
(
   employeenumber,
   lastname,
   firstname,
   extension,
   email,
   officecode,
   reportsto,
   jobtitle,
   dw_office_id,
   dw_reporting_employee_id,
   src_create_timestamp,
   src_update_timestamp,
   dw_create_timestamp,
   dw_update_timestamp,
   etl_batch_no,
   etl_batch_date
)
SELECT
   s.employeenumber,
   s.lastname,
   s.firstname,
   s.extension,
   s.email,
   s.officecode,
   s.reportsto,
   s.jobtitle,
   NULL,                 -- dw_office_id placeholder
   NULL,                 -- dw_reporting_employee_id placeholder
   s.create_timestamp,   -- stage column
   s.update_timestamp,   -- stage column
   CURRENT_TIMESTAMP,
   CURRENT_TIMESTAMP,
   {ETL_BATCH_NO},
   '{ETL_BATCH_DATE}'
FROM {dev_stage}.employees s
WHERE NOT EXISTS (
    SELECT 1
    FROM {dev_dw}.employees d
    WHERE d.employeenumber = s.employeenumber
);
"""

    # --------------------------------------------
    # 2️⃣ UPDATE EXISTING ROWS WITH NEWER DATA
    # --------------------------------------------
    update_query = f"""
UPDATE {dev_dw}.employees d
SET 
    lastname = s.lastname,
    firstname = s.firstname,
    extension = s.extension,
    email = s.email,
    officecode = s.officecode,
    reportsto = s.reportsto,
    jobtitle = s.jobtitle,
    src_update_timestamp = s.update_timestamp,
    dw_update_timestamp = CURRENT_TIMESTAMP,
    etl_batch_no = {ETL_BATCH_NO},
    etl_batch_date = '{ETL_BATCH_DATE}'
FROM {dev_stage}.employees s
WHERE d.employeenumber = s.employeenumber
  AND d.src_update_timestamp < s.update_timestamp;
"""

    # --------------------------------------------
    # 3️⃣ UPDATE dw_reporting_employee_id
    #    employees.reportsTo → employees.dw_employee_id
    # --------------------------------------------
    update_reporting_fk = f"""
UPDATE {dev_dw}.employees d
SET dw_reporting_employee_id = mgr.dw_employee_id
FROM {dev_dw}.employees mgr
WHERE d.reportsto = mgr.employeenumber
  AND d.reportsto IS NOT NULL;
"""

    # --------------------------------------------
    # 4️⃣ UPDATE dw_office_id
    #    employees.officeCode → offices.dw_office_id
    # --------------------------------------------
    update_office_fk = f"""
UPDATE {dev_dw}.employees d
SET dw_office_id = o.dw_office_id
FROM {dev_dw}.offices o
WHERE d.officecode = o.officecode
  AND d.dw_office_id IS NULL;
"""

    try:
        print(f"Loading table {table}...")

        # Run INSERT
        cur.execute(insert_query)
        conn.commit()

        # Run UPDATE
        cur.execute(update_query)
        conn.commit()

        # Resolve FK: reporting employee
        cur.execute(update_reporting_fk)
        conn.commit()

        # Resolve FK: office id
        cur.execute(update_office_fk)
        conn.commit()

        print(f"Table {table} loaded successfully.")

    except Exception as e:
        print(f"Error loading table {table}: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    load_employees_into_dw()
