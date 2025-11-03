import os
import oracledb as db
import pandas as pd
from dotenv import load_dotenv

env_path = r"C:\Users\amarnadh.bellamkonda\Documents\Oracle_Scripts\.env"
load_dotenv(dotenv_path=env_path)
table_name = "PRODUCTLINES"

def extract_table():
    host = os.getenv("ORACLE_HOST")
    port = os.getenv("ORACLE_PORT")
    sid = os.getenv("ORACLE_SID")
    username = os.getenv("ORACLE_USERNAME")
    password = os.getenv("ORACLE_PASSWORD")
    schema = os.getenv("ORACLE_SCHEMA")
    output_path = os.getenv("OUTPUT_PATH", "./data")

    dsn = db.makedsn(host, port, sid=sid)
    conn = db.connect(user=username, password=password, dsn=dsn)
    df = pd.read_sql(f"SELECT PRODUCTLINE, TEXTDESCRIPTION, HTMLDESCRIPTION, IMAGE FROM  {schema}.{table_name}", conn)
    conn.close()

    folder = output_path
    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, f"{table_name}.csv")

    df.to_csv(file_path, index=False)
    print(f" Extracted {len(df)} rows from {table_name} → {file_path}")

if __name__ == "__main__":
    extract_table()