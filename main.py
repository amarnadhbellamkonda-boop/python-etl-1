import os
import oracledb as db
import pandas as pd
from dotenv import load_dotenv

env_path = r"C:\Users\amarnadh.bellamkonda\Documents\Oracle_Scripts\.env"
load_dotenv(dotenv_path=env_path)

def extract_tables():
    host = os.getenv("ORACLE_HOST")
    port = os.getenv("ORACLE_PORT")
    sid = os.getenv("ORACLE_SID")
    username = os.getenv("ORACLE_USERNAME")
    password = os.getenv("ORACLE_PASSWORD")
    schema = os.getenv("ORACLE_SCHEMA")
    output_path = os.getenv("OUTPUT_PATH", "./data")
    tables = os.getenv("TABLES", "").split(",")
    tables = [t.strip() for t in tables if t.strip()]

    dsn = db.makedsn(host, port, sid=sid)

    for table_name in tables:
        print(f"\n Extracting table: {table_name}")
        try:
            conn = db.connect(user=username, password=password, dsn=dsn)
            query = f"SELECT * FROM {schema}.{table_name}"
            df = pd.read_sql(query, conn)

            for col in df.columns:
                df[col] = df[col].apply(lambda x: str(x.read()) if hasattr(x, "read") else x)

            conn.close()

            folder = output_path
            os.makedirs(folder, exist_ok=True)
            file_path = os.path.join(folder, f"{table_name}.csv")
            df.to_csv(file_path, index=False)

            print(f" Extracted {len(df)} rows → {file_path}")

        except Exception as e:
            print(f" Error extracting {table_name}: {e}")

if __name__ == "__main__":
    extract_tables()