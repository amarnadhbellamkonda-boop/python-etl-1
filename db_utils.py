import os
from dotenv import load_dotenv
import oracledb

load_dotenv()

def get_schema_by_timestamp(update_timestamp: str):
    print(f"update timestamp in get_schema_by_timestamp: {update_timestamp}")
    if update_timestamp == "2001-01-01":
        return "CM_20050609"
    allowed_dates = ["2005-06-10", "2005-06-11", "2005-06-12", "2005-06-13", "2005-06-14"]
    if update_timestamp in allowed_dates:
        return f"CM_{update_timestamp.replace('-', '')}"
    raise ValueError(f"Date {update_timestamp} not supported for schema")


def get_connection(update_timestamp):
    user = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    service = os.getenv("DB_SERVICE")
    DB_LINK = os.getenv("DB_LINK")
    
    schema = get_schema_by_timestamp(update_timestamp)
    dsn = f"{host}:{port}/{service}"

    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()
    print(f"update timestamp in get connection is {update_timestamp}")
    print("Connection",conn)
    print("Cursor",cursor)
    # cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA={schema}")
    cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA=j25Amarnadh")

    try:
        cursor.execute(f"DROP PUBLIC DATABASE LINK {DB_LINK}")
        print(f"Dropped existing database link: {DB_LINK}")
    except oracledb.DatabaseError:
        print(f"No existing database link {DB_LINK} found — skipping drop.")
    create_link_sql = f"""
    CREATE PUBLIC DATABASE LINK {DB_LINK}
    CONNECT TO {schema} IDENTIFIED BY {schema}123
    USING '(DESCRIPTION=
      (ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))
      (CONNECT_DATA=(SERVICE_NAME={service}))
    )'
    """
    cursor.execute(create_link_sql)
    print(f"Created database link: {DB_LINK}")
    print("returning in get_connection")
    return conn, cursor