import csv
import os

def export_orders(cursor, update_timestamp):
    table = "Orders"
    columns_str = os.getenv("ORDERS_COLUMNS")
    if not columns_str:
        raise ValueError("ORDERS_COLUMNS not set in .env")
    columns = [col.strip() for col in columns_str.split(",")]

    select_cols = ', '.join(f'"{col}"' for col in columns)
    cursor.execute(f'SELECT {select_cols} FROM "{table.upper()}"')
    rows = cursor.fetchall()
    filename = f"{table}_{update_timestamp}.csv"
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(rows)
    return filename