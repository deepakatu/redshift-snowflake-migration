import pandas as pd
from common_functions import make_engine, load_into_snowflake, snowflake_conn, drop_table, cal_md5_hash
import os

# Query redshift data
def redshift_data(query: str):
    conn = make_engine()
    print("Connected to redshift")
    cursor = conn.cursor()
    cursor.execute(query)
    df: pd.DataFrame = cursor.fetch_dataframe()
    print(f"Count: {len(df)} rows")
    return df

# Add your custom batch/date logic here as needed

def main(tables_to_migrate: list, change: str):
    """
    Read data from redshift and migrate to snowflake
    :param tables_to_migrate: table names to be migrated as list and changes as either 'create' or 'update'
    """
    for table in tables_to_migrate:
        print(f"Processing {table}")
        query = f"SELECT * FROM {table}"
        df = redshift_data(query)
        # Example hash if needed
        df = cal_md5_hash(df, table)
        load_into_snowflake(df, table)

if __name__ == "__main__":
    main([
        "your_redshift_table1",
        "your_redshift_table2"
    ], 'create')
