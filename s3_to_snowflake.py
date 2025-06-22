import pandas as pd
import io
from common_functions import s3_conn, load_into_snowflake, cal_md5_hash, snowflake_conn
import os

# Read file from S3
def s3_data(file: str):
    s3 = s3_conn()
    obj = s3.get_object(
        Bucket=os.getenv('S3_BUCKET_NAME'),
        Key=os.path.join(os.getenv('S3_RAW_DATA_PATH'), file)
    )
    body = obj['Body'].read()
    xls = pd.ExcelFile(io.BytesIO(body))
    return xls

def main2(files_to_migrate: list, change: str):
    """
    Read file from s3 and migrate to snowflake
    :param files_to_migrate: file names to be migrated as list and changes as either 'create' or 'update'
    """
    for file in files_to_migrate:
        print(f"Processing {file}")
        xls = s3_data(file)
        # Assumes the first sheet is to be migrated; adjust as needed
        df = xls.parse(xls.sheet_names[0], skiprows=0, index_col=None, na_values=['NA'])
        df.columns = df.columns.str.strip()
        df = cal_md5_hash(df, file)
        load_into_snowflake(df, file)

if __name__ == "__main__":
    main2([
        "your_file_1.xlsx",
        "your_file_2.xlsx"
    ], 'create')
