import os
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import redshift_connector
import boto3
import hashlib
from snowflake.snowpark.session import Session
import pandas as pd

def make_engine():
    conn = redshift_connector.connect(
        host=os.getenv('REDSHIFT_HOST'),
        database=os.getenv('REDSHIFT_DATABASE'),
        port=int(os.getenv('REDSHIFT_PORT', 5439)),
        user=os.getenv('REDSHIFT_USER'),
        password=os.getenv('REDSHIFT_PASSWORD')
    )
    return conn

def snowflake_conn():
    engine = create_engine(URL(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE')
    ))
    connection_parameters = {
        "account": os.getenv('SNOWFLAKE_ACCOUNT'),
        "user": os.getenv('SNOWFLAKE_USER'),
        "password": os.getenv('SNOWFLAKE_PASSWORD'),
        "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
        "role": os.getenv('SNOWFLAKE_ROLE'),
        "database": os.getenv('SNOWFLAKE_DATABASE'),
        "schema": os.getenv('SNOWFLAKE_SCHEMA')
    }
    session = Session.builder.configs(connection_parameters).create()
    return session, engine

def s3_conn():
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )
    print("Connected to Amazon S3")
    return s3

def load_into_snowflake(df: pd.DataFrame, sf_table: str, replace: str = 'Y'):
    session, engine = snowflake_conn()
    connection = engine.connect()
    print("Connected to Snowflake")
    if replace == 'Y':
        df.to_sql(sf_table, con=engine, index=False, if_exists='replace', chunksize=10000)
    else:
        df.to_sql(sf_table, con=engine, index=False, if_exists='append', chunksize=10000)
    connection.close()
    engine.dispose()

def drop_table(table: str):
    session, engine = snowflake_conn()
    t = session.table(table)
    t.drop_table()
    print("Snowflake table dropped")

def cal_md5_hash(df: pd.DataFrame, sf_table: str):
    # Example hashing logic—customize as needed for your columns
    if sf_table == "your_table_1":
        df['temp'] = df['col1'] + df['col2'].astype(str)
        df['ID'] = [hashlib.md5(val.encode('utf-8')).hexdigest() for val in df['temp']]
    if sf_table == "your_table_2":
        df['temp'] = df['colA'] + df['colB'].astype(str)
        df['ID'] = [hashlib.md5(val.encode('utf-8')).hexdigest() for val in df['temp']]
    # Add more cases as needed for your table schemas
    return df
