import argparse
import os
from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = 'output.parquet'

    os.system(f'curl {url} -o {parquet_name}')

    ##
    # Creating Postgres engine
    ##
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    ##
    # Reading/Treating data
    ##
    df = pd.read_parquet(parquet_name)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    ##
    # Inserting data in Postgres
    ##

    # Creating table (inserting only the columns)
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Inserting rows
    start_time = time()
    chunk_size = 100000
    rows_inserted = 0
    for i in range(0, len(df), chunk_size):
        temp_df = df[i: i+chunk_size]
        temp_df.to_sql(name=table_name, con=engine, if_exists='append')

        rows_inserted += len(temp_df)
        
        print(f'{i} rows inserted. Elapsed time = {round(time() - start_time, 2)}. Total rows inserted = {rows_inserted}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')

    parser.add_argument('--user', help='user name for Postgres')
    parser.add_argument('--password', help='password for Postgres')
    parser.add_argument('--host', help='host for Postgres')
    parser.add_argument('--port', help='port for Postgres')
    parser.add_argument('--db', help='database name for Postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to in Postgres')
    parser.add_argument('--url', help='URL of the Parquet file to be ingested')

    args = parser.parse_args()
    
    main(args)