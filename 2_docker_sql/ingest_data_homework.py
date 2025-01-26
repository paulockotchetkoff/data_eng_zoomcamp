import argparse
from time import time

import pandas as pd
import requests
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    trips_table_name = params.trips_table_name
    zones_table_name = params.zones_table_name
    url_trips = params.url_trips
    url_zones = params.url_zones
    trips_csv_name = 'green_taxi_trips.gz'
    zones_csv_name = 'green_taxi_zones.csv'

    ##
    # Downloading files
    ##
    for url in [url_trips, url_zones]:
        file_name = 'green_taxi_trips.gz'
        if url == url_zones:
            file_name = 'green_taxi_zones.csv'
        
        print(f'Downloading {file_name}')
        response = requests.get(url)
        
        with open(file_name, mode='wb') as f:
            f.write(response.content)

    ##
    # Creating Postgres engine
    ##
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    ##
    # Reading/Treating data
    ##
    df_trips = pd.read_csv(trips_csv_name, compression='gzip')
    df_trips['lpep_pickup_datetime'] = pd.to_datetime(df_trips['lpep_pickup_datetime'])
    df_trips['lpep_dropoff_datetime'] = pd.to_datetime(df_trips['lpep_dropoff_datetime'])

    df_zones = pd.read_csv(zones_csv_name)

    ##
    # Inserting data in Postgres
    ##

    # Creating tables (zones table has few rows, so we can just insert them)
    df_trips.head(0).to_sql(name=trips_table_name, con=engine, if_exists='replace')
    df_zones.to_sql(name=zones_table_name, con=engine, if_exists='replace')

    # Inserting rows to trips table
    start_time = time()
    chunk_size = 100000
    rows_inserted = 0
    for i in range(0, len(df_trips), chunk_size):
        temp_df = df_trips[i: i+chunk_size]
        temp_df.to_sql(name=trips_table_name, con=engine, if_exists='append')

        rows_inserted += len(temp_df)
        
        print(f'Elapsed time = {round(time() - start_time, 2)}. Total rows inserted = {rows_inserted}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for Postgres')
    parser.add_argument('--password', help='password for Postgres')
    parser.add_argument('--host', help='host for Postgres')
    parser.add_argument('--port', help='port for Postgres')
    parser.add_argument('--db', help='database name for Postgres')
    parser.add_argument('--trips_table_name', help='name of the table where we will write the trips data in Postgres')
    parser.add_argument('--zones_table_name', help='name of the table where we will write the zones data in Postgres')
    parser.add_argument('--url_trips', help='URL of the CSV file containing trips to be ingested')
    parser.add_argument('--url_zones', help='URL of the CSV file containing zones to be ingested')

    args = parser.parse_args()
    
    main(args)