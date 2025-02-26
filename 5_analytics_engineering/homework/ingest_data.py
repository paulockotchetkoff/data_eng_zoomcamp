import os
from urllib.request import urlretrieve

from dotenv import load_dotenv
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq

load_dotenv('../creds/.env')

project_id = os.getenv('GCP_PROJECT_ID')
bq_schema_name = os.getenv('BQ_SCHEMA_NAME')
credentials = service_account.Credentials.from_service_account_file(
    '../creds/dbt_sa_creds.json',
)


def ingest_taxi_trips_to_gbq(trips_category: str, years: list, months: list, **kwargs) -> None:
    table_name = kwargs.get('table_name')
    if not table_name:
        table_name = f'{trips_category}_trip_data'
    
    for year in years:
        for month in months:
            file_name = f'{trips_category}_tripdata_{year}-{month}.csv.gz'
            url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{trips_category}/{file_name}'

            urlretrieve(url, file_name)

            df = pd.read_csv(file_name, compression='gzip')
            for col in df.columns:
                if col.endswith('datetime'):
                    df[col] = pd.to_datetime(df[col])

            pandas_gbq.to_gbq(
                dataframe=df,
                destination_table=f'{bq_schema_name}.{table_name}',
                project_id=project_id,
                credentials=credentials,
                if_exists='append',
            )

            print(f'{file_name} successfully ingested.')

            os.remove(file_name)


all_months = [str(i).zfill(2) for i in range(1, 13)]
ingestion_params = {
    'green': {'years': ['2019', '2020'], 'months': all_months},
    'yellow': {'years': ['2019', '2020'], 'months': all_months},
    'fhv': {'years': ['2019'], 'months': all_months},
}

for category, params in ingestion_params.items():
    ingest_taxi_trips_to_gbq(
        trips_category=category,
        years=params['years'],
        months=params['months'],
    )