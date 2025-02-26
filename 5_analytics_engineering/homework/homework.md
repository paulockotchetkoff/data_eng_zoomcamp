## Setup

First, it is necessary to have all the data in BigQuery and make sure the tables have the correct amount of rows. The data was ingested using `pandas_gbq`, in the `ingest_data.py` file. To make sure the correct amount of rows were ingested, the following query, changing the name of the table according to the type of trip:

```SQL
SELECT
  COUNT(1) AS n_rows
FROM
  `ny_taxi.green|yellow|fhv_trip_data`
```

