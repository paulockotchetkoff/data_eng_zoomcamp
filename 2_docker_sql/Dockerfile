FROM python:3.12.8

RUN pip install pandas sqlalchemy psycopg2 fastparquet

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]