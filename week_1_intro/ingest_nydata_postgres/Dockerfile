FROM python:3.10

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY ingest_pipeline.py ingest_pipeline.py

ENTRYPOINT ["python","ingest_pipeline.py"]