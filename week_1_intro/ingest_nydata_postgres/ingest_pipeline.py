import os
import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import argparse
from time import time

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = 'output.parquet'

    os.system(f'wget {url} -O {parquet_name}')

    engine_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'

    engine = create_engine(engine_url)
    print(f'Connected to {engine_url}')

    parquet_file = pq.ParquetFile(parquet_name)

    batch_iter = parquet_file.iter_batches()

    batch_df = next(batch_iter).to_pandas()

    batch_df.tpep_pickup_datetime = pd.to_datetime(batch_df.tpep_pickup_datetime)
    batch_df.tpep_dropoff_datetime = pd.to_datetime(batch_df.tpep_dropoff_datetime)

    batch_df.head(n=0)

    batch_df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    batch_df.to_sql(name=table_name, con=engine, if_exists='append')


    while True:
        t_start = time()
        try:
            batch_df = next(batch_iter).to_pandas()
        except StopIteration:
            print('All done!')
            break
        batch_df.tpep_pickup_datetime = pd.to_datetime(batch_df.tpep_pickup_datetime)
        batch_df.tpep_dropoff_datetime = pd.to_datetime(batch_df.tpep_dropoff_datetime)

        batch_df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print(f'Another chunk bites the dust, in {(t_end - t_start):.3f}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet NYC data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='db name for postgres')
    parser.add_argument('--table-name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()
    main(args)
