import pandas as pd
from sqlalchemy import create_engine
import argparse
import requests
import psycopg2


def download_file(url, local_filename):
    with requests.get(url, stream=True) as response:
        with open(local_filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = 'input.parquet'
    
    download_file(url, parquet_name)

    df = pd.read_parquet(parquet_name)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    batch_size = 100000

    total_rows = len(df)

    num_batches = (total_rows // batch_size) + 1

    for i in range(num_batches):
        start_index = i * batch_size
        end_index = min((i + 1) * batch_size, total_rows)
        
        batch_df = df.iloc[start_index:end_index]
        
        batch_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        
        print(f"Lote {i + 1}/{num_batches} cargado en la base de datos")

    print("Proceso de carga completo.")

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

    parser.add_argument('--user', help='user name for Postgres')
    parser.add_argument('--password', help='pass name for Postgres')
    parser.add_argument('--host', help='host name for Postgres')
    parser.add_argument('--port', help='port name for Postgres')
    parser.add_argument('--db', help='database name for Postgres')
    parser.add_argument('--table_name', help='name of the table where will write the results to')
    parser.add_argument('--url', help='url of the data')

    args = parser.parse_args()

    main(args)

