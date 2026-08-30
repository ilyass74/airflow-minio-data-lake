import boto3
import pandas as pd
import duckdb
from io import StringIO, BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

# Configuration MinIO
MINIO_ENDPOINT = "http://host.docker.internal:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
RAW_BUCKET = "raw-data"
PROCESSED_BUCKET = "processed-data"

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

def extract_from_minio(key='orders.csv'):
    s3 = get_s3_client()
    response = s3.get_object(Bucket=RAW_BUCKET, Key=key)
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))
    return df

def transform(df):
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['year_month'] = df['order_date'].dt.to_period('M').astype(str)
    return df

def load_to_minio(df, prefix='orders'):
    s3 = get_s3_client()
    table = pa.Table.from_pandas(df, preserve_index=False)
    parquet_buffer = BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)
    s3.put_object(Bucket=PROCESSED_BUCKET, Key=f"{prefix}/orders.parquet", Body=parquet_buffer.getvalue())
    print(f"Fichier Parquet ?crit dans {PROCESSED_BUCKET}/{prefix}/orders.parquet")

def query_from_minio():
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("SET s3_endpoint='host.docker.internal:9000';")
    con.execute("SET s3_access_key_id='minioadmin';")
    con.execute("SET s3_secret_access_key='minioadmin';")
    con.execute("SET s3_use_ssl='false';")
    con.execute("SET s3_url_style='path';")
    query = """
        SELECT status, COUNT(*) as nb_orders, SUM(total_amount) as total_amount
        FROM read_parquet('s3://processed-data/orders/*.parquet')
        GROUP BY status
        ORDER BY nb_orders DESC
    """
    result = con.execute(query).fetchdf()
    print(result)
    con.close()

if __name__ == "__main__":
    df = extract_from_minio()
    df_transformed = transform(df)
    load_to_minio(df_transformed)
    query_from_minio()
