import pandas as pd
import duckdb
from pathlib import Path

RAW_PATH = Path("data/raw/orders.csv")
PROCESSED_PATH = Path("data/processed/orders.parquet")

def extract():
    print("Extraction du CSV...")
    df = pd.read_csv(RAW_PATH)
    return df

def transform(df):
    print("Transformation...")
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['year_month'] = df['order_date'].dt.to_period('M').astype(str)
    return df

def load(df):
    print("Chargement en Parquet...")
    PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(
        PROCESSED_PATH,
        partition_cols=['year_month'],
        engine='pyarrow',
        index=False
    )
    print(f"Fichiers Parquet écrits dans {PROCESSED_PATH.parent}")

def query_duckdb():
    print("Interrogation avec DuckDB...")
    con = duckdb.connect()
    query = f"""
        SELECT status, COUNT(*) as nb_orders, SUM(total_amount) as total_amount
        FROM read_parquet('{PROCESSED_PATH}/**/*.parquet')
        GROUP BY status
        ORDER BY nb_orders DESC
    """
    result = con.execute(query).fetchdf()
    print(result)
    con.close()

if __name__ == "__main__":
    df = extract()
    df_transformed = transform(df)
    load(df_transformed)
    query_duckdb()
