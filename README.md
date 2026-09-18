# Airflow + MinIO Data Lake

An order-data ETL prototype combining scheduled orchestration, S3-compatible object
storage, Parquet output and DuckDB SQL queries.

**Stack:** Python, Apache Airflow, MinIO, Boto3, Pandas, PyArrow, DuckDB,
Docker Compose, PostgreSQL and Redis.

## Pipeline

1. Airflow's `local_data_lake_pipeline` DAG runs on an hourly schedule.
2. A Python sensor checks for a CSV object in the `raw-data` bucket every 60 seconds,
   with a one-hour timeout.
3. A Bash task executes `dags/etl_minio.py`.
4. The ETL reads `raw-data/orders.csv`, parses `order_date`, adds `year_month`,
   and writes `processed-data/orders/orders.parquet`.
5. DuckDB queries that Parquet object and prints order counts and total amounts by status.

The MinIO ETL writes one Parquet object. It adds a month column but does **not**
physically partition object-storage output by month. The separate `etl_local.py`
example implements monthly partitioning on the local filesystem.

## What is included

| File | Purpose |
| --- | --- |
| [dags/data_lake_pipeline.py](dags/data_lake_pipeline.py) | Sensor and ETL task dependencies |
| [dags/etl_minio.py](dags/etl_minio.py) | MinIO reads/writes and DuckDB query |
| [dags/etl_local.py](dags/etl_local.py) | Local CSV-to-partitioned-Parquet example |
| [docker-compose.yaml](docker-compose.yaml) | Airflow 3.1.0 services, PostgreSQL 16 and Redis |
| [config/airflow.cfg](config/airflow.cfg) | Mounted Airflow configuration |

## Prepare the environment

Requirements: Docker Desktop with Linux containers, a separately running MinIO
service, and an order CSV. **MinIO is not defined in the supplied Compose file.**

```shell
git clone https://github.com/ilyass74/airflow-minio-data-lake.git
cd airflow-minio-data-lake
```

Before starting Airflow:

1. Make MinIO reachable from the containers at `http://host.docker.internal:9000`.
   On native Linux, configure a host-gateway mapping or adapt the endpoint.
2. Create the `raw-data` and `processed-data` buckets. Upload the CSV as exactly
   `raw-data/orders.csv`. It must contain `order_date`, `status` and `total_amount`.
3. Align MinIO credentials and endpoint settings in both Python scripts and the
   DuckDB `SET s3_*` statements in `etl_minio.py`. These are hard-coded demo settings,
   not environment-variable configuration.
4. Create the `.env` file required by Compose. Set `AIRFLOW_UID` (50000 on Docker
   Desktop), `FERNET_KEY`, a shared `AIRFLOW__API_AUTH__JWT_SECRET`, and your local
   `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD`.
5. Set `_PIP_ADDITIONAL_REQUIREMENTS=boto3 pandas pyarrow duckdb` in `.env` so all
   Airflow task containers have the ETL dependencies. This startup installation is
   convenient for a demo; dependency versions are not locked.

For example, generate a Fernet value using Python with `cryptography` installed:

```shell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Generate a separate JWT secret and use the same value across Airflow services:

```shell
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Paste these values into your local `.env`; do not commit credentials.

## Start and inspect

After completing the configuration:

```shell
docker compose up airflow-init
docker compose up -d
docker compose ps
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Open [localhost:8080](http://localhost:8080), sign in with the account configured
above, then unpause and trigger `local_data_lake_pipeline`. Inspect the sensor and
ETL task logs and confirm that `processed-data/orders/orders.parquet` was written.
DuckDB's first `httpfs` installation requires network access.

The environment variables in Compose select CeleryExecutor and PostgreSQL; the
mounted configuration contains different defaults. Check the effective settings
with `airflow config get-value` when troubleshooting. The DAG uses a legacy
`airflow.operators.bash` import; if the import-error command reports it as missing,
update it to `airflow.providers.standard.operators.bash` for the installed provider.
These setup instructions are based on source review, not a fresh full-stack run.

## Behavior and limitations

- The sensor checks the first ten listed objects for any `.csv`, while the ETL
  always reads `orders.csv`. Uploading an arbitrary CSV is not sufficient.
- Existing input is not marked as processed: later scheduled runs can process it again.
- The fixed Parquet key is overwritten; there is no versioned table format or incremental merge.
- No automated schema, data-quality or end-to-end test suite is included.
- MinIO is an external prerequisite; `docker compose up` alone does not provide the complete pipeline.

Stop Airflow with `docker compose down`. Ordinary shutdown preserves its named
PostgreSQL volume; MinIO data is managed by the separate MinIO service.

## Related project

[Local Data Lake](https://github.com/ilyass74/data-lake-local) demonstrates the
filesystem-only version of the order transformation.
