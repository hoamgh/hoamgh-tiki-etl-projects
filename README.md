# Tiki ETL Project

An ELT pipeline that crawls electronics products from Tiki.vn, stores raw data on AWS S3, transforms with Apache Spark, and loads into SQL Server for analysis.

```
Python Crawler → AWS S3 (raw CSV) → Spark Scala ETL → SQL Server
                         ↑
              Airflow DAG (daily 2AM)
```

## Tech Stack

- **Python** — crawler, brand classification, S3 upload
- **Apache Spark (Scala)** — data transformation & normalization
- **AWS S3** — raw data storage
- **SQL Server** — serving layer (4 tables)
- **Apache Airflow** — pipeline orchestration (Docker)

## Project Structure

```
├── main.py                        # Entry point: crawl + upload to S3
├── config.json                    # Pipeline config
├── .env                           # AWS & SQL credentials (not committed)
├── crawler/
│   ├── scraper.py                 # Tiki API crawler (pagination + retry)
│   ├── brand_classifier.py        # Global Brand vs Local/OEM Generic
│   ├── category_mapper.py         # Recursive leaf-category discovery
│   ├── s3_sink.py                 # Upload snapshot to S3
│   └── sqlserver_sink.py          # Direct SQL sync (optional mode)
├── spark/
│   └── src/TikiRawToSqlJob.scala  # Spark job: S3 CSV → SQL Server
└── airflow/
    └── dags/tiki_lakehouse_dag.py # Airflow DAG
```

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env  # fill in your credentials
```

### `.env` variables

```dotenv
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=ap-southeast-2
S3_BUCKET_NAME=your-bucket
S3_RAW_PREFIX=raw

SQLSERVER_JDBC_URL=jdbc:sqlserver://localhost\SQLEXPRESS;databaseName=TikiAnalysis2026;...
SQLSERVER_USER=...
SQLSERVER_PASSWORD=...
SQLSERVER_TABLE_PREFIX=dbo
```

## Running

### 1. Run the crawler

```bash
python main.py
# or with a specific config:
python main.py config.live.json
```

Outputs a raw snapshot to S3: `s3://<bucket>/raw/run_YYYYMMDD_HHMMSS.csv`

### 2. Run the Spark job

```bash
cd spark
sbt -batch package

spark-submit \
  --packages com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11 \
  --class TikiRawToSqlJob \
  target/scala-2.12/tiki-spark-etl_2.12-0.1.0.jar \
  --input s3a://your-bucket/raw/run_YYYYMMDD_HHMMSS.csv \
  --jdbc-url "jdbc:sqlserver://localhost\SQLEXPRESS;databaseName=TikiAnalysis2026;encrypt=true;trustServerCertificate=true" \
  --table-prefix dbo
```

The job reads from S3 and writes 4 normalized tables: `dbo.Categories`, `dbo.Brands`, `dbo.Sellers`, `dbo.Products`.

> Add `--dry-run true` to validate transform logic without writing to SQL Server.

### 3. Run with Airflow (Docker)

```bash
docker compose -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

Open **http://localhost:8080** — `admin` / `admin`

DAG: `crawl_raw_to_s3 >> spark_transform_to_sql >> validate_sql_connection`

#### Airflow Variables to set

| Variable             | Default                  | Description                            |
| -------------------- | ------------------------ | -------------------------------------- |
| `project_dir`      | `/opt/airflow/project` | Path to project inside container       |
| `python_bin`       | `python`               | Python executable                      |
| `spark_submit_bin` | `spark-submit`         | spark-submit executable                |
| `crawl_config`     | `config.live.json`     | Config file for the crawler            |
| `run_spark`        | `true`                 | Enable/disable the Spark step          |
| `run_sql_validate` | `false`                | Enable/disable the SQL validation step |

## Pipeline Modes

Set `orchestration.pipeline_mode` in `config.json`:

| Mode                  | Description                                      |
| --------------------- | ------------------------------------------------ |
| `s3_raw_then_spark` | *(default)* Upload to S3, then run Spark job   |
| `python_direct_sql` | Crawler writes directly to SQL Server (no Spark) |
