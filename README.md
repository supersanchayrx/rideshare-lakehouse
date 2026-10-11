# Rideshare Lakehouse

A batch ETL pipeline that turns New York City's public rideshare trip records into a partitioned data lake and a small warehouse. Bad rows are caught before they reach either one.

> **Status: in progress.** The roadmap below tracks what is done. Measured results are added to `BENCHMARKS.md` as each part lands.

## The problem

Trip data from a ride-hailing platform arrives every month as large files. Some rows are broken: fares below zero, a dropoff logged before the pickup, a zone ID that doesn't exist. If those rows flow straight into reports, every number downstream is a little wrong and nobody can tell which ones.

This project builds the pipeline a data team would run on that feed:
- load each month once, so a rerun never creates duplicates;
- check every row against explicit rules, and set failing rows aside with the reason they failed;
- store clean trips in a format that is cheap to query by date;
- serve hourly aggregates from a warehouse.

## Data

[NYC TLC High Volume For-Hire Vehicle trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page): every trip dispatched by the large app-based services (Uber, Lyft and others), published monthly since February 2019.

- **Size:** about 19.7–19.9M trips a month. Each month is one Parquet file of 470–510 MB. These figures come from the file metadata for January 2024 and June 2025.
- **Columns (25):**
  - request, on-scene, pickup and dropoff timestamps;
  - pickup and dropoff zone IDs;
  - trip miles and trip time;
  - fare components, tips and driver pay;
  - shared-ride and accessibility flags.
- **Lookup:** the [taxi zone table](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) maps the 265 zone IDs to a borough and zone name.

## Pipeline

```
TLC monthly Parquet ──> raw zone (object storage, files kept as published)
                              │
                     data quality checks ──> quarantine table (row + failed rule)
                              │
                     PySpark transform: clean types, derive duration, speed,
                     fare per mile, hour and weekday
                              │
                 ┌────────────┴─────────────┐
                 ▼                          ▼
   curated Parquet lake            Postgres warehouse (star schema)
   partitioned by pickup date      fact_trip_hourly: zone × hour × company
                                   dim_zone, dim_date, dim_company
```

Airflow runs one DAG per month: ingest → validate → transform → load lake → load warehouse. Each task overwrites only its own month's partition, so a rerun or backfill gives the same result as the first run.

Object storage is AWS S3, or MinIO when running locally. Both use the same S3 API, so the code doesn't change.

## Data quality rules

Each rule has a name, so the quarantine table can report how many rows each rule caught.

| Rule | Check |
|---|---|
| Time order | request ≤ pickup < dropoff |
| Duration | trip time is positive and under a sane maximum |
| Distance | trip miles is non-negative and under a sane maximum |
| Money | fare, tips and driver pay are non-negative |
| Zones | pickup and dropoff zone IDs exist in the zone table |
| Completeness | required columns are not null |
| Month | pickup timestamp falls inside the file's month |

## What will be measured

These go in `BENCHMARKS.md` with the hardware and method used, once they exist:

- rows and GB processed per month and in total;
- runtime of a full reload vs an incremental monthly run;
- share of rows quarantined, broken down by rule;
- query time with and without partition pruning on the lake;
- storage per month: raw vs curated Parquet;
- idempotency: running the same month twice leaves row counts unchanged.

## Roadmap

- [x] Scope the project and choose the dataset
- [ ] Ingest one month into the raw zone
- [ ] Data quality rules and quarantine table
- [ ] PySpark transform into the date-partitioned lake
- [ ] Postgres star schema and warehouse load
- [ ] Airflow DAG with incremental, idempotent monthly runs and backfill
- [ ] Benchmarks (`BENCHMARKS.md`)
- [ ] Write-up

## Planned structure

```
dags/            Airflow DAG: ingest, validate, transform, load
jobs/            PySpark jobs
quality/         data quality rules
sql/             warehouse DDL and example queries
benchmarks/      benchmark scripts and BENCHMARKS.md
data/            local downloads (git-ignored)
```

## Setup

```bash
python -m venv .venv
pip install -r requirements.txt
```

Airflow, Postgres and MinIO will run in Docker Compose. That file arrives with the DAG.
