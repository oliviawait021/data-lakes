# Data Lake ETL Pipeline

This project implements a two-layer data lake for a fictional online retailer, processing clickstream events and order transactions. I built the full pipeline: raw ingestion into S3-compatible object storage, transformation to partitioned Parquet format, and SQL querying via Trino with Hive Metastore — demonstrating the same architecture used by companies running petabyte-scale analytics.

## Tech Stack

- **MinIO** (S3-compatible object storage)
- **Trino** (distributed SQL query engine)
- **Apache Hive Metastore** · **MariaDB**
- **Python 3.12** · **boto3** · **pandas** · **PyArrow** · **tqdm**
- **Docker / Docker Compose**
- **uv** (Python package manager)

## Architecture

```
raw-inputs/          →   data-lake/raw/          →   data-lake/analytics/
(JSON + CSV files)       (partitioned by type)        (partitioned Parquet)
                                                             ↓
                                                    Trino + Hive Metastore
                                                    (SQL query interface)
```

Two data domains, each partitioned by `platform/year/month/day`:
- **Clickstream** (JSON source)
- **Orders** (CSV source)

## Key Features

- **Two-layer lake design** — raw layer preserves source files untouched; analytics layer stores clean, typed Parquet for query efficiency
- **Metadata-driven ingestion** — platform and date metadata is embedded in source files and extracted at ingest time; falls back to filename parsing if missing
- **Schema normalization** — clickstream JSON has variable structure; the transformer applies fallback logic to align all records to a consistent schema before writing Parquet
- **Partition pruning** — Trino skips irrelevant partitions using directory structure; queries on partitioned tables run ~30x faster than on an equivalent non-partitioned table (validated with `EXPLAIN ANALYZE`)
- **Idempotent transformation** — re-running the transform script overwrites existing partitions cleanly

## What I Learned

- How data lake layering (raw → analytics) separates storage concerns from query concerns
- Why Parquet + partitioning is the standard for large-scale analytics (columnar reads + partition pruning)
- How Trino uses Hive Metastore to maintain table/partition metadata without a traditional RDBMS
- Handling semi-structured source data with inconsistent schemas in a production pipeline

## Running Locally

**Prerequisites:** Docker, Python 3.12+, `uv`

```bash
# Start all services (MinIO, Trino, Hive Metastore, MariaDB)
docker compose up -d   # from aqua-node/

# Install Python dependencies
uv sync

# Unzip raw data files into raw-inputs/

# 1. Ingest raw files into the lake
uv run python populate_data_lake.py

# 2. Transform raw files to Parquet
uv run python transform_raw_data.py

# 3. Initialize Trino tables
docker exec -it trino trino
# Then run commands from trino_init.sql
```

MinIO UI: `http://localhost:9000`
