# ⚽ Football Analytics Lakehouse – End‑to‑End Data Engineering Project

This repository contains a **production‑shaped local lakehouse pipeline** built to simulate real‑world data engineering workflows using Apache Spark and Prefect.

The project ingests raw football datasets, models them into analytical tables, and orchestrates the entire Bronze → Silver → Gold lifecycle with validation, retries, and monitoring.

This is not a tutorial project. It is designed to practice **engineering thinking**: contracts between layers, standalone jobs, deterministic outputs, and operational recovery.

---

## 🏗 Architecture (High Level)

```
Raw CSV
   ↓
Bronze (Parquet, faithful ingestion)
   ↓
Silver (modeled dimensions)
   ↓
Gold (facts + KPIs)

Prefect
   ↓
Spark Jobs (Bronze → Silver → Gold)
```

Each layer is implemented as an **independent Spark job**. Prefect orchestrates execution order, retries failures, and surfaces pipeline state.

---

## 🧰 Tech Stack

- Python 3
- Apache Spark (PySpark)
- Parquet
- Prefect (local‑first orchestration)
- Linux / WSL

---

## 📂 Project Structure

```
football-lakehouse/
│
├── raw/                     # Original CSV datasets
├── bronze/                  # Raw → Parquet landing zone
├── silver/                  # Modeled dimensions
├── gold/                    # Facts + KPI tables
│
├── pipelines/
│   ├── bronze/
│   │   └── bronze_ingest.py
│   ├── silver/
│   │   └── silver_transform.py
│   ├── gold/
│   │   └── gold_fact_player_match.py
│   └── main_pipeline_flow.py
│
└── README.md
```

---

## ✅ Implemented Features

### Bronze Layer
- CSV ingestion via Spark
- Folder‑per‑table Parquet layout
- Row manifests
- Fail‑fast on empty tables

### Silver Layer
- Snake_case normalization
- Type casting
- Domain column selection
- Deduplication on business keys
- Basic data quality checks

### Gold Layer
- `fact_player_match` (player × game spine)
- Player KPIs
- Player‑season KPIs
- Club‑season KPIs
- Metrics: matches, minutes, goals, assists, goal involvement, per90, cards
- Strict validation before write

### Orchestration (Prefect)
- Unified Bronze → Silver → Gold pipeline
- Standalone Spark jobs per layer
- Retry logic per task
- Flow‑level monitoring
- Fail‑fast semantics

---

## ▶ Running the Full Pipeline

From project root:

```bash
python pipelines/main_pipeline_flow.py
```

This executes:

```
Bronze → Silver → Gold
```

with retries and validation.

---

## ▶ Running Individual Layers

Bronze:

```bash
python pipelines/bronze/bronze_ingest.py \
  --raw-path /home/gnana/football-lakehouse/raw \
  --bronze-path /home/gnana/football-lakehouse/bronze
```

Silver:

```bash
python pipelines/silver/silver_transform.py \
  --bronze-path /home/gnana/football-lakehouse/bronze \
  --silver-path /home/gnana/football-lakehouse/silver
```

Gold:

```bash
python pipelines/gold/gold_fact_player_match.py \
  --silver-path /home/gnana/football-lakehouse/silver \
  --gold-path /home/gnana/football-lakehouse/gold
```

---

## 🧠 Engineering Principles Practiced

- Layered lakehouse design (Bronze / Silver / Gold)
- Standalone, parameterized Spark jobs
- Idempotent writes
- Explicit validation and failure propagation
- Orchestration separated from business logic
- Row‑count verification
- Deterministic rebuilds

---

## 🚀 Roadmap

Next phases include:

- Backfills (partial reprocessing)
- Scheduling
- Streaming simulation
- Data quality contracts
- Feature tables + ML bridge

---

## 📌 Why This Project

This project focuses on **systems thinking**, not just Spark usage:

- Designing contracts between layers
- Building recoverable pipelines
- Treating failures as first‑class citizens
- Operating data workflows like production systems

---

## Author

Built as part of a professional data engineering roadmap.

