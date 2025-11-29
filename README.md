#  Modular ETL Pipeline — Azure Blob → Snowflake & Snowflake Parallel Ingestion!!!

This repository contains a robust ETL framework that ingests:

- **Azure Blob Storage → Snowflake (CSV ingestion)**
- **Snowflake Source → Snowflake Target migration**

with:

✔ L1 / TEMP → L2 Final promotion  
✔ Slack summary reporting  
✔ Ingestion logging  
✔ Parallel execution for faster pipelines  
✔ Fully environment-driven configuration  

---

## ✨ Key Features

| Feature | Description |
|---|---|
| Azure CSV ingestion | Loads CSVs → Pandas → Snowflake L1 → promotes to L2 |
| Snowflake → Snowflake sync | Auto-detects source tables and ingests all in parallel |
| L1 / L2 ingestion model | Prevents corruption + ensures data lineage visibility |
| Slack monitoring | Sends completion summary + mismatch alerts |
| Ingestion logging table | Tracks row counts, timestamp, layer, and source |
| Modular + scalable | Clean structure for adding more connectors or DQ checks |

---

## 📂 Repository Structure

```
etl-pipeline/
├── src/
│   ├── azure_ingest.py
│   ├── snowflake_ingest.py
│   ├── config.py
│   ├── connections.py
│   ├── slack_utils.py
│   ├── logging_utils.py
│   ├── main.py
│   └── helpers.py (optional placeholder)
├── config/
│   └── .env.example
├── requirements.txt
└── README.md
```

---

## 🧠 ETL Process Flow

### Azure Blob → Snowflake

1. Discover CSVs or load from AZURE_FILES list  
2. Convert to DataFrame  
3. Insert into `<TABLE>_TEMP` (L1)  
4. Promote into `<TABLE>` (L2)  
5. Record ingestion metrics  
6. Slack + log output  

### Snowflake → Snowflake (Parallel)

| Stage | Operation |
|---|---|
| Table scan | Reads all tables from INFORMATION_SCHEMA |
| Parallel execution | ThreadPool spawns workers per table |
| Load to L1 | `_TEMP` staging per table |
| Promote to L2 | Replace existing final table after null checks|
| Logging | Stores record counts for traceability |

---

## 🔐 .env Configuration (Required)

```
SLACK_TOKEN=
SLACK_CHANNEL=#etl-alerts

AZURE_CONNECTION_STRING=
AZURE_CONTAINER=
AZURE_FILES=      # comma separated or empty = autodetect

SF_SOURCE_USER=
SF_SOURCE_ACCOUNT=
SF_SOURCE_PRIVATE_KEY=
SF_SOURCE_WAREHOUSE=
SF_SOURCE_DATABASE=
SF_SOURCE_SCHEMA=
SF_SOURCE_ROLE=

SF_TARGET_USER=
SF_TARGET_ACCOUNT=
SF_TARGET_PRIVATE_KEY=
SF_TARGET_WAREHOUSE=
SF_TARGET_DATABASE=
SF_TARGET_SCHEMA=
SF_TARGET_ROLE=

LOG_TABLE=ETL_DB.ETL_SCHEMA.INGESTION_LOGS
```

Copy `.env.example` → `.env` and populate.

---

## 📦 Installation

```
pip install -r requirements.txt
```

---

## ▶️ Run the Pipeline

```
python src/main.py
```

---

## 📡 Slack Summary Output Example

```
ETL SUMMARY — 12 Feb 2025 09:48PM
────────────────────────────────────────
TABLE_1         SRC=10XXXX  FINAL=10XXX   ✓
TABLE_2         SRC=YYYY   FINAL=YYYY    ✓
TABLE_3         SRC=0      FINAL=0       skipped
TABLE_4         SRC=1500   FINAL=1487    ⚠ mismatch
```

---

## 🛠 Requirements

```
pandas
python-dotenv
requests
snowflake-connector-python
azure-storage-blob
sqlalchemy
```

---

## 🔥 Optional Enhancements

| Suggestion | Benefit |
|---|---|
| Add Data Quality layer | threshold rules, schema drift alerts |
| Enable CDC / MERGE logic | Avoids full reload, incremental ingest supported |
| Convert to Airflow / Prefect DAG | Scheduling + monitoring + retries |
| Add AWS S3/GCS handlers | Multi-cloud ingestion architecture |

---

## 🧾 .gitignore Recommended

```
.env
*.p8
__pycache__/
*.log
```

---

## 📄 License

MIT License (recommended for open-source use)

```
MIT License
Copyright (c) 2025
Permission is hereby granted, free of charge, to any person obtaining a copy
...
```

---

## 🤝 Contributions

PRs, discussions, and extensions welcome! Add new connectors, DQ validation modules, or orchestration integrations and help grow the framework. Credit Would be Appreciated
