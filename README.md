# 🚆 Swiss Train Delay Data Pipeline

**Data Engineering Project – I.BA_DENG_MM.F2601 (FS 2026)**

---


## Overview

This project implements an end-to-end batch data pipeline for analyzing delays in Swiss public transport, focusing exclusively on train operations.

The pipeline ingests raw operational data, processes it into structured tables, and provides aggregated delay metrics for downstream analysis.

The system is fully containerized and orchestrated using Kestra, ensuring reproducibility and ease of use

---


## Architecture

The pipeline follows a batch ELT approach:

1. **Ingestion**
   - Downloads raw data from Swiss Open Transport Data

2. **Staging**
   - Loads raw CSV data into a staging table

3. **Transformation**
   - Filters to train-only records 
   - Parses timestamps and normalizes schema  
   - Computes delay metrics and derived features  
   - Deduplicates records using a generated event key  

4. **Final Tables**
   - `fact_stop_events` (event-level data)
   - `station_delay_daily` (aggregated metrics)

5. **Orchestration**
   - Managed via Kestra workflows and subflows  

---


## Dataset

**Source:** Swiss Open Transport Data [https://opentransportdata.swiss](https://opentransportdata.swiss)

**Scope:**
- Only **train records** are processed  
- Other transport modes are excluded during transformation  


**Data includes:**
- Identifiers (event, train, operator)
- Station-level info
- Timestamps (scheduled vs predicted)
- Derived delay metrics
---


## Tech Stack

- Python (data ingestion and transformation)
- PostgreSQL (data storage)
- pgAdmin (database interface)
- Kestra (workflow orchestration)
- Docker Compose (environment setup)

---


## Project Structure

```
.
├── docker-compose.yml
├── kestra/
│ ├── ingest_current_workflow.yaml
│ ├── backfill_workflow.yaml
│ └── elt_file_workflow.yaml
├── src/
│ ├── ingestion/
│ │ ├── ingest_current.py
│ │ └── ingest_backfill.py
│ ├── transformation/
│ │ └── stop_event_transformation.py
│ └── util/
├── README.md
```

---


## Prerequisites
Ensure the following tools are installed on your system:

- **Docker** (including Docker Compose)

### Notes

- The pipeline is designed to run fully inside Docker.  
- No local Python setup is required unless you want to execute scripts manually.

---


## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/jaybrandon/I.BA_DENG_MM.PROJECT.git

cd I.BA_DENG_MM.PROJECT

uv sync
uv venv

# Activate environment
source .venv/bin/activate   # Linux / Mac
.venv\Scripts\activate      # Windows
```

---

### 2. Start all services

 ```bash
docker compose up -d
```

---

### 3. Access services

- **Kestra UI:** http://localhost:8080  
- **pgAdmin:** http://localhost:8085  

---

### 4. Run the pipeline

#### Option A: Ingest latest data

1. Open Kestra UI  
2. Navigate to flows  
3. Execute: ingest_current_data


#### Option B: Backfill historical data

1. Open Kestra UI  
2. Navigate to flows  
3. Execute: backfill_data  
4. Provide:
   - Month (MM)  
   - Year  

---

### 5. Verify results

Connect to PostgreSQL via pgAdmin and check:

- `fact_stop_events`
- `station_delay_daily`

How to connect to PostgreSQL will be discussed in the following section.

---

## Database Connection

### Login

* Email: `admin@admin.com`
* Password: `root`

---

### Create Server Connection

1. Click `Add new Server`
2. Enter a Server-Name in the `General` tab
3. Switch to the `Connection` tab and fill in the content of following table


| Field    | Value           |
| -------- | --------------- |
| Host     | pgdatabase      |
| Port     | 5432            |
| Username | root            |
| Password | root            |

4. Click `Save`

---
### Verify Server Connection & Existence of tables

Now a new Server should appear on the left hand side.

You can check if the tables exist by clicking:

```
Servers -> <Your Server Name> -> swiss-transport -> schemas -> tables
```

There you should see:

| Table                   | Description                  |
| ----------------------- | ---------------------------- |
| **stg_stop_events**     | Raw staging data             |
| **fact_stop_events**    | Cleaned & feature engineered |
| **station_delay_daily** | Aggregated delay metrics     |

---

## Example Queries

Now you can click on the Query tool and explore the Data yourself

```sql
-- Average delay per station starting with highest delay
SELECT 
    haltestellen_name,
    AVG(avg_delay_seconds) AS avg_delay_seconds
FROM station_delay_daily
WHERE avg_delay_seconds IS NOT NULL
GROUP BY haltestellen_name
ORDER BY avg_delay_seconds DESC
LIMIT 20;

-- List Stations starting with a specific letter
--You can use this to look for a station you want to use in the next Query
SELECT DISTINCT haltestellen_name
FROM station_delay_daily
WHERE haltestellen_name ILIKE 'A%'   -- replace A with any letter
ORDER BY haltestellen_name;

-- Analyze delays for your local station (replace 'Rotkreuz' with desired station to analyze)
SELECT 
    service_date,
    avg_delay_seconds,
    delay_percentage
FROM station_delay_daily
WHERE haltestellen_name ILIKE '%Rotkreuz%'
ORDER BY service_date;
```
---


## Pipeline Behavior & Automation

Once the setup is complete, the pipeline is ready for repeated execution.

- The ingestion workflow is scheduled (via Kestra) to fetch new batch data every day at 2:00 am.
- New data is appended and processed through the transformation pipeline  
- Aggregated tables are refreshed to reflect the latest state  

---


## Future Work

* Terraform (GCS + BigQuery)
* Cloud ingestion pipeline
* Cloud transformation pipeline
