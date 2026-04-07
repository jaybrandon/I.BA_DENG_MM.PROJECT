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


## Dataset Overview

**Source:** Swiss Open Transport Data [https://opentransportdata.swiss](https://opentransportdata.swiss)

**Scope:**
- Only **train records** are processed  
- Other transport modes are excluded during transformation  


**Main Features**
| Feature             | Description                                      |
| ------------------- | ------------------------------------------------ |
| BETRIEBSTAG         | Operating day of the trip (date of service)      |
| FAHRT_BEZEICHNER    | Unique trip identifier (Trip ID)                 |
| BETREIBER_NAME      | Name of the transport operator                   |
| PRODUKT_ID          | Type of transport (e.g., train, bus)             |
| LINIEN_ID           | Line identifier                                  |
| LINIEN_TEXT         | Human-readable line name                         |
| VERKEHRSMITTEL_TEXT | Transport mode (e.g., RE, S-Bahn)                |
| HALTESTELLEN_NAME   | Name of the station/stop                         |
| BPUIC               | Unique station identifier                        |
| ANKUNFTSZEIT        | Scheduled arrival time                           |
| AN_PROGNOSE         | Predicted/actual arrival time                    |
| ABFAHRTSZEIT        | Scheduled departure time                         |
| AB_PROGNOSE         | Predicted/actual departure time                  |
| DURCHFAHRT_TF       | Indicates if the vehicle passes without stopping |
| FAELLT_AUS_TF       | Indicates if the trip was cancelled              |

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

### 5. Connect to PostgreSQL (pgAdmin)

1. Open pgAdmin: http://localhost:8085  
2. Login:
   - Email: admin@admin.com
   - Password: root
3. Create a new server:
   - General → Name: (any name)
   - Connection:
     - Host: pgdatabase
     - Port: 5432
     - Username: root
     - Password: root

4. Click `Save`

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
