# 🚆 Swiss Public Transport Delay Pipeline

**Data Engineering Project – I.BA_DENG_MM.F2601 (FS 2026)**

---

## 📌 Project Overview

This project was developed as part of the *Data Engineering course (I.BA_DENG_MM.F2601)* in Spring Semester 2026.

The goal is to design and implement a **fully reproducible end-to-end batch data pipeline** using real-world data engineering tools and practices.

### 🔧 What the Pipeline Does

* Ingests raw transport data in batch mode
* Stores data in a local PostgreSQL database
* Transforms data into analysis-ready tables
* Orchestrates workflows using Kestra
* Runs fully reproducible via Docker Compose

---

## 📊 Dataset

* **Source:** Open Transport Data Switzerland
* **URL:** [https://opentransportdata.swiss](https://opentransportdata.swiss)
* **Type:** Public transport stop event data (arrivals, departures, delays)

---

## 👤 Use Case & Persona

### **Transport Data Analyst**

| Aspect       | Description                                  |
| ------------ | -------------------------------------------- |
| **Goal**     | Analyze delays in Swiss public transport     |
| **Problem**  | Raw data is large and difficult to query     |
| **Solution** | Clean dataset + aggregated delay metrics     |
| **Value**    | Identify delay hotspots & optimize transport |

---

## 🏗️ Architecture Overview

```
OpenTransportData
        ↓
Python Ingestion
        ↓
PostgreSQL (Staging)
        ↓
Transformation (SQL/Python)
        ↓
Analytics Tables
        ↓
Kestra Orchestration
```

---

## 📁 Repository Structure

```
.
├── src/
│   ├── ingestion/        # Batch ingestion scripts
│   ├── transformation/   # Transformation logic
│   └── util/             # Database utilities
│
├── kestra/
│   └── workflows/        # Kestra workflows
│
├── notebooks/
│   └── exploration.ipynb # Data exploration
│
├── docker-compose.yaml   # Full environment setup
├── pyproject.toml        # Python dependencies
└── README.md
```

---

## ⚙️ Prerequisites

If you participated in the weekly course exercises, you should already have everything set up 😉
Otherwise you need:

* Docker + Docker Compose
* Python (with `uv`)

---

## 🚀 Setup & Installation

```bash
git clone https://github.com/jaybrandon/I.BA_DENG_MM.PROJECT.git
cd I.BA_DENG_MM.PROJECT

uv sync
uv venv

# Activate environment
source .venv/bin/activate   # Linux / Mac
.venv\Scripts\activate      # Windows

docker compose up -d
```

---

## 🌐 Services & Access

| Service | URL                                            | Description            |
| ------- | ---------------------------------------------- | ---------------------- |
| Kestra  | [http://localhost:8080](http://localhost:8080) | Workflow orchestration |
| pgAdmin | [http://localhost:8085](http://localhost:8085) | Database UI            |

---

## 🔄 Running the Pipeline

### 1. Open Kestra

```
http://localhost:8080
```

### 2. Import Workflow

* Navigate to **Flows**
* Import from: `kestra/workflows/`

### 3. Execute Workflow

**Recommended (fast):**

* `ingest_current_workflow`

**Alternative (slow):**

* `backfill_workflow`


---

## 🗄️ Database Setup (pgAdmin)

### Login

* Email: `admin@admin.com`
* Password: `root`

### Create Server Connection

| Field    | Value           |
| -------- | --------------- |
| Host     | pgdatabase      |
| Port     | 5432            |
| Username | root            |
| Password | root            |

    Navigate to the tables: Servers -> Databases -> swiss_transport -> Schemas -> Tables
---

## 📊 Data Model

| Table                   | Description                  |
| ----------------------- | ---------------------------- |
| **stg_stop_events**     | Raw staging data             |
| **fact_stop_events**    | Cleaned & feature engineered |
| **station_delay_daily** | Aggregated delay metrics     |

---

## 🔍 Example Queries

```sql
-- Average delay per station starting with highest delay
SELECT 
    haltestellen_name,
    AVG(avg_delay_arrival_sec) AS avg_delay_seconds
FROM station_delay_daily
WHERE avg_delay_arrival_sec IS NOT NULL
GROUP BY haltestellen_name
ORDER BY avg_delay_seconds DESC
LIMIT 20;

-- List Stations starting with a specific letter
SELECT DISTINCT haltestellen_name
FROM station_delay_daily
WHERE haltestellen_name ILIKE 'A%'   -- replace A with any letter
ORDER BY haltestellen_name;

-- Analyze delays for your local station (replace 'Rotkreuz' with desired station to analyze)
SELECT 
    service_date,
    avg_delay_arrival_sec,
    delay_percentage
FROM station_delay_daily
WHERE haltestellen_name ILIKE '%Rotkreuz%'
ORDER BY service_date;
```

---

## 🔁 Pipeline Behavior & Automation

Once the setup is complete, the pipeline is ready for repeated execution.

- The ingestion workflow is scheduled (via Kestra) to fetch new batch data every day  
- New data is appended and processed through the transformation pipeline  
- Aggregated tables are refreshed to reflect the latest state  

### ✅ Idempotency

The pipeline is designed to be idempotent:

- Running the pipeline multiple times does not create duplicates 
- Existing data is safely refreshed or updated  
- Transformations produce consistent results across runs  

This ensures the system can run reliably in a production-like setting.

---

## 🛠️ Troubleshooting

| Issue                  | Solution                       |
| ---------------------- | ------------------------------ |
| Services not reachable | Wait after `docker compose up` |
| DB connection issues   | Check hostname `pgdatabase`    |
| Workflow issues        | Check Kestra logs              |

---

## 🔮 Future Work

* Terraform (GCS + BigQuery)
* Cloud ingestion pipeline
* Cloud transformation pipeline
