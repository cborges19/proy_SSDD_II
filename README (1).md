# Airbnb Malaga — Distributed Data Processing Pipeline

End-to-end data platform for analysing Airbnb accommodation data in Malaga
(Inside Airbnb dataset, September 2025). The project covers three academic
phases: ETL orchestration with Airflow, real-time analytics with Kafka and
Spark Structured Streaming, and an interactive Streamlit application backed
by scikit-learn ML models served over Kafka.

---

## Table of Contents

1. [Project Structure](#1-project-structure)
2. [Prerequisites](#2-prerequisites)
3. [Getting Started — Infrastructure Setup](#3-getting-started--infrastructure-setup)
4. [Phase 1 — Airflow ETL Pipelines](#4-phase-1--airflow-etl-pipelines)
5. [Phase 2 — Spark Structured Streaming Queries](#5-phase-2--spark-structured-streaming-queries)
6. [Phase 3 — Interactive Streamlit Application](#6-phase-3--interactive-streamlit-application)
7. [Kafka Topics Reference](#7-kafka-topics-reference)
8. [Configuration](#8-configuration)

---

## 1. Project Structure

```
.
├── config.toml                     # Central configuration (Kafka, paths)
├── docker-compose.yml              # Kafka + Schema Registry (Docker)
├── pyproject.toml / uv.lock        # Dependency management (uv)
├── check_kafka.sh                  # Kafka inspection utility script
├── data/
│   ├── raw/                        # Source CSVs (listings, calendar, reviews)
│   └── output/
│       ├── listings_gold.parquet   # Written by Airflow — read by Streamlit
│       └── reports/                # Auto-generated HTML EDA dashboards
├── models/                         # Trained scikit-learn artefacts (.pkl)
│   ├── price_model.pkl
│   └── knn_model.pkl
├── notebooks/
│   ├── eda/                        # Exploratory analysis
│   ├── prototyping/                # DAG prototyping
│   ├── queries/                    # Interactive streaming queries + live map
│   │   ├── calendar_query.ipynb
│   │   ├── reviews_query.ipynb
│   │   └── map_reviews_query.ipynb
│   └── models/
│       └── model_selection.py      # MLflow model comparison script
└── src/
    ├── utils.py                    # Shared transformation utilities
    ├── kafka/
    │   ├── consumer_kafka.py       # Standalone Kafka consumer / monitor
    │   └── producer_kafka.py       # Avro producer + error DLQ producer
    ├── queries/                    # Productionised Spark Streaming scripts
    │   ├── calendar_query.py       # Phase 2 — occupancy windows
    │   ├── reviews_query.py        # Phase 2 — VADER sentiment
    │   ├── error_counter.py        # Phase 2 — DLQ monitor
    │   └── ml_processor.py         # Phase 3 — RF + KNN inference over Kafka
    ├── transformations/            # Airflow DAGs
    │   ├── dag_listings.py
    │   ├── dag_calendar.py
    │   └── dag_reviews.py
    ├── models/
    │   └── train_models.py         # Offline model training (RF + KNN)
    ├── interface/                  # Streamlit application
    │   ├── app.py
    │   ├── kafka_client.py
    │   └── components/
    │       ├── tab_search.py
    │       ├── tab_prediction.py
    │       └── tab_suggestions.py
    └── reports/                    # EDA logic + HTML templates
        ├── report_listings.py
        ├── report_calendar.py
        └── report_reviews.py
```

---

## 2. Prerequisites

- Docker Desktop installed and running
- Python 3.10 or higher
- `uv` package manager: `pip install uv`
- All source CSVs in `data/raw/` (see Step 0 below)

---

## 3. Getting Started — Infrastructure Setup

### Step 0 — Prepare the source data

The raw CSVs are not included in the repository. Download the Malaga dataset
from Inside Airbnb and place the decompressed files in `data/raw/`:

```
https://insideairbnb.com/get-the-data/
```

Required files: `listings.csv`, `calendar.csv`, `reviews.csv`

### Step 1 — Start Kafka

```bash
docker compose up -d
```

This starts:
- Apache Kafka 7.4 (KRaft mode, no ZooKeeper) on port 9092
- Confluent Schema Registry on port 8081

### Step 2 — Install Python dependencies

```bash
uv sync
```

This creates a `.venv` and installs all pinned dependencies from `uv.lock`.

### Step 3 — Configure environment variables

The following variables must be set in every terminal session used for Airflow.

**Linux / macOS (Bash or Zsh):**
```bash
export AIRFLOW_HOME=$(pwd)
export PYTHONPATH=$(pwd)
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/src/transformations
```

**Windows (PowerShell):**
```powershell
$env:AIRFLOW_HOME=$PWD
$env:PYTHONPATH=$PWD
$env:AIRFLOW__CORE__DAGS_FOLDER="$PWD\src\transformations"
```

**Windows (CMD):**
```cmd
set AIRFLOW_HOME=%cd%
set PYTHONPATH=%cd%
set AIRFLOW__CORE__DAGS_FOLDER=%cd%\src\transformations
```

---

## 4. Phase 1 — Airflow ETL Pipelines

### Start Airflow

```bash
uv run airflow standalone
```

This initialises the SQLite database, creates an `admin` user, starts the
scheduler and web server simultaneously. The generated password is printed
to the terminal and saved to `standalone_admin_password.txt`.

Access the UI at: `http://localhost:8080`

### Run the DAGs

1. Open `http://localhost:8080` and log in with `admin` / generated password
2. Enable each DAG by toggling the switch on the left
3. Trigger a run by clicking the play button under Actions

| DAG | Datasets | Key steps |
|-----|----------|-----------|
| `airbnb_master_pipeline` | listings.csv | Extract, Transform (40+ ops), Enrich, Validate, EDA report, save Gold parquet, Load to Kafka |
| `airbnb_calendar_pipeline` | calendar.csv | Extract, Transform, Enrich (event mapping), Validate, EDA report, Load to Kafka |
| `airbnb_reviews_pipeline` | reviews.csv | Extract, Transform (NLP clean), Enrich, Validate, EDA report, Load to Kafka |

### Results after ETL

After a successful run:

- **Gold parquet:** `data/output/listings_gold.parquet` (required by Phase 3)
- **Kafka topics:** `airbnb_listings_gold`, `airbnb_calendar_gold`, `airbnb_reviews_gold` (Avro)
- **HTML dashboards:** `data/output/reports/listings/`, `/calendar/`, `/reviews/`

### Verify Kafka data

```bash
chmod +x check_kafka.sh

# Inspect Gold topics
./check_kafka.sh airbnb_listings_gold 1
./check_kafka.sh airbnb_reviews_gold 1
./check_kafka.sh airbnb_calendar_gold 1

# Inspect validation failures (Dead Letter Queue)
./check_kafka.sh pipeline_errors 5
```

### Manual Kafka consumer (without Spark)

Activate the virtualenv first:

```bash
# Linux / macOS
source .venv/bin/activate

# Windows PowerShell
.venv\Scripts\Activate.ps1

# Windows CMD
.venv\Scripts\activate.bat
```

Then run:

```bash
# Linux / macOS
python3 src/kafka/consumer_kafka.py airbnb_listings_gold

# Windows
python src\kafka\consumer_kafka.py airbnb_listings_gold
```

Allowed topic names: `airbnb_listings_gold`, `airbnb_calendar_gold`,
`airbnb_reviews_gold`, `pipeline_errors`

---

## 5. Phase 2 — Spark Structured Streaming Queries

These queries read from the Kafka Gold topics populated by Phase 1.
The Spark session automatically downloads required JARs on first run.

> **Output note:** Results are redirected to text files using the OS `>`
> operator rather than a Spark file sink, because `complete` output mode
> (required for global aggregation tables) is not supported by file sinks
> in Spark Structured Streaming.

### Query 1 — Calendar occupancy (30-day sliding windows)

```bash
# Linux / macOS
python3 src/queries/calendar_query.py > data/output/salida_1.txt

# Windows
python src\queries\calendar_query.py > data\output\salida_1.txt
```

Computes mean occupancy and total bookings per 30-day window (7-day slide)
with a 7-day watermark.

### Query 2 — Review sentiment classification (annual windows)

```bash
# Linux / macOS
python3 src/queries/reviews_query.py > data/output/salida_2.txt

# Windows
python src\queries\reviews_query.py > data\output\salida_2.txt
```

Applies VADER sentiment analysis via Pandas UDF and groups by 365-day
tumbling windows and sentiment category (positive / negative / informational).

### Query 3 — Validation error monitor

```bash
# Linux / macOS
python3 src/queries/error_counter.py > data/output/salida_3.txt

# Windows
python src\queries\error_counter.py > data\output\salida_3.txt
```

Reads the `pipeline_errors` DLQ and displays all validation failures in
append mode.

### Query 4 — Geospatial sentiment map (notebook)

Open and run `notebooks/queries/map_reviews_query.ipynb`. This performs a
stream-stream join between listings (coordinates) and reviews (sentiment),
rendering a live Folium map coloured by sentiment type.

---

## 6. Phase 3 — Interactive Streamlit Application

This phase adds a web interface on top of the existing pipeline. It requires
the Gold parquet from Phase 1 and optionally the Spark ML processor for
live inference.

### Step A — Install additional dependencies

```bash
uv add streamlit folium streamlit-folium branca mlflow
# Optional but recommended for best model performance:
uv add xgboost
```

### Step B — Train the ML models

Run once after each Airflow ETL execution:

```bash
python src/models/train_models.py
```

This reads `data/output/listings_gold.parquet`, trains a Random Forest price
prediction pipeline and a NearestNeighbors KNN recommendation model, and
saves both to `models/`.

To compare models and explore results in MLflow, use the model selection script:

```bash
python notebooks/models/model_selection.py
mlflow ui --port 5000
# Open http://localhost:5000
```

### Step C — Start the Spark ML Processor (optional, for streaming tabs)

Required only for Tabs 2 and 3 (Kafka-based price prediction and recommendations).
Run in a dedicated terminal and keep it running:

```bash
# Linux / macOS
python3 src/queries/ml_processor.py

# Windows
python src\queries\ml_processor.py
```

This subscribes to `topic_peticiones` and `topic_likes` and publishes results
to `topic_precios` and `topic_sugerencias`.

### Step D — Launch the Streamlit application

```bash
# Linux / macOS
streamlit run src/interface/app.py

# Windows
streamlit run src\interface\app.py
```

Open the browser at: `http://localhost:8501`

### Application tabs

| Tab | Layer | Description |
|-----|-------|-------------|
| Search & Map | Batch | Filters 20K+ listings; Folium tile map with clustering and satellite toggle; CSV export |
| Price Prediction | Streaming | Host form → Kafka → Spark RF model → predicted price + neighbourhood benchmark |
| Live Suggestions | Streaming | Like a listing → Kafka → Spark KNN → top-3 similar listings with cosine similarity scores |

Tab 2 includes a local-inference fallback toggle for demos without Spark running.

---

## 7. Kafka Topics Reference

| Topic | Format | Direction | Purpose |
|-------|--------|-----------|---------|
| `airbnb_listings_gold` | Avro | Airflow → consumers | Enriched listings Gold layer |
| `airbnb_calendar_gold` | Avro | Airflow → consumers | Enriched calendar Gold layer |
| `airbnb_reviews_gold` | Avro | Airflow → consumers | Enriched reviews Gold layer |
| `pipeline_errors` | JSON | Airflow → monitor | Validation failure Dead Letter Queue |
| `topic_peticiones` | JSON | Streamlit → Spark | Price prediction requests |
| `topic_precios` | JSON | Spark → Streamlit | Price prediction responses |
| `topic_likes` | JSON | Streamlit → Spark | Recommendation requests (like events) |
| `topic_sugerencias` | JSON | Spark → Streamlit | KNN recommendation responses |

---

## 8. Configuration

All environment-specific values are centralised in `config.toml`:

```toml
[paths]
data_dir      = "data/raw"
output_dir    = "data/output"
templates_dir = "src/reports"

[kafka]
bootstrap_servers  = "localhost:9092"
schema_registry_url = "http://localhost:8081"
```

Airflow runs outside the Docker network, so Kafka must be referenced
via `localhost:9092`, not the internal Docker hostname `kafka:29092`.
