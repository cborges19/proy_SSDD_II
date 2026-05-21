# Airbnb Málaga · Streamlit Interface

Interactive web dashboard that wraps the full data pipeline with a
three-tab UI: batch search, streaming price prediction, and live KNN
recommendations.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Streamlit App                                │
│   Tab 1 · Search & Map    Tab 2 · Price Pred.   Tab 3 · Suggestions │
│   ┌─────────────────┐     ┌───────────────────┐  ┌───────────────┐  │
│   │  reads directly │     │ Kafka Producer    │  │ Kafka Producer│  │
│   │  from parquet   │     │ topic_peticiones  │  │ topic_likes   │  │
│   └────────┬────────┘     └────────┬──────────┘  └───────┬───────┘  │
│            │                       │                      │          │
│            ▼                       ▼                      ▼          │
│   listings_gold.parquet      Spark ML Processor (ml_processor.py)   │
│   (written by Airflow)        ┌────────────────────────────────┐    │
│                               │  Random Forest (price_model.pkl)│    │
│                               │  NearestNeighbors (knn_model.pkl)│   │
│                               └────────────────────────────────┘    │
│                                       │               │             │
│                               topic_precios   topic_sugerencias     │
│                                       │               │             │
│                               Kafka Consumer   Kafka Consumer        │
│                               (Streamlit polls with request_id)     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

All commands assume you are at the project root with the virtualenv active.

### 1. Airflow pipeline (already done)

The Streamlit interface requires the Gold parquet to be present:

```
data/output/listings_gold.parquet
```

This is written by the `save_gold_parquet` task added to `dag_listings.py`
(see `src/transformations/dag_listings_patch.py` for the instructions).

### 2. Install Streamlit

```bash
uv add streamlit pydeck
```

### 3. Train the ML models

```bash
python src/models/train_models.py
```

Produces:
- `models/price_model.pkl`   — sklearn RF Pipeline for price prediction
- `models/knn_model.pkl`     — NearestNeighbors bundle for recommendations

### 4. Start the Kafka infrastructure

```bash
docker compose up -d
```

### 5. Start the Spark ML Processor

In a **dedicated terminal** (keep it running):

```bash
# Linux / Mac
python3 src/queries/ml_processor.py

# Windows
python src\queries\ml_processor.py
```

This subscribes to `topic_peticiones` and `topic_likes` and publishes
results to `topic_precios` and `topic_sugerencias`.

### 6. Launch the Streamlit app

In another terminal:

```bash
# Linux / Mac
streamlit run src/interface/app.py

# Windows
streamlit run src\interface\app.py
```

Open your browser at `http://localhost:8501`.

---

## File Structure

```
src/
├── interface/
│   ├── app.py                  # Main Streamlit entry point
│   ├── kafka_client.py         # Request-reply Kafka utilities
│   └── components/
│       ├── tab_search.py       # Tab 1: Batch search + PyDeck map
│       ├── tab_prediction.py   # Tab 2: Streaming price prediction
│       └── tab_suggestions.py  # Tab 3: Streaming KNN recommendations
├── models/
│   └── train_models.py         # Offline model training (RF + KNN)
└── queries/
    └── ml_processor.py         # Spark Streaming ML processor (both jobs)

models/                         # Generated artefacts (git-ignored)
├── price_model.pkl
└── knn_model.pkl
```

---

## Kafka Topics (new, created automatically)

| Topic               | Direction           | Format        | Purpose                          |
|---------------------|---------------------|---------------|----------------------------------|
| `topic_peticiones`  | Streamlit → Spark   | JSON          | Price prediction requests        |
| `topic_precios`     | Spark → Streamlit   | JSON          | Price prediction responses       |
| `topic_likes`       | Streamlit → Spark   | JSON          | Like / recommendation requests   |
| `topic_sugerencias` | Spark → Streamlit   | JSON          | KNN recommendation responses     |

---

## Notes

- **Timeout**: if Spark doesn't respond within 45 seconds, Streamlit
  falls back to local sklearn inference (Tab 2) or shows a warning (Tab 3).
- **Model fallback**: Tab 2 has a toggle to bypass Kafka and run local
  inference directly, useful when demonstrating without Spark.
- **Session state**: liked listings and recommendations persist within
  the browser tab session and are cleared with the "Clear selections" button.
