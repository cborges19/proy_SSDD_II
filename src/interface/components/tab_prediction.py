"""
Tab 2 · Price Prediction 
==========================================
UX flow:
  1. User fills in listing characteristics via a form.
  2. Streamlit produces a JSON request to *topic_peticiones*.
  3. The Spark ML processor (src/queries/ml_processor.py) reads it,
     applies a pre-trained Random Forest pipeline, and publishes the
     predicted price to *topic_precios*.
  4. Streamlit polls *topic_precios* for a matching request_id and
     displays the result.

Fallback: if the sklearn model file is present locally, a direct
inference path is offered when Kafka/Spark is unavailable.
"""

import pathlib
import logging

import joblib
import numpy as np
import pandas as pd
import streamlit as st

from src.interface.kafka_client import request_price_prediction

logger = logging.getLogger(__name__)

# ----- FEATURE SCHEMA -----
_NUM_FEATURES  = ["accommodates", "bathrooms", "bedrooms", "beds", "num_amenities", "dist_center_km"]
_CAT_FEATURES  = ["room_type", "neighbourhood_cleansed"]
_BOOL_FEATURES = ["host_is_superhost", "instant_bookable"]
ALL_FEATURES   = _NUM_FEATURES + _CAT_FEATURES + _BOOL_FEATURES

# ----- ROOM TYPE OPTIONS -----
_ROOM_TYPES = ["Entire home/apt", "Private room", "Hotel room", "Shared room"]


@st.cache_data(show_spinner=False)
def _load_neighbourhood_list(gold_path: pathlib.Path) -> list[str]:
    """Reads unique neighbourhood values from the gold parquet."""
    if not gold_path.exists():
        return []
    df = pd.read_parquet(gold_path, columns=["neighbourhood_cleansed"])
    return sorted(df["neighbourhood_cleansed"].dropna().unique().tolist())


@st.cache_resource(show_spinner=False)
def _load_local_model(models_dir: pathlib.Path):
    """Loads the local sklearn Pipeline from disk (used as fallback)."""
    model_path = models_dir / "price_model.pkl"
    if model_path.exists():
        return joblib.load(model_path)
    return None


def _build_feature_row(form_values: dict) -> pd.DataFrame:
    """Converts the Streamlit form dict into a single-row DataFrame for inference."""
    row = {col: [form_values.get(col)] for col in ALL_FEATURES}
    return pd.DataFrame(row)


def _local_predict(model, form_values: dict) -> float | None:
    """Runs local sklearn inference as a fallback when Spark is unavailable."""
    if model is None:
        return None
    try:
        X = _build_feature_row(form_values)
        prediction = model.predict(X)[0]
        return max(0.0, float(prediction))
    except Exception as exc:
        logger.warning("Local model inference failed: %s", exc)
        return None


def render_prediction_tab(project_root: pathlib.Path, config: dict) -> None:
    """
    Renders the Price Prediction tab.

    Parameters
    ----------
    project_root : pathlib.Path
        Absolute path to the project root directory.
    config : dict
        Parsed config.toml dictionary.
    """
    bootstrap_servers = config["kafka"]["bootstrap_servers"]
    gold_path   = project_root / "data" / "output" / "listings_gold.parquet"
    models_dir  = project_root / "models"

    # Pre-load neighbourhood list and local model
    neighbourhoods = _load_neighbourhood_list(gold_path)
    local_model    = _load_local_model(models_dir)

    # ----- HEADER -----
    st.markdown("### 💶 Price Prediction for Hosts")
    st.caption(
        '<span class="badge-streaming">STREAMING</span>  '
        "Requests travel through Kafka → Spark ML → Kafka → here.",
        unsafe_allow_html=True,
    )

    # ----- INFO BANNERS -----
    if local_model is None:
        st.warning(
            "No trained model found at `models/price_model.pkl`.  "
            "Run `python src/models/train_models.py` first.",
            icon="⚠️",
        )

    st.info(
        "Fill in your listing details and click **Predict Price**.  "
        "The request is sent to Kafka; Spark Streaming applies the ML model "
        "and the prediction appears below within a few seconds.",
        icon="ℹ️",
    )

    st.divider()

    # ----- FORM -----
    left, right = st.columns([2, 1])

    with left:
        st.subheader("Listing Characteristics")

        form_col1, form_col2 = st.columns(2)

        with form_col1:
            room_type = st.selectbox("Room type", _ROOM_TYPES)
            neighbourhood = st.selectbox(
                "Neighbourhood",
                options=neighbourhoods if neighbourhoods else ["(data not loaded)"],
            )
            accommodates = st.slider("Maximum guests", 1, 16, 2)
            bedrooms     = st.slider("Bedrooms", 0, 10, 1)

        with form_col2:
            beds         = st.slider("Beds", 0, 15, 1)
            bathrooms    = st.slider("Bathrooms", 0.0, 10.0, 1.0, step=0.5)
            num_amenities = st.slider("Number of amenities", 0, 100, 25)
            dist_center_km = st.slider("Distance to city centre (km)", 0.0, 30.0, 2.0, step=0.1)

        superhost       = st.checkbox("Host is Superhost", value=False)
        instant_bookable = st.checkbox("Instant Bookable", value=False)

    with right:
        st.subheader("Pipeline Architecture")
        st.markdown(
            """
            ```
            Streamlit
               │  produce JSON
               ▼
            topic_peticiones
               │
               ▼
            Spark Streaming
            ┌──────────────┐
            │  Load .pkl   │
            │  RF Pipeline │
            │  predict()   │
            └──────────────┘
               │  produce result
               ▼
            topic_precios
               │  poll (≤45s)
               ▼
            Streamlit  ✔
            ```
            """
        )

    st.divider()

    # ----- PREDICTION TRIGGER -----
    col_btn, col_result = st.columns([1, 2])

    with col_btn:
        use_streaming = st.toggle("Use Kafka / Spark Streaming", value=True)
        predict_clicked = st.button("Predict Price", type="primary", use_container_width=True)

    if predict_clicked:
        form_values = {
            "room_type":            room_type,
            "neighbourhood_cleansed": neighbourhood,
            "accommodates":         accommodates,
            "bedrooms":             float(bedrooms),
            "beds":                 float(beds),
            "bathrooms":            float(bathrooms),
            "num_amenities":        float(num_amenities),
            "dist_center_km":       dist_center_km,
            "host_is_superhost":    int(superhost),
            "instant_bookable":     int(instant_bookable),
        }

        predicted_price = None

        # --- Streaming path ---
        if use_streaming:
            with st.spinner("📡 Sending to Kafka · waiting for Spark response…"):
                response = request_price_prediction(form_values, bootstrap_servers)

            if response and "predicted_price" in response:
                predicted_price = response["predicted_price"]
                source_label = "Spark Streaming (via Kafka)"
            else:
                st.warning(
                    "Kafka/Spark did not respond within the timeout. "
                    "Falling back to local model inference.",
                    icon="⚠️",
                )
                predicted_price = _local_predict(local_model, form_values)
                source_label = "Local model (fallback)"
        else:
            # --- Local fallback path ---
            predicted_price = _local_predict(local_model, form_values)
            source_label = "Local model (direct)"

        # --- Display result ---
        with col_result:
            if predicted_price is not None:
                st.success(f"### 💶 Estimated Price: €{predicted_price:.2f} / night")
                st.caption(f"Source: {source_label}")

                # Contextual benchmark
                if gold_path.exists() and neighbourhoods:
                    df_bench = pd.read_parquet(
                        gold_path, columns=["neighbourhood_cleansed", "room_type", "price"]
                    )
                    bench = df_bench[
                        (df_bench["neighbourhood_cleansed"] == neighbourhood)
                        & (df_bench["room_type"] == room_type)
                    ]["price"]
                    if not bench.empty:
                        median_price = bench.median()
                        delta = predicted_price - median_price
                        st.metric(
                            label=f"Neighbourhood median ({room_type})",
                            value=f"€{median_price:.2f}",
                            delta=f"{delta:+.2f} vs your estimate",
                        )
            else:
                st.error(
                    "Could not produce a prediction.  "
                    "Ensure the ML model is trained and Spark is running.",
                    icon="❌",
                )
