"""
Model Training Script
======================
Trains two scikit-learn models from the Gold-layer listings parquet and
saves them to the ``models/`` directory for use by the Spark ML processor
and the Streamlit fallback inference path.

Models produced
---------------
models/price_model.pkl
    Full sklearn Pipeline:
      ColumnTransformer (impute + encode) → RandomForestRegressor
    Input features : ALL_FEATURES (numerics + categoricals + booleans)
    Target         : price (€ / night, original scale)

models/knn_model.pkl
    sklearn NearestNeighbors fitted on scaled numerical features.
    Used for content-based listing recommendations.

models/knn_metadata.pkl
    Dict containing:
      - 'listing_ids'   : np.ndarray of listing IDs (index aligned with knn_model)
      - 'feature_matrix': np.ndarray of scaled feature vectors
      - 'display_df'    : pd.DataFrame with display columns for the API response

Usage
-----
    python src/models/train_models.py

The script expects ``data/output/listings_gold.parquet`` to exist.
Run the Airflow pipeline first if it does not.
"""

import logging
import pathlib
import sys

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.neighbors import NearestNeighbors
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder, StandardScaler

# ----- PATH SETUP -----
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("TrainModels")

# ----- PATHS -----
GOLD_PATH  = PROJECT_ROOT / "data" / "output" / "listings_gold.parquet"
MODELS_DIR = PROJECT_ROOT / "models"

# ----- FEATURE SCHEMA -----
# Numerical features (continuous)
PRICE_FEATURES_NUM = [
    "accommodates",
    "bathrooms",
    "bedrooms",
    "beds",
    "num_amenities",
    "dist_center_km",
]

# Categorical features (low-to-medium cardinality)
PRICE_FEATURES_CAT = [
    "room_type",
    "neighbourhood_cleansed",
]

# Boolean features (stored as int 0/1 or bool)
PRICE_FEATURES_BOOL = [
    "host_is_superhost",
    "instant_bookable",
]

# All features fed to the prediction model
ALL_PRICE_FEATURES = PRICE_FEATURES_NUM + PRICE_FEATURES_CAT + PRICE_FEATURES_BOOL

# Target variable
PRICE_TARGET = "price"

# KNN uses only numerical features for distance computation
KNN_FEATURES = [
    "accommodates",
    "bathrooms",
    "bedrooms",
    "beds",
    "num_amenities",
    "dist_center_km",
    "review_scores_rating",
    "trust_score",
    "price",
]

# KNN display columns returned in the recommendation response
KNN_DISPLAY_COLS = [
    "id",
    "neighbourhood_cleansed",
    "room_type",
    "accommodates",
    "bedrooms",
    "price",
    "review_scores_rating",
    "dist_center_km",
]

# Number of nearest neighbours
KNN_N_NEIGHBORS = 4   # 4 so we can exclude the query listing itself → 3 results


def _load_and_clean(path: pathlib.Path) -> pd.DataFrame:
    """
    Loads the gold parquet and applies minimal preprocessing needed
    for model training (bool normalisation, null filtering on target).
    """
    logger.info("Loading gold parquet from: %s", path)
    df = pd.read_parquet(path)
    logger.info("Loaded %d rows, %d columns", len(df), len(df.columns))

    # Normalise boolean columns to int (0/1) for sklearn compatibility
    for col in PRICE_FEATURES_BOOL:
        if col in df.columns:
            df[col] = df[col].map(
                {True: 1, False: 0, "t": 1, "f": 0, 1: 1, 0: 0}
            ).fillna(0).astype(int)

    # Drop rows with null target
    before = len(df)
    df = df.dropna(subset=[PRICE_TARGET])
    logger.info("Dropped %d rows with null '%s'. Remaining: %d", before - len(df), PRICE_TARGET, len(df))

    # Guard: price must be positive
    df = df[df[PRICE_TARGET] > 0]
    logger.info("After price > 0 filter: %d rows", len(df))

    return df


def _build_price_pipeline() -> Pipeline:
    """
    Constructs the sklearn price-prediction Pipeline.

    Preprocessing:
      - Numerical : median imputation
      - Categorical: most_frequent imputation + OrdinalEncoder
      - Boolean    : constant imputation (0) — already int
    """
    num_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
    ])

    cat_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("encoder", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)),
    ])

    bool_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value=0)),
    ])

    preprocessor = ColumnTransformer([
        ("num",  num_transformer,  PRICE_FEATURES_NUM),
        ("cat",  cat_transformer,  PRICE_FEATURES_CAT),
        ("bool", bool_transformer, PRICE_FEATURES_BOOL),
    ])

    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("model", RandomForestRegressor(
            n_estimators=300,
            max_depth=12,
            min_samples_leaf=4,
            n_jobs=-1,
            random_state=42,
        )),
    ])

    return pipeline


def train_price_model(df: pd.DataFrame, models_dir: pathlib.Path) -> None:
    """
    Trains the price prediction pipeline and saves it to disk.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned gold dataset.
    models_dir : pathlib.Path
        Output directory for model artefacts.
    """
    logger.info("--- Training price prediction model ---")

    # Select only available feature columns
    available_num  = [c for c in PRICE_FEATURES_NUM  if c in df.columns]
    available_cat  = [c for c in PRICE_FEATURES_CAT  if c in df.columns]
    available_bool = [c for c in PRICE_FEATURES_BOOL if c in df.columns]

    feature_cols = available_num + available_cat + available_bool
    logger.info("Features used: %s", feature_cols)

    X = df[feature_cols]
    y = df[PRICE_TARGET]

    pipeline = _build_price_pipeline()
    pipeline.fit(X, y)

    # Quick in-sample evaluation (for logging only)
    y_pred = pipeline.predict(X)
    mae = np.abs(y - y_pred).mean()
    logger.info("In-sample MAE: %.2f €", mae)

    # Save
    out_path = models_dir / "price_model.pkl"
    joblib.dump(pipeline, out_path)
    logger.info("Price model saved to: %s", out_path)


def train_knn_model(df: pd.DataFrame, models_dir: pathlib.Path) -> None:
    """
    Trains a NearestNeighbors model on scaled numerical features for
    content-based listing recommendations.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned gold dataset.
    models_dir : pathlib.Path
        Output directory for model artefacts.
    """
    logger.info("--- Training KNN recommendation model ---")

    # Select only available KNN feature columns
    available_knn = [c for c in KNN_FEATURES if c in df.columns]
    logger.info("KNN features: %s", available_knn)

    df_knn = df[["id"] + available_knn].dropna(subset=available_knn).copy()
    logger.info("KNN training rows (no nulls in features): %d", len(df_knn))

    listing_ids    = df_knn["id"].values
    feature_matrix = df_knn[available_knn].values

    # Scale features to equalise contribution
    scaler         = StandardScaler()
    feature_scaled = scaler.fit_transform(feature_matrix)

    # Fit NearestNeighbors with cosine metric
    knn = NearestNeighbors(
        n_neighbors=min(KNN_N_NEIGHBORS, len(df_knn)),
        metric="cosine",
        algorithm="brute",
        n_jobs=-1,
    )
    knn.fit(feature_scaled)

    # Build display DataFrame for quick lookups in the Spark processor
    display_cols_available = [c for c in KNN_DISPLAY_COLS if c in df.columns]
    display_df = df[display_cols_available].copy()

    # Save model + metadata as a single artefact bundle
    knn_bundle = {
        "knn_model":      knn,
        "scaler":         scaler,
        "listing_ids":    listing_ids,
        "feature_matrix": feature_scaled,   # already scaled
        "feature_cols":   available_knn,
        "display_df":     display_df,
    }
    out_path = models_dir / "knn_model.pkl"
    joblib.dump(knn_bundle, out_path)
    logger.info("KNN bundle saved to: %s", out_path)


def main() -> None:
    """Entry point: trains all models and saves them to ``models/``."""
    # ----- GUARD: gold parquet must exist -----
    if not GOLD_PATH.exists():
        logger.error(
            "Gold parquet not found at: %s\n"
            "Run the 'airbnb_master_pipeline' Airflow DAG first.",
            GOLD_PATH,
        )
        sys.exit(1)

    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    df = _load_and_clean(GOLD_PATH)

    train_price_model(df, MODELS_DIR)
    train_knn_model(df, MODELS_DIR)

    logger.info("All models trained and saved successfully to: %s", MODELS_DIR)


if __name__ == "__main__":
    main()
