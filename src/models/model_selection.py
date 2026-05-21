"""
Model Selection & Production Training Script
=============================================
Evaluates multiple regression algorithms for price prediction, logs all
metrics and plots to MLflow, and automatically selects the best performing
model. The winner is then re-trained on 100% of the dataset and saved
alongside the KNN recommendation model for production use.

Models produced
---------------
models/price_model.pkl
    Full sklearn Pipeline of the BEST performing algorithm.
    Input features : Available numerics + categoricals + booleans.
    Target         : price (€ / night).

models/knn_model.pkl
    sklearn NearestNeighbors fitted on scaled numerical features.
    Used for content-based listing recommendations.

Usage
-----
    python3 src/models/model_selection.py
    
    # Launch MLflow UI after running:
    mlflow ui --port 5000
"""

import logging
import pathlib
import sys
import warnings

import joblib
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    ExtraTreesRegressor,
    GradientBoostingRegressor,
    RandomForestRegressor,
)
from sklearn.impute import SimpleImputer
from sklearn.linear_model import ElasticNet, Lasso, Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.neighbors import NearestNeighbors
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder, StandardScaler

warnings.filterwarnings("ignore")

# ----- XGBOOST -----
try:
    from xgboost import XGBRegressor
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    print("XGBoost not installed — skipping XGBRegressor.")

# ----- PATH SETUP -----
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("ModelSelection")

# ----- PATHS -----
GOLD_PATH  = PROJECT_ROOT / "data" / "output" / "listings_gold.parquet"
MODELS_DIR = PROJECT_ROOT / "models"
MLFLOW_DIR = PROJECT_ROOT / "mlruns"

# ----- FEATURE SCHEMA -----
PRICE_FEATURES_NUM = [
    "accommodates", "bathrooms", "bedrooms", "beds",
    "num_amenities", "dist_center_km"
]
PRICE_FEATURES_CAT = [
    "room_type", "neighbourhood_cleansed"
]
PRICE_FEATURES_BOOL = [
    "host_is_superhost", "instant_bookable"
]

ALL_PRICE_FEATURES = PRICE_FEATURES_NUM + PRICE_FEATURES_CAT + PRICE_FEATURES_BOOL
PRICE_TARGET = "price"

KNN_FEATURES = [
    "accommodates", "bathrooms", "bedrooms", "beds",
    "num_amenities", "dist_center_km",
    "review_scores_rating", "trust_score", "price"
]

KNN_DISPLAY_COLS = [
    "id", "neighbourhood_cleansed", "room_type",
    "accommodates", "bedrooms", "price",
    "review_scores_rating", "dist_center_km"
]
KNN_N_NEIGHBORS = 4


def _load_and_clean(path: pathlib.Path) -> pd.DataFrame:
    """
    Loads the gold parquet and applies minimal preprocessing needed
    for model training (bool normalisation, null filtering on target).
    """
    if not path.exists():
        logger.error(
            "Gold parquet not found at: %s\n"
            "Run the Airflow ETL pipeline first.", path
        )
        sys.exit(1)

    logger.info("Loading gold parquet from: %s", path)
    df = pd.read_parquet(path)

    # Normalise boolean columns to int (0/1) 
    for col in PRICE_FEATURES_BOOL:
        if col in df.columns:
            df[col] = df[col].map(
                {True: 1, False: 0, "t": 1, "f": 0, 1: 1, 0: 0}
            ).fillna(0).astype(int)

    # Drop rows with null target and negative prices
    df = df.dropna(subset=[PRICE_TARGET])
    df = df[df[PRICE_TARGET] > 0]
    
    logger.info("Loaded and cleaned %d rows", len(df))
    return df


def _build_pipeline_for_estimator(estimator, available_features: dict) -> Pipeline:
    """
    Constructs the sklearn Pipeline dynamically based on the available
    features and the provided estimator.
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
        ("num",  num_transformer,  available_features["num"]),
        ("cat",  cat_transformer,  available_features["cat"]),
        ("bool", bool_transformer, available_features["bool"]),
    ])

    return Pipeline([
        ("preprocessor", preprocessor),
        ("model", estimator),
    ])


def evaluate_and_select_price_model(df: pd.DataFrame, models_dir: pathlib.Path) -> None:
    """
    Evaluates candidate algorithms using an 80/20 split for fair comparison,
    logs metrics/plots to MLflow, and finally re-trains the best model on
    the 100% of the dataset to save it for production.
    """
    logger.info("--- Starting Price Model Search & Selection ---")

    # Determine available features
    avail_features = {
        "num":  [c for c in PRICE_FEATURES_NUM  if c in df.columns],
        "cat":  [c for c in PRICE_FEATURES_CAT  if c in df.columns],
        "bool": [c for c in PRICE_FEATURES_BOOL if c in df.columns],
    }
    feature_cols = avail_features["num"] + avail_features["cat"] + avail_features["bool"]

    X = df[feature_cols]
    y = df[PRICE_TARGET]

    # Split solely for evaluation purposes
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    candidate_models = {
        "Ridge Regression":  Ridge(alpha=10.0),
        "Lasso Regression":  Lasso(alpha=1.0, max_iter=5000),
        "ElasticNet":        ElasticNet(alpha=1.0, l1_ratio=0.5, max_iter=5000),
        "Extra Trees":       ExtraTreesRegressor(n_estimators=200, max_depth=12, n_jobs=-1, random_state=42),
        "Gradient Boosting": GradientBoostingRegressor(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42),
        "Random Forest":     RandomForestRegressor(n_estimators=300, max_depth=12, min_samples_leaf=4, n_jobs=-1, random_state=42),
    }

    if HAS_XGB:
        candidate_models["XGBoost"] = XGBRegressor(
            n_estimators=300, max_depth=6, learning_rate=0.05, 
            subsample=0.8, colsample_bytree=0.8, random_state=42, 
            eval_metric="mae", verbosity=0
        )

    mlflow.set_tracking_uri(f"file://{MLFLOW_DIR}")
    mlflow.set_experiment("airbnb_price_prediction")

    results = []
    best_mae = float("inf")
    best_model_name = None
    best_estimator = None

    # ----- MLflow Search & Evaluation -----
    for model_name, estimator in candidate_models.items():
        logger.info("Evaluating: %s", model_name)
        pipeline = _build_pipeline_for_estimator(estimator, avail_features)

        with mlflow.start_run(run_name=f"Eval_{model_name}"):
            # Cross-validation on train split
            cv_scores = cross_val_score(
                pipeline, X_train, y_train,
                scoring="neg_mean_absolute_error", cv=5, n_jobs=-1
            )
            cv_mae = -cv_scores.mean()
            cv_std = cv_scores.std()

            # Fit and evaluate on test split
            pipeline.fit(X_train, y_train)
            y_pred = pipeline.predict(X_test)

            mae  = mean_absolute_error(y_test, y_pred)
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))
            r2   = r2_score(y_test, y_pred)

            mlflow.log_param("model_type", model_name)
            mlflow.log_metric("test_mae", mae)
            mlflow.log_metric("cv_mae_mean", cv_mae)
            mlflow.log_metric("test_r2", r2)

            results.append({
                "Model": model_name,
                "CV MAE": round(cv_mae, 2),
                "CV Std": round(cv_std, 2),
                "Test MAE": round(mae, 2),
                "Test R2": round(r2, 3),
            })

            # Track the best model based on Test MAE
            if mae < best_mae:
                best_mae = mae
                best_model_name = model_name
                best_estimator = estimator

    results_df = pd.DataFrame(results).sort_values("Test MAE").reset_index(drop=True)
    
    print("\n=== Price Prediction — Model Comparison ===")
    print(results_df.to_string(index=False))

    # ----- Production Retraining (100% of data) -----
    logger.info("--- Retraining winning model (%s) on 100%% of data ---", best_model_name)
    prod_pipeline = _build_pipeline_for_estimator(best_estimator, avail_features)
    
    with mlflow.start_run(run_name=f"Production_{best_model_name}"):
        prod_pipeline.fit(X, y)
        
        # Log production metrics and physical model to MLflow
        mlflow.log_param("is_production", True)
        mlflow.log_param("n_training_rows", len(X))
        mlflow.sklearn.log_model(
            prod_pipeline,
            artifact_path="prod_model",
        )
        
        # Save artifact for the API / Streamlit
        out_path = models_dir / "price_model.pkl"
        joblib.dump(prod_pipeline, out_path)
        logger.info("Production price model saved to: %s", out_path)


def train_knn_model(df: pd.DataFrame, models_dir: pathlib.Path) -> None:
    """
    Trains a NearestNeighbors model on scaled numerical features for
    content-based listing recommendations, saving it to disk and MLflow.
    """
    logger.info("--- Training KNN recommendation model ---")

    available_knn = [c for c in KNN_FEATURES if c in df.columns]
    df_knn = df[["id"] + available_knn].dropna(subset=available_knn).copy()

    listing_ids    = df_knn["id"].values
    feature_matrix = df_knn[available_knn].values

    scaler         = StandardScaler()
    feature_scaled = scaler.fit_transform(feature_matrix)

    # Fit NearestNeighbors 
    knn = NearestNeighbors(
        n_neighbors=min(KNN_N_NEIGHBORS, len(df_knn)),
        metric="cosine",
        algorithm="brute",
        n_jobs=-1,
    )
    knn.fit(feature_scaled)

    # Compute average intra-cluster diversity 
    sample_idx = np.random.choice(len(listing_ids), size=min(500, len(listing_ids)), replace=False)
    dists, _ = knn.kneighbors(feature_scaled[sample_idx])
    avg_dist = dists[:, 1:].mean()

    display_cols_available = [c for c in KNN_DISPLAY_COLS if c in df.columns]
    display_df = df[display_cols_available].copy()

    knn_bundle = {
        "knn_model":      knn,
        "scaler":         scaler,
        "listing_ids":    listing_ids,
        "feature_matrix": feature_scaled,
        "feature_cols":   available_knn,
        "display_df":     display_df,
    }
    
    out_path = models_dir / "knn_model.pkl"
    joblib.dump(knn_bundle, out_path)
    logger.info("KNN bundle saved to: %s", out_path)

    mlflow.set_experiment("airbnb_knn_recommendation")
    with mlflow.start_run(run_name="Production_KNN"):
        mlflow.log_param("metric", "cosine")
        mlflow.log_param("n_neighbors", KNN_N_NEIGHBORS)
        mlflow.log_param("n_features", len(available_knn))
        mlflow.log_metric("avg_neighbour_distance", avg_dist)
        mlflow.log_artifact(str(out_path))


def main() -> None:
    """Entry point: runs evaluation, selects winner, trains production models."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    MLFLOW_DIR.mkdir(parents=True, exist_ok=True)

    df = _load_and_clean(GOLD_PATH)

    evaluate_and_select_price_model(df, MODELS_DIR)
    train_knn_model(df, MODELS_DIR)

    logger.info("--- Pipeline Finished Successfully ---")
    logger.info("Models are ready in: %s", MODELS_DIR)


if __name__ == "__main__":
    main()