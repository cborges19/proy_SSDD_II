"""
Airbnb Málaga · Interactive Data Dashboard
===========================================
Main Streamlit entry point.
Orchestrates the three pipeline tabs:
  - Tab 1: Batch search and map (reads gold parquet)
  - Tab 2: Price prediction via Kafka + Spark Streaming
  - Tab 3: Live KNN-based recommendations via Kafka + Spark Streaming

Run with:
    streamlit run src/interface/app.py
"""

import sys
import pathlib
import tomllib
import streamlit as st

# ----- PATH SETUP -----
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ----- PAGE CONFIG -----
st.set_page_config(
    page_title="Airbnb Málaga · Data Dashboard",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ----- GLOBAL CSS -----
st.markdown(
    """
    <style>
        /* Header branding */
        .app-title {
            font-size: 2.4rem;
            font-weight: 800;
            color: #FF5A5F;
            letter-spacing: -0.5px;
            margin-bottom: 0;
        }
        .app-subtitle {
            font-size: 0.95rem;
            color: #888;
            margin-top: 0.1rem;
            margin-bottom: 1.5rem;
        }
        /* Metric cards */
        .metric-card {
            background: #fafafa;
            border-radius: 10px;
            padding: 1rem 1.2rem;
            border-left: 4px solid #FF5A5F;
        }
        /* Tab polish */
        .stTabs [data-baseweb="tab-list"] { gap: 6px; }
        .stTabs [data-baseweb="tab"] {
            border-radius: 8px 8px 0 0;
            font-weight: 500;
        }
        /* Listing cards in suggestions tab */
        .listing-card {
            background: #fff;
            border: 1px solid #e8e8e8;
            border-radius: 10px;
            padding: 1rem;
            margin-bottom: 0.6rem;
            transition: box-shadow 0.2s;
        }
        .listing-card:hover { box-shadow: 0 2px 12px rgba(0,0,0,0.08); }
        /* Status badges */
        .badge-streaming {
            background: #d4edda;
            color: #155724;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.78rem;
            font-weight: 600;
        }
        .badge-batch {
            background: #cce5ff;
            color: #004085;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.78rem;
            font-weight: 600;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ----- HEADER -----
st.markdown(
    '<div class="app-title">🏠 Airbnb Málaga · Data Pipeline Dashboard</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="app-subtitle">'
    'Real-time analytics powered by Apache Kafka · Spark Streaming · Scikit-learn · Airflow'
    "</div>",
    unsafe_allow_html=True,
)
st.divider()


# ----- CONFIG -----
@st.cache_resource
def load_config() -> dict:
    """Loads project config from config.toml (cached for the session)."""
    with open(PROJECT_ROOT / "config.toml", "rb") as f:
        return tomllib.load(f)


config = load_config()

# ----- TAB COMPONENTS -----
from src.interface.components.tab_search import render_search_tab       # noqa: E402
from src.interface.components.tab_prediction import render_prediction_tab  # noqa: E402
from src.interface.components.tab_suggestions import render_suggestions_tab  # noqa: E402

tab1, tab2, tab3 = st.tabs(
    ["🔍  Search & Map", "💶  Price Prediction", "⭐  Live Suggestions"]
)

with tab1:
    render_search_tab(PROJECT_ROOT, config)

with tab2:
    render_prediction_tab(PROJECT_ROOT, config)

with tab3:
    render_suggestions_tab(PROJECT_ROOT, config)
