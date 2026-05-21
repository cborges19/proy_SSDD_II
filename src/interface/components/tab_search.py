"""
Tab 1 · Search & Map (Batch Layer)
====================================
Reads the Gold-layer listings parquet produced by the Airflow DAG and
renders an interactive filter panel + PyDeck map + summary table.

Data source: data/output/listings_gold.parquet  (written by the DAG's
             save_gold_parquet task after validation).
"""

import pathlib

import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st

# ----- COLUMNS USED IN THIS TAB -----
_DISPLAY_COLS = [
    "id",
    "neighbourhood_cleansed",
    "room_type",
    "accommodates",
    "bedrooms",
    "beds",
    "bathrooms",
    "price",
    "review_scores_rating",
    "number_of_reviews",
    "dist_center_km",
    "host_is_superhost",
    "instant_bookable",
    "latitude",
    "longitude",
]

# ----- COLOUR SCALE: price → RGB (blue=cheap, red=expensive) -----
def _price_to_rgb(normalised: float) -> list[int]:
    """Maps a 0-1 price ratio to a blue→red colour gradient."""
    r = int(255 * normalised)
    b = int(255 * (1 - normalised))
    return [r, 80, b, 180]


@st.cache_data(show_spinner="Loading listings data…")
def _load_listings(gold_path: pathlib.Path) -> pd.DataFrame:
    """Reads the gold parquet and returns only the columns needed for this tab."""
    df = pd.read_parquet(gold_path)

    # Keep only columns that actually exist to guard against schema changes
    available = [c for c in _DISPLAY_COLS if c in df.columns]
    df = df[available].copy()

    # Cast boolean columns to a display-friendly type
    for col in ("host_is_superhost", "instant_bookable"):
        if col in df.columns:
            df[col] = df[col].map({True: "✔ Yes", False: "No", 1: "✔ Yes", 0: "No"})

    # Round floats for display
    for col in ("price", "review_scores_rating", "dist_center_km"):
        if col in df.columns:
            df[col] = df[col].round(2)

    return df


def _build_deck(df: pd.DataFrame) -> pdk.Deck:
    """Constructs a PyDeck scatter-plot layer coloured by relative price."""
    if df.empty or "latitude" not in df.columns:
        return None

    # Normalise price to [0, 1] for colour mapping
    price_min, price_max = df["price"].min(), df["price"].max()
    price_range = price_max - price_min if price_max != price_min else 1.0
    df = df.copy()
    df["_color"] = ((df["price"] - price_min) / price_range).apply(_price_to_rgb)

    # Tooltip fields (only show columns that are present)
    tooltip_fields = {
        "neighbourhood_cleansed": "Neighbourhood",
        "room_type": "Room type",
        "accommodates": "Guests",
        "price": "€/night",
        "review_scores_rating": "Rating",
    }
    tooltip_html = "<br/>".join(
        f"<b>{label}:</b> {{{col}}}"
        for col, label in tooltip_fields.items()
        if col in df.columns
    )

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_fill_color="_color",
        get_radius=80,
        radius_min_pixels=3,
        radius_max_pixels=14,
        pickable=True,
        auto_highlight=True,
    )

    view = pdk.ViewState(
        latitude=df["latitude"].mean(),
        longitude=df["longitude"].mean(),
        zoom=12,
        pitch=0,
    )

    return pdk.Deck(
        layers=[layer],
        initial_view_state=view,
        tooltip={"html": tooltip_html, "style": {"color": "white", "background": "#333"}},
        map_style="mapbox://styles/mapbox/light-v10",
    )


def render_search_tab(project_root: pathlib.Path, config: dict) -> None:
    """
    Renders the entire Search & Map tab.

    Parameters
    ----------
    project_root : pathlib.Path
        Absolute path to the project root directory.
    config : dict
        Parsed config.toml dictionary.
    """
    gold_path = project_root / "data" / "output" / "listings_gold.parquet"

    # ----- GUARD: data not yet available -----
    if not gold_path.exists():
        st.info(
            "📂 **Gold parquet not found.**  "
            "Run the `airbnb_master_pipeline` DAG in Airflow first; "
            "it will write the file to `data/output/listings_gold.parquet`.",
            icon="ℹ️",
        )
        return

    df_full = _load_listings(gold_path)

    # ----- SIDEBAR FILTERS -----
    st.sidebar.markdown("## 🔍 Filter Listings")
    st.sidebar.caption('<span class="badge-batch">BATCH</span>', unsafe_allow_html=True)

    # Price range
    p_min, p_max = float(df_full["price"].min()), float(df_full["price"].max())
    price_range = st.sidebar.slider(
        "Price per night (€)",
        min_value=p_min,
        max_value=p_max,
        value=(p_min, min(p_min + 200.0, p_max)),
        step=5.0,
    )

    # Neighbourhood
    neighbourhoods = sorted(df_full["neighbourhood_cleansed"].dropna().unique())
    selected_nh = st.sidebar.multiselect(
        "Neighbourhood", options=neighbourhoods, default=[]
    )

    # Room type
    room_types = sorted(df_full["room_type"].dropna().unique()) if "room_type" in df_full else []
    selected_rt = st.sidebar.multiselect(
        "Room type", options=room_types, default=[]
    )

    # Guests capacity
    max_acc = int(df_full["accommodates"].max()) if "accommodates" in df_full else 16
    min_guests = st.sidebar.slider("Minimum guests capacity", 1, max_acc, 1)

    # Superhost only
    superhost_only = st.sidebar.checkbox("Superhost only", value=False)

    # ----- APPLY FILTERS -----
    mask = (df_full["price"] >= price_range[0]) & (df_full["price"] <= price_range[1])

    if selected_nh:
        mask &= df_full["neighbourhood_cleansed"].isin(selected_nh)

    if selected_rt and "room_type" in df_full:
        mask &= df_full["room_type"].isin(selected_rt)

    if "accommodates" in df_full:
        mask &= df_full["accommodates"] >= min_guests

    if superhost_only and "host_is_superhost" in df_full:
        mask &= df_full["host_is_superhost"] == "✔ Yes"

    df_filtered = df_full[mask].reset_index(drop=True)

    # ----- TOP METRICS -----
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Listings found", f"{len(df_filtered):,}")
    col2.metric(
        "Avg price / night",
        f"€{df_filtered['price'].mean():.0f}" if not df_filtered.empty else "—",
    )
    col3.metric(
        "Avg rating",
        f"{df_filtered['review_scores_rating'].mean():.2f} ⭐"
        if not df_filtered.empty and "review_scores_rating" in df_filtered
        else "—",
    )
    col4.metric(
        "Avg dist. to centre",
        f"{df_filtered['dist_center_km'].mean():.1f} km"
        if not df_filtered.empty and "dist_center_km" in df_filtered
        else "—",
    )

    st.divider()

    # ----- MAP -----
    st.subheader("🗺️ Listing Locations  ·  colour = price (blue→red)")
    if df_filtered.empty:
        st.warning("No listings match the current filters.")
    else:
        deck = _build_deck(df_filtered)
        if deck:
            st.pydeck_chart(deck, use_container_width=True)

    st.divider()

    # ----- DATA TABLE -----
    st.subheader(f"📋 Listing Details  ({len(df_filtered):,} results)")
    table_cols = [
        c for c in _DISPLAY_COLS
        if c in df_filtered.columns and c not in ("latitude", "longitude")
    ]
    st.dataframe(
        df_filtered[table_cols],
        use_container_width=True,
        height=400,
        hide_index=True,
    )

    # ----- DOWNLOAD -----
    csv = df_filtered[table_cols].to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Download filtered results as CSV",
        data=csv,
        file_name="airbnb_malaga_filtered.csv",
        mime="text/csv",
    )
