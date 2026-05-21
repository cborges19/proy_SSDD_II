"""
Tab 3 · Live Suggestions (Streaming Layer)
==========================================
UX flow:
  1. User browses a paginated grid of listings.
  2. Clicking "❤️ Like" on any listing sends its ID to *topic_likes*.
  3. The Spark ML processor looks up the KNN model, finds the K most
     similar listings, and publishes their IDs to *topic_sugerencias*.
  4. Streamlit polls for the matching response and renders the
     recommendation cards.

Session state keys used:
  - st.session_state['liked_id']       : last liked listing ID
  - st.session_state['recommendations']: last recommendation payload
"""

import pathlib
import logging

import pandas as pd
import streamlit as st

from src.interface.kafka_client import request_recommendations

logger = logging.getLogger(__name__)

# ----- DISPLAY COLUMNS FOR LISTING CARDS -----
_CARD_COLS = [
    "id",
    "neighbourhood_cleansed",
    "room_type",
    "accommodates",
    "bedrooms",
    "price",
    "review_scores_rating",
    "dist_center_km",
    "description",
]

# ----- ITEMS PER PAGE IN THE BROWSE GRID -----
_PAGE_SIZE = 9


@st.cache_data(show_spinner="Loading listings…")
def _load_browse_data(gold_path: pathlib.Path) -> pd.DataFrame:
    """
    Loads and prepares the listing cards dataset.
    Only columns needed for display are kept to minimise memory usage.
    """
    available_cols = _CARD_COLS.copy()
    df = pd.read_parquet(gold_path)
    available_cols = [c for c in available_cols if c in df.columns]
    df = df[available_cols].copy()

    # Truncate long descriptions for card display
    if "description" in df.columns:
        df["description"] = (
            df["description"].fillna("").str.slice(0, 120) + "…"
        )

    # Round floats
    for col in ("price", "review_scores_rating", "dist_center_km"):
        if col in df.columns:
            df[col] = df[col].round(2)

    return df.reset_index(drop=True)


def _render_listing_card(
    row: pd.Series,
    bootstrap_servers: str,
    highlighted: bool = False,
) -> None:
    """
    Renders a single listing card with a 'Like' button.
    Clicking the button triggers the Kafka recommendation request.
    """
    border_style = "border: 2px solid #FF5A5F;" if highlighted else ""

    nh   = row.get("neighbourhood_cleansed", "—")
    rt   = row.get("room_type", "—")
    acc  = row.get("accommodates", "—")
    beds = row.get("bedrooms", "—")
    price = f"€{row['price']:.0f}" if pd.notna(row.get("price")) else "—"
    rating = f"⭐ {row['review_scores_rating']:.1f}" if pd.notna(row.get("review_scores_rating")) else "—"
    dist = f"{row['dist_center_km']:.1f} km" if pd.notna(row.get("dist_center_km")) else "—"
    desc = row.get("description", "")

    st.markdown(
        f"""
        <div class="listing-card" style="{border_style}">
            <b>{nh}</b> · {rt}<br/>
            👤 {acc} guests · 🛏 {beds} bedrooms · {price}/night<br/>
            {rating} · 📍 {dist} from centre<br/>
            <small style="color:#888">{desc}</small>
        </div>
        """,
        unsafe_allow_html=True,
    )

    button_key = f"like_btn_{row['id']}"
    if st.button("❤️ Like", key=button_key, use_container_width=True):
        st.session_state["liked_id"] = int(row["id"])
        with st.spinner("📡 Sending like to Kafka · fetching recommendations…"):
            response = request_recommendations(int(row["id"]), bootstrap_servers)
        if response and "recommendations" in response:
            st.session_state["recommendations"] = response["recommendations"]
            st.session_state["liked_id"] = int(row["id"])
        else:
            st.warning(
                "No recommendations received within the timeout.  "
                "Ensure the Spark ML processor is running.",
                icon="⚠️",
            )
        st.rerun()


def _render_recommendation_panel(
    df_all: pd.DataFrame, recommendations: list[dict]
) -> None:
    """
    Renders the recommendation results panel on the right side of the screen.
    """
    st.markdown("### 🎯 You might also like…")

    if not recommendations:
        st.info("Like a listing on the left to see personalised recommendations.")
        return

    for rec in recommendations:
        rec_id = rec.get("id")
        similarity = rec.get("similarity_score", None)

        # Look up the listing in the gold data
        row_matches = df_all[df_all["id"] == rec_id]
        if row_matches.empty:
            continue

        row = row_matches.iloc[0]
        nh    = row.get("neighbourhood_cleansed", "—")
        rt    = row.get("room_type", "—")
        price = f"€{row['price']:.0f}" if pd.notna(row.get("price")) else "—"
        rating = f"⭐ {row['review_scores_rating']:.1f}" if pd.notna(row.get("review_scores_rating")) else "—"
        sim_label = f"  ·  {similarity:.0%} match" if similarity is not None else ""

        with st.expander(f"🏠 {nh} · {rt} · {price}/night{sim_label}", expanded=True):
            st.markdown(f"**Rating:** {rating}")
            if "accommodates" in row:
                st.markdown(f"**Guests:** {row['accommodates']}  ·  **Bedrooms:** {row.get('bedrooms', '—')}")
            if "description" in row:
                st.caption(row["description"])


def render_suggestions_tab(project_root: pathlib.Path, config: dict) -> None:
    """
    Renders the Live Suggestions tab.

    Parameters
    ----------
    project_root : pathlib.Path
        Absolute path to the project root directory.
    config : dict
        Parsed config.toml dictionary.
    """
    bootstrap_servers = config["kafka"]["bootstrap_servers"]
    gold_path = project_root / "data" / "output" / "listings_gold.parquet"

    # ----- HEADER -----
    st.markdown("### ⭐ Live Listing Recommendations")
    st.caption(
        '<span class="badge-streaming">STREAMING</span>  '
        "Like a listing → Kafka → Spark KNN → recommendations appear here.",
        unsafe_allow_html=True,
    )

    # ----- GUARD: data not yet available -----
    if not gold_path.exists():
        st.info(
            "📂 **Gold parquet not found.**  "
            "Run the `airbnb_master_pipeline` DAG in Airflow first.",
            icon="ℹ️",
        )
        return

    df_all = _load_browse_data(gold_path)

    # ----- FILTER BAR -----
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    with filter_col1:
        room_filter = st.selectbox(
            "Room type",
            ["All"] + sorted(df_all["room_type"].dropna().unique().tolist())
            if "room_type" in df_all else ["All"],
            key="sugg_rt",
        )
    with filter_col2:
        nh_opts = ["All"] + sorted(df_all["neighbourhood_cleansed"].dropna().unique().tolist()) \
            if "neighbourhood_cleansed" in df_all else ["All"]
        nh_filter = st.selectbox("Neighbourhood", nh_opts, key="sugg_nh")
    with filter_col3:
        max_price = float(df_all["price"].max()) if "price" in df_all else 500.0
        price_limit = st.slider("Max price (€/night)", 10.0, max_price, min(200.0, max_price), step=10.0, key="sugg_p")

    # Apply filters
    df_browse = df_all.copy()
    if room_filter != "All" and "room_type" in df_browse:
        df_browse = df_browse[df_browse["room_type"] == room_filter]
    if nh_filter != "All" and "neighbourhood_cleansed" in df_browse:
        df_browse = df_browse[df_browse["neighbourhood_cleansed"] == nh_filter]
    if "price" in df_browse:
        df_browse = df_browse[df_browse["price"] <= price_limit]

    df_browse = df_browse.reset_index(drop=True)

    # ----- PAGINATION -----
    total_pages = max(1, (len(df_browse) + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)
    page_start = (page - 1) * _PAGE_SIZE
    page_end   = page_start + _PAGE_SIZE
    df_page    = df_browse.iloc[page_start:page_end]

    st.caption(f"Showing {page_start + 1}–{min(page_end, len(df_browse))} of {len(df_browse)} listings")

    # ----- TWO-COLUMN LAYOUT: browse (left) | recommendations (right) -----
    browse_col, reco_col = st.columns([2, 1])

    liked_id = st.session_state.get("liked_id")

    with browse_col:
        if df_page.empty:
            st.warning("No listings match the current filters.")
        else:
            card_cols = st.columns(3)
            for idx, (_, row) in enumerate(df_page.iterrows()):
                with card_cols[idx % 3]:
                    _render_listing_card(
                        row=row,
                        bootstrap_servers=bootstrap_servers,
                        highlighted=(liked_id is not None and row["id"] == liked_id),
                    )

    with reco_col:
        recommendations = st.session_state.get("recommendations", [])
        if liked_id:
            liked_row = df_all[df_all["id"] == liked_id]
            if not liked_row.empty:
                r = liked_row.iloc[0]
                st.success(
                    f"❤️ You liked: **{r.get('neighbourhood_cleansed', '—')}** · "
                    f"{r.get('room_type', '—')} · €{r.get('price', '—'):.0f}/night"
                )
        _render_recommendation_panel(df_all, recommendations)

    # ----- CLEAR BUTTON -----
    if liked_id or st.session_state.get("recommendations"):
        if st.button("🔄 Clear selections", key="clear_likes"):
            st.session_state.pop("liked_id", None)
            st.session_state.pop("recommendations", None)
            st.rerun()
