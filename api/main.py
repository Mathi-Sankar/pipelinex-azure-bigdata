"""PipelineX Analytics API.

Serves the Gold star-schema (produced by the Azure Databricks pipeline) as a
REST API. Aggregations are computed once at startup from the exported Gold
CSVs and cached in memory, so every endpoint responds in milliseconds.

Author: Mathi Sankar M R <mathisankar707@gmail.com>
Run locally:  uvicorn api.main:app --reload
Docs:         http://localhost:8000/docs
"""

from __future__ import annotations

from pathlib import Path
from functools import lru_cache

import pandas as pd
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

REPO_ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = REPO_ROOT / "powerbi" / "gold_export"

app = FastAPI(
    title="PipelineX Analytics API",
    description="REST API over the Gold star-schema from the Azure Databricks pipeline. "
    "Consumed by the React dashboard (deployed separately on Vercel).",
    version="1.0.0",
)

# Allow the frontend (any origin) to call the API from the browser.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# --------------------------------------------------------------------------- #
# Data loading — read once, cache in memory.
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1)
def _load() -> dict[str, pd.DataFrame]:
    fact = pd.read_csv(GOLD_DIR / "fact_sales.csv")
    product = pd.read_csv(GOLD_DIR / "dim_product.csv")
    customer = pd.read_csv(GOLD_DIR / "dim_customer.csv")
    date = pd.read_csv(GOLD_DIR / "dim_date.csv")
    seller = pd.read_csv(GOLD_DIR / "dim_seller.csv")
    return {"fact": fact, "product": product, "customer": customer, "date": date, "seller": seller}


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@app.get("/api/health")
def health() -> dict:
    """Liveness probe for CI/CD and uptime checks."""
    return {"status": "ok"}


@app.get("/api/kpis")
def kpis() -> dict:
    """Headline KPIs for the dashboard cards."""
    d = _load()
    fact = d["fact"]
    return {
        "total_revenue": round(float(fact["gross_revenue"].sum()), 2),
        "total_orders": int(fact["order_id"].nunique()),
        "total_customers": int(d["customer"]["customer_id"].nunique()),
        "total_products": int(d["product"]["product_id"].nunique()),
        "total_sellers": int(d["seller"]["seller_id"].nunique()),
        "avg_review_score": round(float(fact["review_score"].mean()), 2),
        "avg_delivery_days": round(float(fact["delivery_days"].mean()), 1),
    }


@app.get("/api/top-categories")
def top_categories(limit: int = Query(10, ge=1, le=50)) -> list[dict]:
    """Revenue and items sold by product category (English names from MongoDB)."""
    d = _load()
    merged = d["fact"].merge(d["product"][["product_id", "product_category_en"]], on="product_id", how="left")
    merged = merged[merged["product_category_en"] != "unknown"]
    grouped = (
        merged.groupby("product_category_en")
        .agg(revenue=("gross_revenue", "sum"), items_sold=("order_item_id", "count"))
        .reset_index()
        .sort_values("revenue", ascending=False)
        .head(limit)
    )
    return [
        {
            "category": row["product_category_en"],
            "revenue": round(float(row["revenue"]), 2),
            "items_sold": int(row["items_sold"]),
        }
        for _, row in grouped.iterrows()
    ]


@app.get("/api/state-revenue")
def state_revenue() -> list[dict]:
    """Revenue and order counts by Brazilian customer state."""
    d = _load()
    merged = d["fact"].merge(d["customer"][["customer_id", "customer_state"]], on="customer_id", how="left")
    grouped = (
        merged.groupby("customer_state")
        .agg(revenue=("gross_revenue", "sum"), orders=("order_id", "nunique"))
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    return [
        {
            "state": row["customer_state"],
            "revenue": round(float(row["revenue"]), 2),
            "orders": int(row["orders"]),
        }
        for _, row in grouped.iterrows()
    ]


@app.get("/api/revenue-trend")
def revenue_trend() -> list[dict]:
    """Monthly revenue trend over the dataset's active period.

    order_date is already an ISO 'YYYY-MM-DD' string, so we slice the month
    directly instead of parsing datetimes — far lighter on memory (important
    on small free-tier instances).
    """
    fact = _load()["fact"]
    month = fact["order_date"].astype(str).str.slice(0, 7)
    grouped = (
        fact.assign(month=month)
        .dropna(subset=["month"])
        .groupby("month")
        .agg(revenue=("gross_revenue", "sum"), orders=("order_id", "nunique"))
        .reset_index()
        .sort_values("month")
    )
    grouped = grouped[grouped["month"].str.match(r"\d{4}-\d{2}")]
    return [
        {"month": row["month"], "revenue": round(float(row["revenue"]), 2), "orders": int(row["orders"])}
        for _, row in grouped.iterrows()
    ]


@app.get("/api/top-sellers")
def top_sellers(limit: int = Query(10, ge=1, le=50)) -> list[dict]:
    """Best sellers by revenue, with delivery + review quality."""
    d = _load()
    merged = d["fact"].merge(d["seller"][["seller_id", "seller_state"]], on="seller_id", how="left")
    grouped = (
        merged.groupby(["seller_id", "seller_state"])
        .agg(
            revenue=("gross_revenue", "sum"),
            items_sold=("order_item_id", "count"),
            avg_review=("review_score", "mean"),
            avg_delivery_days=("delivery_days", "mean"),
        )
        .reset_index()
        .sort_values("revenue", ascending=False)
        .head(limit)
    )
    return [
        {
            "seller_id": row["seller_id"],
            "state": row["seller_state"],
            "revenue": round(float(row["revenue"]), 2),
            "items_sold": int(row["items_sold"]),
            "avg_review": round(float(row["avg_review"]), 2) if pd.notna(row["avg_review"]) else None,
            "avg_delivery_days": round(float(row["avg_delivery_days"]), 1) if pd.notna(row["avg_delivery_days"]) else None,
        }
        for _, row in grouped.iterrows()
    ]


# --------------------------------------------------------------------------- #
# Root — points visitors at the interactive API docs.
# --------------------------------------------------------------------------- #
@app.get("/")
def root() -> dict:
    return {
        "service": "PipelineX Analytics API",
        "docs": "/docs",
        "endpoints": [
            "/api/health", "/api/kpis", "/api/top-categories",
            "/api/state-revenue", "/api/revenue-trend", "/api/top-sellers",
        ],
    }
