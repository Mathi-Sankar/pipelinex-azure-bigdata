"""Regenerate the Gold star-schema CSVs from the full Olist dataset + MongoDB.

This reproduces the same star schema the Azure Databricks pipeline builds, but
locally with pandas, so the exported CSVs used by the API match the Azure Gold
output (full 32,951-product MongoDB enrichment). One-off utility.

Author: Mathi Sankar M R
Usage:
    export MONGO_URI="mongodb+srv://..."
    python scripts/regenerate_gold.py --raw-dir "C:/Users/mathi/Downloads/archive"
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

try:
    from pymongo import MongoClient
except ImportError:
    sys.exit("pip install 'pymongo[srv]'")

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "powerbi" / "gold_export"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", required=True, help="Folder with the full Olist CSVs")
    args = ap.parse_args()

    uri = os.environ.get("MONGO_URI")
    if not uri:
        sys.exit("Set MONGO_URI env var")

    raw = Path(args.raw_dir)
    print("Reading raw Olist CSVs...")
    orders = pd.read_csv(raw / "olist_orders_dataset.csv")
    items = pd.read_csv(raw / "olist_order_items_dataset.csv")
    customers = pd.read_csv(raw / "olist_customers_dataset.csv")
    products = pd.read_csv(raw / "olist_products_dataset.csv")
    sellers = pd.read_csv(raw / "olist_sellers_dataset.csv")
    payments = pd.read_csv(raw / "olist_order_payments_dataset.csv")
    reviews = pd.read_csv(raw / "olist_order_reviews_dataset.csv")

    # Dedup on natural keys (mirrors the Silver layer)
    orders = orders.drop_duplicates("order_id")
    items = items.drop_duplicates(["order_id", "order_item_id"])
    customers = customers.drop_duplicates("customer_id")
    products = products.drop_duplicates("product_id")
    sellers = sellers.drop_duplicates("seller_id")

    print("Fetching MongoDB enrichment...")
    client = MongoClient(uri)
    docs = list(client["pipelinex"]["product_catalogue"].find({}, {"_id": 0}))
    catalogue = pd.DataFrame(docs)[["product_id", "category_en", "tags", "margin_pct"]]
    catalogue = catalogue.rename(columns={"category_en": "product_category_en"})
    catalogue["tags"] = catalogue["tags"].apply(
        lambda t: ",".join(t) if isinstance(t, list) else (t or "")
    )
    print(f"  {len(catalogue):,} enrichment docs")

    # dim_product (enriched, SCD2 shape)
    dim_product = products.merge(catalogue, on="product_id", how="left")
    dim_product["product_category_en"] = dim_product["product_category_en"].fillna("unknown")
    dim_product = dim_product[
        ["product_id", "product_category_name", "product_category_en", "tags",
         "margin_pct", "product_weight_g"]
    ].copy()
    dim_product["effective_from"] = pd.Timestamp.today().date()
    dim_product["effective_to"] = pd.NaT
    dim_product["is_current"] = True

    # dim_date
    orders["order_purchase_timestamp"] = pd.to_datetime(
        orders["order_purchase_timestamp"], errors="coerce"
    )
    dd = orders[["order_purchase_timestamp"]].dropna().copy()
    dd["date_key"] = dd["order_purchase_timestamp"].dt.date
    dim_date = pd.DataFrame({"date_key": pd.unique(dd["date_key"])})
    dk = pd.to_datetime(dim_date["date_key"])
    dim_date["year"] = dk.dt.year
    dim_date["quarter"] = dk.dt.quarter
    dim_date["month"] = dk.dt.month
    dim_date["month_name"] = dk.dt.strftime("%B")
    dim_date["day"] = dk.dt.day
    dim_date["day_of_week"] = dk.dt.strftime("%A")
    dim_date["is_weekend"] = dk.dt.dayofweek.isin([5, 6])

    # fact_sales (grain = order_item), payment allocated by price share
    order_total = items.groupby("order_id")["price"].sum().rename("order_total_price")
    pay = payments.groupby("order_id")["payment_value"].sum().rename("payment_total")
    rev = (
        reviews.sort_values("review_creation_date")
        .drop_duplicates("order_id", keep="last")[["order_id", "review_score"]]
    )

    orders["order_delivered_customer_date"] = pd.to_datetime(
        orders["order_delivered_customer_date"], errors="coerce"
    )

    fact = (
        items.merge(orders, on="order_id")
        .merge(order_total, on="order_id")
        .merge(pay, on="order_id", how="left")
        .merge(rev, on="order_id", how="left")
    )
    fact["order_date"] = fact["order_purchase_timestamp"].dt.date
    fact["gross_revenue"] = fact["price"] + fact["freight_value"]
    fact["allocated_payment"] = fact["payment_total"] * fact["price"] / fact["order_total_price"]
    fact["delivery_days"] = (
        fact["order_delivered_customer_date"] - fact["order_purchase_timestamp"]
    ).dt.days

    fact_sales = fact[
        ["order_id", "order_item_id", "product_id", "seller_id", "customer_id",
         "order_date", "order_status", "price", "freight_value", "gross_revenue",
         "allocated_payment", "review_score", "delivery_days"]
    ]

    # dim_customer / dim_seller
    dim_customer = customers.copy()
    dim_customer["_ingest_ts"] = pd.Timestamp.now()
    dim_seller = sellers.copy()
    dim_seller["_ingest_ts"] = pd.Timestamp.now()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fact_sales.to_csv(OUT_DIR / "fact_sales.csv", index=False)
    dim_product.to_csv(OUT_DIR / "dim_product.csv", index=False)
    dim_customer.to_csv(OUT_DIR / "dim_customer.csv", index=False)
    dim_seller.to_csv(OUT_DIR / "dim_seller.csv", index=False)
    dim_date.to_csv(OUT_DIR / "dim_date.csv", index=False)

    print(f"\nWrote fresh Gold CSVs to {OUT_DIR}")
    print(f"  fact_sales:   {len(fact_sales):,} rows")
    print(f"  dim_product:  {len(dim_product):,} rows "
          f"({(dim_product['product_category_en'] != 'unknown').sum():,} enriched)")
    print(f"  dim_customer: {len(dim_customer):,} rows")
    print(f"  dim_seller:   {len(dim_seller):,} rows")
    print(f"  dim_date:     {len(dim_date):,} rows")


if __name__ == "__main__":
    main()
