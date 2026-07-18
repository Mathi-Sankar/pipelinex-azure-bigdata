"""Seed MongoDB Atlas with the product-catalogue enrichment collection.

Author: Mathi Sankar M R <mathisankar707@gmail.com>
Project: PipelineX — Azure Big Data Pipeline

Reads olist_products_dataset.csv + product_category_name_translation.csv,
joins on category name, generates tags + a fake margin_pct, and upserts
into `pipelinex.product_catalogue` in Atlas.

Env vars:
    MONGO_URI — connection string (mongodb+srv://...)

Usage:
    python scripts/seed_mongodb.py --data-dir data/samples
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import sys
from pathlib import Path

try:
    from pymongo import MongoClient, UpdateOne
except ImportError:
    sys.exit("pymongo not installed. Run: pip install 'pymongo[srv]'")

TAG_POOL = {
    "electronics": ["gadget", "tech", "wired"],
    "furniture": ["home", "bulky", "assembly"],
    "beauty": ["skincare", "giftable"],
    "books": ["media", "lightweight"],
    "sports": ["outdoor", "fitness"],
    "toys": ["kids", "giftable"],
    "food": ["consumable", "perishable"],
    "fashion": ["apparel", "seasonal"],
}


def load_translations(path: Path) -> dict[str, str]:
    with path.open(encoding="utf-8-sig") as f:
        return {r["product_category_name"]: r["product_category_name_english"] for r in csv.DictReader(f)}


def tags_for(category_en: str) -> list[str]:
    for key, pool in TAG_POOL.items():
        if key in category_en:
            return pool
    return ["general"]


def build_docs(products_csv: Path, translation_csv: Path) -> list[dict]:
    xlat = load_translations(translation_csv)
    rng = random.Random(42)
    docs = []
    with products_csv.open(encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            cat_pt = row["product_category_name"] or ""
            cat_en = xlat.get(cat_pt, "unknown")
            docs.append({
                "product_id": row["product_id"],
                "category_pt": cat_pt,
                "category_en": cat_en,
                "tags": tags_for(cat_en),
                "margin_pct": round(rng.uniform(0.08, 0.42), 3),
            })
    return docs


def upsert(uri: str, docs: list[dict]) -> None:
    client = MongoClient(uri)
    coll = client["pipelinex"]["product_catalogue"]
    coll.create_index("product_id", unique=True)
    ops = [UpdateOne({"product_id": d["product_id"]}, {"$set": d}, upsert=True) for d in docs]
    result = coll.bulk_write(ops, ordered=False)
    print(f"upserted={result.upserted_count} modified={result.modified_count} total_docs={coll.count_documents({})}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data/samples")
    args = ap.parse_args()

    uri = os.environ.get("MONGO_URI")
    if not uri:
        sys.exit("Set MONGO_URI env var (mongodb+srv://user:pass@cluster.../).")

    data_dir = Path(args.data_dir)
    docs = build_docs(
        data_dir / "olist_products_dataset.csv",
        data_dir / "product_category_name_translation.csv",
    )
    print(f"Prepared {len(docs)} product docs — seeding Atlas...")
    upsert(uri, docs)


if __name__ == "__main__":
    main()
