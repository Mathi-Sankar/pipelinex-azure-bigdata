# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze → Silver
# MAGIC
# MAGIC **Author:** Mathi Sankar M R · PipelineX
# MAGIC
# MAGIC Reads raw Olist CSVs landed by ADF in ADLS Gen2 Bronze, applies
# MAGIC schema, cleans nulls / duplicates, and writes typed Delta tables to Silver.
# MAGIC
# MAGIC **Inputs (Bronze):**
# MAGIC - `abfss://bronze@<storage>.dfs.core.windows.net/olist/<table>/*.csv`
# MAGIC
# MAGIC **Outputs (Silver):**
# MAGIC - `abfss://silver@<storage>.dfs.core.windows.net/olist/<table>` (Delta)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType,
)

# COMMAND ----------

dbutils.widgets.text("storage_account", "pipelinexlake")
dbutils.widgets.text("bronze_container", "bronze")
dbutils.widgets.text("silver_container", "silver")

STORAGE = dbutils.widgets.get("storage_account")
BRONZE = f"abfss://{dbutils.widgets.get('bronze_container')}@{STORAGE}.dfs.core.windows.net/olist"
SILVER = f"abfss://{dbutils.widgets.get('silver_container')}@{STORAGE}.dfs.core.windows.net/olist"

# COMMAND ----------

# MAGIC %md ## Schemas — pinned so a bad CSV row can't drift types

# COMMAND ----------

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
])

order_items_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
])

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", DoubleType(), True),
    StructField("product_length_cm", DoubleType(), True),
    StructField("product_height_cm", DoubleType(), True),
    StructField("product_width_cm", DoubleType(), True),
])

sellers_schema = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
])

payments_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), False),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
])

reviews_schema = StructType([
    StructField("review_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("review_score", IntegerType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", TimestampType(), True),
    StructField("review_answer_timestamp", TimestampType(), True),
])

# COMMAND ----------

def csv_to_silver(name: str, schema, dedup_keys: list[str]) -> None:
    src = f"{BRONZE}/{name}"
    dst = f"{SILVER}/{name}"
    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .schema(schema)
        .csv(src)
        .dropDuplicates(dedup_keys)
        .withColumn("_ingest_ts", F.current_timestamp())
    )
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(dst)
    )
    print(f"{name}: wrote {df.count():,} rows → {dst}")

# COMMAND ----------

csv_to_silver("orders", orders_schema, ["order_id"])
csv_to_silver("order_items", order_items_schema, ["order_id", "order_item_id"])
csv_to_silver("customers", customers_schema, ["customer_id"])
csv_to_silver("products", products_schema, ["product_id"])
csv_to_silver("sellers", sellers_schema, ["seller_id"])
csv_to_silver("payments", payments_schema, ["order_id", "payment_sequential"])
csv_to_silver("reviews", reviews_schema, ["review_id"])

# COMMAND ----------

# MAGIC %md ## Quality gate — fail the run if orders drop below a floor

# COMMAND ----------

n_orders = spark.read.format("delta").load(f"{SILVER}/orders").count()
assert n_orders > 0, "Silver orders is empty — upstream Bronze load likely failed"
print(f"Silver quality gate passed: {n_orders:,} orders")
