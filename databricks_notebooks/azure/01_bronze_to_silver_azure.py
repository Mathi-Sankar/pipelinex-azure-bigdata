# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze → Silver (Azure Databricks + ADLS Gen2)
# MAGIC
# MAGIC **Author:** Mathi Sankar M R · PipelineX
# MAGIC
# MAGIC Reads Olist CSVs from ADLS Gen2 Bronze, applies pinned schemas,
# MAGIC deduplicates, and writes typed Delta tables to Silver.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType,
)

dbutils.widgets.text("storage_account", "pipelinexlake700", "Storage Account Name")
dbutils.widgets.text("storage_key", "", "Storage Account Access Key")

STORAGE = dbutils.widgets.get("storage_account")
KEY = dbutils.widgets.get("storage_key")
assert KEY and len(KEY) > 50, "Paste your storage account key into the widget above"

spark.conf.set(
    f"fs.azure.account.key.{STORAGE}.dfs.core.windows.net",
    KEY,
)

BRONZE = f"abfss://bronze@{STORAGE}.dfs.core.windows.net/olist"
SILVER = f"abfss://silver@{STORAGE}.dfs.core.windows.net/olist"

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

def csv_to_silver(filename, name, schema, dedup_keys):
    src = f"{BRONZE}/{filename}"
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
    (df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").save(dst))
    print(f"{name}: wrote {df.count():,} rows -> {dst}")

# COMMAND ----------

csv_to_silver("olist_orders_dataset.csv",         "orders",      orders_schema,      ["order_id"])
csv_to_silver("olist_order_items_dataset.csv",    "order_items", order_items_schema, ["order_id", "order_item_id"])
csv_to_silver("olist_customers_dataset.csv",      "customers",   customers_schema,   ["customer_id"])
csv_to_silver("olist_products_dataset.csv",       "products",    products_schema,    ["product_id"])
csv_to_silver("olist_sellers_dataset.csv",        "sellers",     sellers_schema,     ["seller_id"])
csv_to_silver("olist_order_payments_dataset.csv", "payments",    payments_schema,    ["order_id", "payment_sequential"])
csv_to_silver("olist_order_reviews_dataset.csv",  "reviews",     reviews_schema,     ["review_id"])

# COMMAND ----------

n_orders = spark.read.format("delta").load(f"{SILVER}/orders").count()
assert n_orders > 0, "Silver orders is empty!"
print(f"Silver quality gate passed: {n_orders:,} orders")
