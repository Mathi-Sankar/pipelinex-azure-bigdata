# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver → Gold with MongoDB Enrichment (Databricks Free Edition)
# MAGIC
# MAGIC **Author:** Mathi Sankar M R · PipelineX
# MAGIC
# MAGIC Local variant. Uses **pymongo** (installed in notebook 00) instead of
# MAGIC the Spark MongoDB connector — Free Edition doesn't allow attaching the
# MAGIC connector JAR. We pull the catalogue collection into pandas, convert
# MAGIC to Spark, then continue.

# COMMAND ----------

# MAGIC %pip install pymongo[srv]==4.6.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pymongo import MongoClient
import pandas as pd

# COMMAND ----------

# MAGIC %md ## Config
# MAGIC Paste your MongoDB connection string in the widget below when the notebook
# MAGIC first runs. In real Azure use Databricks secrets; Free Edition can't.

# COMMAND ----------

dbutils.widgets.text("mongo_uri", "", "MongoDB URI")
MONGO_URI = dbutils.widgets.get("mongo_uri")
assert MONGO_URI.startswith("mongodb+srv://"), "Fill in the mongo_uri widget above"

SILVER = "/Volumes/workspace/default/olist_raw/silver"
GOLD   = "/Volumes/workspace/default/olist_raw/gold"

# COMMAND ----------

orders    = spark.read.format("delta").load(f"{SILVER}/orders")
items     = spark.read.format("delta").load(f"{SILVER}/order_items")
customers = spark.read.format("delta").load(f"{SILVER}/customers")
products  = spark.read.format("delta").load(f"{SILVER}/products")
sellers   = spark.read.format("delta").load(f"{SILVER}/sellers")
payments  = spark.read.format("delta").load(f"{SILVER}/payments")
reviews   = spark.read.format("delta").load(f"{SILVER}/reviews")

# COMMAND ----------

# MAGIC %md ## MongoDB enrichment via pymongo

# COMMAND ----------

client = MongoClient(MONGO_URI)
docs = list(client["pipelinex"]["product_catalogue"].find({}, {"_id": 0}))
print(f"Fetched {len(docs)} enrichment docs from MongoDB")

catalogue_pdf = pd.DataFrame(docs)[["product_id", "category_en", "tags", "margin_pct"]]
catalogue = spark.createDataFrame(catalogue_pdf).withColumnRenamed("category_en", "product_category_en")

# COMMAND ----------

# MAGIC %md ## dim_product (SCD-2 shape)

# COMMAND ----------

products_enriched = (
    products.alias("p")
    .join(catalogue.alias("c"), "product_id", "left")
    .select(
        "product_id",
        "product_category_name",
        F.coalesce(F.col("c.product_category_en"), F.lit("unknown")).alias("product_category_en"),
        F.col("c.tags"),
        F.col("c.margin_pct"),
        "product_weight_g",
    )
    .withColumn("effective_from", F.current_date())
    .withColumn("effective_to", F.lit(None).cast("date"))
    .withColumn("is_current", F.lit(True))
)

(products_enriched.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD}/dim_product"))

# COMMAND ----------

(customers.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD}/dim_customer"))
(sellers.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD}/dim_seller"))

# COMMAND ----------

# MAGIC %md ## dim_date

# COMMAND ----------

dim_date = (
    orders.select(F.to_date("order_purchase_timestamp").alias("date_key")).distinct()
    .filter(F.col("date_key").isNotNull())
    .withColumn("year",  F.year("date_key"))
    .withColumn("quarter", F.quarter("date_key"))
    .withColumn("month", F.month("date_key"))
    .withColumn("month_name", F.date_format("date_key", "MMMM"))
    .withColumn("day",   F.dayofmonth("date_key"))
    .withColumn("day_of_week", F.date_format("date_key", "EEEE"))
    .withColumn("is_weekend",  F.dayofweek("date_key").isin(1, 7))
)
(dim_date.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD}/dim_date"))

# COMMAND ----------

# MAGIC %md ## fact_sales (grain = order_item)

# COMMAND ----------

order_total_price = items.groupBy("order_id").agg(F.sum("price").alias("order_total_price"))
payment_per_order = payments.groupBy("order_id").agg(F.sum("payment_value").alias("payment_total"))
review_first = (
    reviews.withColumn("rn", F.row_number().over(
        Window.partitionBy("order_id").orderBy(F.col("review_creation_date").desc())))
    .filter(F.col("rn") == 1)
    .select("order_id", "review_score")
)

fact_sales = (
    items.alias("i")
    .join(orders.alias("o"), "order_id")
    .join(order_total_price.alias("t"), "order_id")
    .join(payment_per_order.alias("p"), "order_id", "left")
    .join(review_first.alias("r"), "order_id", "left")
    .select(
        F.col("i.order_id"),
        F.col("i.order_item_id"),
        F.col("i.product_id"),
        F.col("i.seller_id"),
        F.col("o.customer_id"),
        F.to_date("o.order_purchase_timestamp").alias("order_date"),
        F.col("o.order_status"),
        F.col("i.price"),
        F.col("i.freight_value"),
        (F.col("i.price") + F.col("i.freight_value")).alias("gross_revenue"),
        F.when(F.col("t.order_total_price") > 0,
               F.col("p.payment_total") * F.col("i.price") / F.col("t.order_total_price"))
            .alias("allocated_payment"),
        F.col("r.review_score"),
        F.datediff(F.col("o.order_delivered_customer_date"),
                   F.col("o.order_purchase_timestamp")).alias("delivery_days"),
    )
)

# Note: partitionBy is skipped on Free Edition Volumes (they enforce a flat
# folder policy on writes). Prod version keeps partitionBy('order_date').
(fact_sales.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD}/fact_sales"))

# COMMAND ----------

# MAGIC %md ## Verify + sample query

# COMMAND ----------

n = spark.read.format("delta").load(f"{GOLD}/fact_sales").count()
print(f"Gold fact_sales rows: {n:,}")

# COMMAND ----------

# MAGIC %md ### Quick BI-style query: revenue by category

# COMMAND ----------

display(
    spark.read.format("delta").load(f"{GOLD}/fact_sales").alias("f")
    .join(spark.read.format("delta").load(f"{GOLD}/dim_product").alias("p"), "product_id")
    .groupBy("product_category_en")
    .agg(F.round(F.sum("gross_revenue"), 2).alias("revenue"),
         F.count("*").alias("items_sold"))
    .orderBy(F.desc("revenue"))
    .limit(10)
)
