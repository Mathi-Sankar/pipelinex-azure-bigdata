# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver → Gold with MongoDB Enrichment (Azure Databricks)
# MAGIC
# MAGIC **Author:** Mathi Sankar M R · PipelineX

# COMMAND ----------

# MAGIC %pip install pymongo[srv]==4.6.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pymongo import MongoClient
import pandas as pd

dbutils.widgets.text("storage_account", "pipelinexlake700", "Storage Account Name")
dbutils.widgets.text("storage_key", "", "Storage Account Access Key")
dbutils.widgets.text("mongo_uri", "", "MongoDB URI")

STORAGE = dbutils.widgets.get("storage_account")
KEY = dbutils.widgets.get("storage_key")
MONGO_URI = dbutils.widgets.get("mongo_uri")
assert KEY and len(KEY) > 50, "Paste storage key"
assert MONGO_URI.startswith("mongodb+srv://"), "Paste MongoDB URI"

spark.conf.set(f"fs.azure.account.key.{STORAGE}.dfs.core.windows.net", KEY)

SILVER = f"abfss://silver@{STORAGE}.dfs.core.windows.net/olist"
GOLD   = f"abfss://gold@{STORAGE}.dfs.core.windows.net/olist"

# COMMAND ----------

orders    = spark.read.format("delta").load(f"{SILVER}/orders")
items     = spark.read.format("delta").load(f"{SILVER}/order_items")
customers = spark.read.format("delta").load(f"{SILVER}/customers")
products  = spark.read.format("delta").load(f"{SILVER}/products")
sellers   = spark.read.format("delta").load(f"{SILVER}/sellers")
payments  = spark.read.format("delta").load(f"{SILVER}/payments")
reviews   = spark.read.format("delta").load(f"{SILVER}/reviews")

# COMMAND ----------

client = MongoClient(MONGO_URI)
docs = list(client["pipelinex"]["product_catalogue"].find({}, {"_id": 0}))
print(f"Fetched {len(docs)} enrichment docs from MongoDB Atlas")

catalogue_pdf = pd.DataFrame(docs)[["product_id", "category_en", "tags", "margin_pct"]]
catalogue = spark.createDataFrame(catalogue_pdf).withColumnRenamed("category_en", "product_category_en")

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

dim_date = (
    orders.select(F.to_date("order_purchase_timestamp").alias("date_key")).distinct()
    .filter(F.col("date_key").isNotNull())
    .withColumn("year", F.year("date_key"))
    .withColumn("quarter", F.quarter("date_key"))
    .withColumn("month", F.month("date_key"))
    .withColumn("month_name", F.date_format("date_key", "MMMM"))
    .withColumn("day", F.dayofmonth("date_key"))
    .withColumn("day_of_week", F.date_format("date_key", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("date_key").isin(1, 7))
)
(dim_date.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD}/dim_date"))

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

(fact_sales.write.format("delta").mode("overwrite")
    .partitionBy("order_date")
    .option("overwriteSchema", "true").save(f"{GOLD}/fact_sales"))

# COMMAND ----------

spark.sql(f"OPTIMIZE delta.`{GOLD}/fact_sales` ZORDER BY (customer_id, product_id)")

# COMMAND ----------

n = spark.read.format("delta").load(f"{GOLD}/fact_sales").count()
print(f"Gold fact_sales rows: {n:,}")

# COMMAND ----------

# MAGIC %md ### Top-10 revenue by category — proof the Azure pipeline works

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
