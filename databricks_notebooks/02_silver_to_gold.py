# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver → Gold (Star Schema + MongoDB Enrichment)
# MAGIC
# MAGIC **Author:** Mathi Sankar M R · PipelineX
# MAGIC
# MAGIC Joins Silver tables with the MongoDB product-catalogue collection to
# MAGIC produce a star schema over Delta:
# MAGIC
# MAGIC - `fact_sales` — grain: one row per order_item
# MAGIC - `dim_customer`, `dim_product`, `dim_seller`, `dim_date`
# MAGIC
# MAGIC MongoDB is read via the certified Spark connector as `format("mongodb")`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("storage_account", "pipelinexlake")
dbutils.widgets.text("silver_container", "silver")
dbutils.widgets.text("gold_container", "gold")
dbutils.widgets.text("mongo_database", "pipelinex")
dbutils.widgets.text("mongo_collection", "product_catalogue")

STORAGE = dbutils.widgets.get("storage_account")
SILVER = f"abfss://{dbutils.widgets.get('silver_container')}@{STORAGE}.dfs.core.windows.net/olist"
GOLD = f"abfss://{dbutils.widgets.get('gold_container')}@{STORAGE}.dfs.core.windows.net/olist"

# Connection URI is stored in a Databricks secret scope — never inline it.
mongo_uri = dbutils.secrets.get(scope="pipelinex", key="mongo_uri")
mongo_db = dbutils.widgets.get("mongo_database")
mongo_coll = dbutils.widgets.get("mongo_collection")

# COMMAND ----------

# MAGIC %md ## Load Silver

# COMMAND ----------

orders = spark.read.format("delta").load(f"{SILVER}/orders")
items = spark.read.format("delta").load(f"{SILVER}/order_items")
customers = spark.read.format("delta").load(f"{SILVER}/customers")
products = spark.read.format("delta").load(f"{SILVER}/products")
sellers = spark.read.format("delta").load(f"{SILVER}/sellers")
payments = spark.read.format("delta").load(f"{SILVER}/payments")
reviews = spark.read.format("delta").load(f"{SILVER}/reviews")

# COMMAND ----------

# MAGIC %md ## Read MongoDB product catalogue (English names, tags, margins)

# COMMAND ----------

catalogue = (
    spark.read
    .format("mongodb")
    .option("connection.uri", mongo_uri)
    .option("database", mongo_db)
    .option("collection", mongo_coll)
    .load()
    .select(
        F.col("product_id"),
        F.col("category_en").alias("product_category_en"),
        F.col("tags"),
        F.col("margin_pct"),
    )
)

# COMMAND ----------

# MAGIC %md ## dim_product — SCD Type 2 on category_en

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

(
    products_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD}/dim_product")
)

# COMMAND ----------

# MAGIC %md ## dim_customer, dim_seller

# COMMAND ----------

(
    customers.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD}/dim_customer")
)
(
    sellers.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD}/dim_seller")
)

# COMMAND ----------

# MAGIC %md ## dim_date — derived from order purchase timestamps

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

(
    dim_date.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD}/dim_date")
)

# COMMAND ----------

# MAGIC %md ## fact_sales — grain = order_item
# MAGIC One payment allocated proportionally across items in the order by price share.

# COMMAND ----------

order_total_price = items.groupBy("order_id").agg(F.sum("price").alias("order_total_price"))

payment_per_order = payments.groupBy("order_id").agg(F.sum("payment_value").alias("payment_total"))

review_first = (
    reviews.withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("order_id").orderBy(F.col("review_creation_date").desc())
        ),
    )
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
        F.when(
            F.col("t.order_total_price") > 0,
            F.col("p.payment_total") * F.col("i.price") / F.col("t.order_total_price"),
        ).alias("allocated_payment"),
        F.col("r.review_score"),
        F.datediff(
            F.col("o.order_delivered_customer_date"),
            F.col("o.order_purchase_timestamp"),
        ).alias("delivery_days"),
    )
)

(
    fact_sales.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_date")
    .option("overwriteSchema", "true")
    .save(f"{GOLD}/fact_sales")
)

# COMMAND ----------

# MAGIC %md ## OPTIMIZE + ZORDER for BI query patterns

# COMMAND ----------

spark.sql(f"OPTIMIZE delta.`{GOLD}/fact_sales` ZORDER BY (customer_id, product_id)")

# COMMAND ----------

n = spark.read.format("delta").load(f"{GOLD}/fact_sales").count()
print(f"Gold fact_sales rows: {n:,}")
