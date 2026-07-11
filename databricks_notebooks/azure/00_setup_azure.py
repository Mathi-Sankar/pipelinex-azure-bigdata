# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Azure Setup: Configure ADLS Access + Install pymongo
# MAGIC
# MAGIC **Author:** Mathi Sankar M R · PipelineX
# MAGIC
# MAGIC Configures the cluster to read/write ADLS Gen2 using the storage account
# MAGIC access key, and installs `pymongo` for MongoDB enrichment.
# MAGIC
# MAGIC **Prereq:** upload Olist CSVs to `bronze/olist/` in your ADLS container.

# COMMAND ----------

# MAGIC %pip install pymongo[srv]==4.6.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Storage account access — paste your key in the widget when the notebook first runs.
# For prod, use a Databricks secret scope (dbutils.secrets.get) instead of a widget.
dbutils.widgets.text("storage_account", "pipelinexlake700", "Storage Account Name")
dbutils.widgets.text("storage_key", "", "Storage Account Access Key")

STORAGE = dbutils.widgets.get("storage_account")
KEY = dbutils.widgets.get("storage_key")
assert KEY and len(KEY) > 50, "Paste your storage account key into the widget above"

spark.conf.set(
    f"fs.azure.account.key.{STORAGE}.dfs.core.windows.net",
    KEY,
)

# COMMAND ----------

BRONZE = f"abfss://bronze@{STORAGE}.dfs.core.windows.net/olist"
SILVER = f"abfss://silver@{STORAGE}.dfs.core.windows.net/olist"
GOLD   = f"abfss://gold@{STORAGE}.dfs.core.windows.net/olist"

files = dbutils.fs.ls(BRONZE)
print(f"Found {len(files)} files in Bronze:")
for f in files:
    print(f"  {f.name}  ({f.size:,} bytes)")

assert len(files) >= 7, "Upload at least 7 CSVs to bronze/olist/ before continuing"
print(f"\nBronze:  {BRONZE}")
print(f"Silver:  {SILVER}")
print(f"Gold:    {GOLD}")
print("\nSetup OK — proceed to notebook 01.")
