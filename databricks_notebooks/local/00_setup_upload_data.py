# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup: upload data + install pymongo
# MAGIC
# MAGIC **Author:** Mathi Sankar M R · PipelineX
# MAGIC
# MAGIC **Run this once** before the pipeline notebooks. It:
# MAGIC 1. Installs `pymongo` on the cluster
# MAGIC 2. Verifies the uploaded Olist CSVs land at `/Volumes/workspace/default/olist_raw/`
# MAGIC 3. Creates the Bronze / Silver / Gold folders under `/tmp/pipelinex/`
# MAGIC
# MAGIC ### Upload the CSVs first
# MAGIC Left sidebar → **Catalog** → **workspace** → **default** → **+ Create → Volume**
# MAGIC (managed, name it `olist_raw`) → open the volume → **Upload** → drop all 8 files
# MAGIC from `data/samples/`.

# COMMAND ----------

# MAGIC %pip install pymongo[srv]==4.6.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Free Edition disables /tmp and /dbfs paths, so we stage everything inside
# a Unity Catalog Volume. The volume itself was created in the setup step;
# Spark will create the subfolders on first write, so no mkdirs needed.
RAW_VOL = "/Volumes/workspace/default/olist_raw"
SILVER  = "/Volumes/workspace/default/olist_raw/silver"
GOLD    = "/Volumes/workspace/default/olist_raw/gold"

files = [f for f in dbutils.fs.ls(RAW_VOL) if f.name.endswith(".csv")]
print(f"Found {len(files)} raw CSVs in {RAW_VOL}:")
for f in files:
    print(f"  {f.name}  ({f.size:,} bytes)")

assert len(files) >= 7, "Expected at least 7 CSVs — did you upload them all to the volume?"
print(f"\nSilver will be written to: {SILVER}")
print(f"Gold   will be written to: {GOLD}")
print("\nSetup OK — proceed to notebook 01.")
