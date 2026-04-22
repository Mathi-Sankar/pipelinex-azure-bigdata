# ADF Pipelines

Two pipelines drive the Bronze layer:

| Pipeline | Source | Sink | Downstream |
| --- | --- | --- | --- |
| `pl_ingest_csv_to_bronze` | HTTP / Blob-staged Olist CSVs | ADLS Gen2 `bronze/olist/<table>/ingest_date=YYYY-MM-DD/` | Triggers `01_bronze_to_silver` on Databricks |
| `pl_ingest_mongodb_to_bronze` | MongoDB Atlas `pipelinex.product_catalogue` | ADLS Gen2 `bronze/mongo/product_catalogue/ingest_date=YYYY-MM-DD/*.json` | Triggers `02_silver_to_gold` on Databricks |

## Linked services required

- `ls_http_olist` — HTTP linked service (base URL of your Olist staging bucket)
- `ls_mongo_atlas` — MongoDB Atlas connection (uses connection string secret)
- `ls_adls_bronze` — ADLS Gen2 with managed identity
- `ls_databricks` — Databricks linked service (access token in Key Vault)

## Datasets required

- `ds_http_olist_csv` — parameterised on `table`
- `ds_adls_bronze_csv` — parameterised on `table`, `ingest_date`
- `ds_mongo_product_catalogue`
- `ds_adls_bronze_mongo` — parameterised on `collection`, `ingest_date`

## Trigger schedule

A single tumbling-window trigger `tr_daily_0200_utc` fires both pipelines at 02:00 UTC daily.
