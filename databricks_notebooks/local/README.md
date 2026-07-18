# Databricks Free Edition — Local Run

Free Edition doesn't let you attach the MongoDB Spark connector JAR and
doesn't have ADLS, so we use adapted notebooks:

| Notebook | Purpose |
| --- | --- |
| `00_setup_upload_data.py` | Installs `pymongo`, verifies uploaded CSVs, creates staging folders |
| `01_bronze_to_silver_local.py` | Same logic as prod, reads from Unity Catalog Volume, writes Delta to `/tmp/pipelinex/silver` |
| `02_silver_to_gold_local.py` | Enriches via `pymongo` → Spark DataFrame instead of the Spark MongoDB connector |

## Setup steps

1. **Upload CSVs**
   In Databricks → **Catalog** → **workspace** → **default** →
   **+ Create → Volume** (managed, name `olist_raw`).
   Open the volume → **Upload** → drop all 8 CSVs from
   `data/samples/`.

2. **Import notebooks**
   Workspace → **Import** → pick these three `.py` files. Databricks auto-detects
   the notebook format.

3. **Run in order**
   - Run `00_setup_upload_data` — confirms files landed
   - Run `01_bronze_to_silver_local` — should show 7 tables written
   - Open `02_silver_to_gold_local`, paste your MongoDB URI into the widget,
     then Run All

4. **Verify** — the last cell of notebook 02 displays a top-10 revenue-by-category
   table.

## When to use these vs the top-level notebooks

- **`databricks_notebooks/local/`** — local demo (this folder). Portable to
  Databricks Free Edition, no Azure or connector JAR needed.
- **`databricks_notebooks/`** — production notebooks. Deploy to Azure
  Databricks with the certified MongoDB Spark connector attached and the
  `pipelinex` secret scope populated.
