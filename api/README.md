# PipelineX Analytics API

A FastAPI service that exposes the Gold star-schema (produced by the Azure
Databricks pipeline) as a REST API, plus a single-page dashboard frontend.

## Endpoints

| Method | Path | Returns |
| --- | --- | --- |
| GET | `/` | Dashboard HTML |
| GET | `/api/health` | Liveness probe |
| GET | `/api/kpis` | Headline KPIs (revenue, orders, customers, review score) |
| GET | `/api/top-categories?limit=N` | Revenue + items by product category |
| GET | `/api/state-revenue` | Revenue + orders by Brazilian state |
| GET | `/api/revenue-trend` | Monthly revenue time-series |
| GET | `/api/top-sellers?limit=N` | Best sellers with delivery + review quality |

Interactive API docs auto-generated at `/docs` (Swagger UI).

## Run locally

```bash
pip install -r api/requirements.txt
uvicorn api.main:app --reload
# open http://localhost:8000
```

## Test

```bash
pytest api/test_main.py -v
```

## Deploy

Pushing to `main` triggers:
1. **GitHub Actions** (`.github/workflows/ci.yml`) — runs the test suite
2. **Render** (`render.yaml`) — builds and deploys the service, auto-deploy on green

## Architecture

```
Gold CSVs (from Azure Databricks pipeline)
        │
        ▼
   FastAPI (pandas aggregations, cached at startup)
        │
        ├── /api/* JSON endpoints
        └── / static dashboard (Chart.js)
```
