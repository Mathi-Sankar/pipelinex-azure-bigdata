"""API tests for the PipelineX Analytics API.

Run: pytest api/test_main.py -v
"""

from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


def test_health():
    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_kpis_shape_and_values():
    r = client.get("/api/kpis")
    assert r.status_code == 200
    body = r.json()
    # Every expected KPI is present.
    for key in [
        "total_revenue", "total_orders", "total_customers",
        "total_products", "total_sellers", "avg_review_score", "avg_delivery_days",
    ]:
        assert key in body
    # Sanity checks on the known dataset.
    assert body["total_orders"] > 90_000
    assert body["total_products"] == 32_951
    assert 0 <= body["avg_review_score"] <= 5


def test_top_categories_default_limit():
    r = client.get("/api/top-categories")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 10
    # Sorted descending by revenue.
    revenues = [row["revenue"] for row in data]
    assert revenues == sorted(revenues, reverse=True)
    # "unknown" bucket must be filtered out.
    assert all(row["category"] != "unknown" for row in data)


def test_top_categories_respects_limit():
    r = client.get("/api/top-categories?limit=3")
    assert r.status_code == 200
    assert len(r.json()) == 3


def test_top_categories_limit_validation():
    # limit above the allowed max is rejected.
    assert client.get("/api/top-categories?limit=999").status_code == 422


def test_state_revenue_sorted():
    r = client.get("/api/state-revenue")
    assert r.status_code == 200
    data = r.json()
    assert len(data) > 0
    revenues = [row["revenue"] for row in data]
    assert revenues == sorted(revenues, reverse=True)
    # São Paulo leads Brazilian e-commerce.
    assert data[0]["state"] == "SP"


def test_revenue_trend_chronological():
    r = client.get("/api/revenue-trend")
    assert r.status_code == 200
    months = [row["month"] for row in r.json()]
    assert months == sorted(months)


def test_top_sellers():
    r = client.get("/api/top-sellers?limit=5")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 5
    assert all("seller_id" in row for row in data)


def test_frontend_served_at_root():
    r = client.get("/")
    assert r.status_code == 200
    assert "PipelineX" in r.text
