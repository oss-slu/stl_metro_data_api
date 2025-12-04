# tests/read_service/test_building_permits_endpoint.py

from datetime import datetime, date
from sqlalchemy import text
import pytest

from read_service.app import app, engine

@pytest.fixture
def client():
    with app.test_client() as c:
        yield c

@pytest.fixture
def seed_building_permits():
    """
    Ensure the stlouis_building_permit table exists, then seed it with
    a mix of active and inactive rows.

    Uses raw SQL so tests don't depend on ORM models or migrations.
    """
    with engine.begin() as conn:
        # 1) Create table if it doesn't exist yet
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stlouis_building_permit (
                id SERIAL PRIMARY KEY,
                neighborhood TEXT NOT NULL,
                total_permits INTEGER NOT NULL,
                total_value NUMERIC(14, 2) NOT NULL,
                avg_days_to_issue INTEGER,
                created_on TIMESTAMP NOT NULL,
                data_posted_on DATE NOT NULL,
                is_active BOOLEAN NOT NULL
            );
        """))

        # 2) Clean out any existing rows
        conn.execute(text("DELETE FROM stlouis_building_permit;"))

        # 3) Insert 2 active + 1 inactive
        conn.execute(
            text("""
                INSERT INTO stlouis_building_permit
                    (neighborhood, total_permits, total_value, avg_days_to_issue,
                     created_on, data_posted_on, is_active)
                VALUES
                    (:n1, :tp1, :tv1, :avg1, :created1, :posted1, TRUE),
                    (:n2, :tp2, :tv2, :avg2, :created2, :posted2, TRUE),
                    (:n3, :tp3, :tv3, :avg3, :created3, :posted3, FALSE)
            """),
            {
                "n1": "Shaw",
                "tp1": 100,
                "tv1": 1_000_000.00,
                "avg1": 10,
                "created1": datetime(2025, 1, 1, 12, 0, 0),
                "posted1": date(2025, 1, 1),

                "n2": "Soulard",
                "tp2": 50,
                "tv2": 500_000.00,
                "avg2": 8,
                "created2": datetime(2025, 2, 1, 12, 0, 0),
                "posted2": date(2025, 2, 1),

                "n3": "Shaw",
                "tp3": 80,
                "tv3": 800_000.00,
                "avg3": 12,
                "created3": datetime(2024, 12, 1, 12, 0, 0),
                "posted3": date(2024, 12, 1),
            }
        )

def test_get_building_returns_only_active(client, seed_building_permits):
    """
    GET /api/building should return only rows where is_active = TRUE.
    """
    resp = client.get("/api/building")
    assert resp.status_code == 200

    data = resp.get_json()
    assert "results" in data
    assert data["total"] == 2
    assert len(data["results"]) == 2

    neighborhoods = {row["neighborhood"] for row in data["results"]}
    assert neighborhoods == {"Shaw", "Soulard"}

    # ensure all returned rows are active
    for row in data["results"]:
        assert row["is_active"] is True

def test_pagination_works(client, seed_building_permits):
    """
    Pagination params page + page_size must control result size.
    """
    r1 = client.get("/api/building?page=1&page_size=1")
    assert r1.status_code == 200
    d1 = r1.get_json()
    assert d1["page"] == 1
    assert d1["page_size"] == 1
    assert d1["total"] == 2
    assert len(d1["results"]) == 1

    r2 = client.get("/api/building?page=2&page_size=1")
    assert r2.status_code == 200
    d2 = r2.get_json()
    assert d2["page"] == 2
    assert d2["page_size"] == 1
    assert d2["total"] == 2
    assert len(d2["results"]) == 1

    names = {d1["results"][0]["neighborhood"], d2["results"][0]["neighborhood"]}
    assert names == {"Shaw", "Soulard"}

def test_rate_limit_stub_does_not_block(client, seed_building_permits):
    """
    Rate limiting stub should not block requests (no 429).
    """
    resp = client.get("/api/building")
    assert resp.status_code == 200
