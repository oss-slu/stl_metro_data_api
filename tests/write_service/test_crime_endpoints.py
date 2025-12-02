import pytest

from read_service.app import app, SessionLocal, engine
# from app import app, SessionLocal, engine
from sqlalchemy import text

@pytest.fixture(scope="module")
def test_client():
    app.config["TESTING"] = True
    client = app.test_client()

    # Prepare database with sample rows
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM stlouis_gov_crime"))

        conn.execute(text("""
            INSERT INTO stlouis_gov_crime (id, created_on, data_posted_on, is_active, raw_json)
            VALUES
            (1, NOW(), NOW(), 1, '{"crime": "robbery"}'),
            (2, NOW(), NOW(), 1, '{"crime": "theft"}'),
            (3, NOW(), NOW(), 0, '{"crime": "inactive"}')
        """))
        conn.commit()

    return client


def test_health_ok(test_client):
    res = test_client.get("/health")
    assert res.status_code == 200
    body = res.get_json()
    assert body["service"] == "read_service"
    assert "postgres" in body


def test_query_stub(test_client):
    res = test_client.get("/query-stub")
    assert res.status_code == 200
    assert res.get_json() == {"message": "This is a query stub endpoint"}


def test_crime_default_pagination(test_client):
    """GET /api/crime should return only active records and default pagination."""
    res = test_client.get("/api/crime")
    assert res.status_code == 200

    data = res.get_json()
    assert data["page"] == 1
    assert data["page_size"] == 50
    assert data["total"] == 2            # only 2 active rows
    assert data["total_pages"] == 1
    assert len(data["crimes"]) == 2

    # Validate fields
    crime = data["crimes"][0]
    assert "id" in crime
    assert "created_on" in crime
    assert "data_posted_on" in crime
    assert crime["is_active"] is True
    assert isinstance(crime["raw_json"], dict)


def test_crime_pagination_page_1(test_client):
    res = test_client.get("/api/crime?page=1&page_size=1")
    assert res.status_code == 200

    data = res.get_json()
    assert data["page"] == 1
    assert data["page_size"] == 1
    assert data["total"] == 2
    assert data["total_pages"] == 2
    assert len(data["crimes"]) == 1


def test_crime_pagination_page_2(test_client):
    res = test_client.get("/api/crime?page=2&page_size=1")
    assert res.status_code == 200

    data = res.get_json()
    assert data["page"] == 2
    assert len(data["crimes"]) == 1


def test_crime_invalid_pagination(test_client):
    res = test_client.get("/api/crime?page=abc&page_size=def")
    assert res.status_code == 400
