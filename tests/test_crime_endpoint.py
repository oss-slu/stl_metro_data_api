import pytest
from read_service.app import app, engine
from sqlalchemy import text

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

def seed_test_data():
    """Insert mock rows into stlouis_gov_crime."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM stlouis_gov_crime"))

        conn.execute(text("""
            INSERT INTO stlouis_gov_crime 
            (id, created_on, data_posted_on, is_active, description)
            VALUES
            (1, NOW(), NOW(), 1, 'Test Crime Active'),
            (2, NOW(), NOW(), 0, 'Inactive Crime'),
            (3, NOW(), NOW(), 1, 'Another Active Crime')
        """))

def test_get_crime_basic(client):
    seed_test_data()

    response = client.get("/api/crime")
    data = response.get_json()

    assert response.status_code == 200
    assert "crimes" in data
    assert len(data["crimes"]) == 2  # Only active records
    assert data["total"] == 2

def test_get_crime_pagination(client):
    seed_test_data()

    response = client.get("/api/crime?page=1&page_size=1")
    data = response.get_json()

    assert response.status_code == 200
    assert len(data["crimes"]) == 1
    assert data["total_pages"] == 2
