## tests/write_service/test_crime_endpoints.py
import pytest
from read_service.app import app, SessionLocal, engine
from sqlalchemy import text, inspect
from write_service.consumers.models import StLouisCrimeStats
from datetime import datetime

@pytest.fixture(scope="module")
def test_client():
    app.config["TESTING"] = True
    client = app.test_client()

    # Get list of existing columns in the table
    inspector = inspect(engine)
    existing_columns = [c["name"] for c in inspector.get_columns("stlouis_gov_crime")]

    # Map ORM columns to SQL definitions (simple defaults)
    required_columns = {
        "created_on": "TIMESTAMP DEFAULT NOW()",
        "data_posted_on": "TIMESTAMP DEFAULT NOW()",
        "is_active": "BOOLEAN DEFAULT TRUE",
        "raw_json": "JSON NOT NULL DEFAULT '{}'"
    }

    # Add any missing columns
    with engine.connect() as conn:
        for col, definition in required_columns.items():
            if col not in existing_columns:
                conn.execute(text(f"ALTER TABLE stlouis_gov_crime ADD COLUMN {col} {definition}"))

        conn.commit()

        # Clean table
        conn.execute(text("DELETE FROM stlouis_gov_crime"))

        # Insert test rows
        conn.execute(text("""
            INSERT INTO stlouis_gov_crime (id, created_on, data_posted_on, is_active, raw_json)
            VALUES
            (1, NOW(), NOW(), TRUE, '{"crime": "robbery"}'),
            (2, NOW(), NOW(), TRUE, '{"crime": "theft"}'),
            (3, NOW(), NOW(), FALSE, '{"crime": "inactive"}')
        """))
        conn.commit()

    return client
