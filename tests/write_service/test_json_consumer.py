"""
Tests for the JSON consumer and ensure data is stored in a database successfully and in the right format.
"""

from unittest.mock import patch
from sqlalchemy import create_engine, text
from src.write_service.consumers.json_consumer import save_into_database

def test_save_to_database():
    """
    This is a function that ensures that sample data is stored correctly in a test database.
    """
    
    test_data = [{"traffic count": 24}, {"traffic count": 34}]
    
    # We don't want to use the real database, so we must use a temporary one in memory (deletes when connection closed)
    engine = create_engine("sqlite:///:memory:")

    # We will use patch to replace the create_engine() function so it uses the temporary engine not the real one
    with patch("src.write_service.consumers.json_consumer.create_engine") as original_engine:
        original_engine.return_value = engine
        session = save_into_database(test_data, "test", "Traffic Count")

        # Let's see if the data is saved correctly or not
        result = session.execute(text("SELECT * FROM test")).fetchall()

        # There should only be 2 rows
        assert len(result) == 2

        # Check to see if the cell data is accurate
        assert result[0].name == "Traffic Count Entity #1"
        assert result[0].content == '{"traffic count": 24}'
        assert result[1].name == "Traffic Count Entity #2"
        assert result[1].content == '{"traffic count": 34}'
        session.close()