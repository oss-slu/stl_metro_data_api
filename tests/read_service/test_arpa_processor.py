"""
Tests for the ARPA funds processor (read service) and ensure data can be read from the database correctly
"""

from unittest.mock import patch
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, DeclarativeBase
from read_service.processors.arpa_processor import Base, DataTable, retrieve_from_database

def test_save_to_database():
    """
    This is a function that ensures that sample data can be retrieved correctly from a test database
    """

    test_data = [{"organization": "United Way"}, {"organization": "Urban League"}]

    # We don't want to use the real database, so we must use a temporary one in memory (deletes when connection closed)
    engine = create_engine("sqlite:///:memory:")

    session = Session(engine)

    # Create the table if not created yet
    Base.metadata.create_all(engine)
    
    with Session(engine) as session:
        entity_counter = 1

        # Save the sample data in the test database
        for entity in test_data:
            new_row = DataTable(
                name = "ARPA Funds Entity #" + str(entity_counter),
                content = entity,
                is_active = True)
            session.add(new_row)
            entity_counter += 1

        session.commit() 

    # We will use patch to replace the create_engine() function so it uses the temporary engine not the real one
    with patch("read_service.processors.arpa_processor.create_engine") as original_engine:
        original_engine.return_value = engine
        
        # Let's see if the data is retrieved correctly or not
        result = retrieve_from_database()

        # There should only be 2 rows
        assert len(result) == 2

        # Check to see if only active data is returned as expected
        assert result[0].get("name") == "ARPA Funds Entity #1"
        assert result[0].get("content") == {'organization': 'United Way'}
        assert result[0].get("is_active") == True
        assert result[1].get("name") == "ARPA Funds Entity #2"
        assert result[1].get("content") == {'organization': 'Urban League'}
        assert result[0].get("is_active") == True

        session.close()
