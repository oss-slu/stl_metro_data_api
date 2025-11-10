# tests/write_service/test_web_consumer.py
"""
Tests for web consumer.
"""

import pytest
from unittest.mock import Mock, patch
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.write_service.consumers.models import Base, StLouisCensusData
from src.write_service.consumers.web_consumer import consume_web_data

@pytest.fixture
def test_db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    yield Session
    Base.metadata.drop_all(engine)

def test_consumer_inserts_data(test_db):
    """Test that consumer inserts Kafka messages into database."""
    # Mock Kafka consumer
    mock_message = Mock()
    mock_message.value = {"neighborhood": "Downtown", "population": 5442}
    
    with patch("src.write_service.consumers.web_consumer.KafkaConsumer") as MockConsumer:
        mock_consumer_instance = MockConsumer.return_value
        mock_consumer_instance.__iter__.return_value = [mock_message]
        
        # Run consumer (will process 1 message and stop)
        consume_web_data(max_messages=1)
        
        # Verify data in database
        session = test_db()
        records = session.query(StLouisCensusData).all()
        assert len(records) == 1
        assert records[0].raw_json["neighborhood"] == "Downtown"
        session.close()

@pytest.mark.integration
def test_consumer_with_real_kafka():
    """
    Integration test with real Kafka.
    
    Requires: Docker Compose with Kafka + PostgreSQL running
    """
    # This test actually connects to Kafka and PostgreSQL
    # Run with: pytest -m integration
    pass  # TODO: Implement after Docker setup confirmed