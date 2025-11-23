# tests/write_service/test_web_consumer.py
"""
Tests for web consumer.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import cast, create_engine
from sqlalchemy.orm import sessionmaker
import sys
import pathlib

# ADD SRC TO PATH FIRST (before any src imports!)
ROOT_SRC = pathlib.Path(__file__).resolve().parents[2] / "src"
if str(ROOT_SRC) not in sys.path:
    sys.path.insert(0, str(ROOT_SRC))

# NOW import from src (after path is set up)
from write_service.consumers.models import Base, StLouisCensusData
from write_service.consumers.web_consumer import consume_web_data, retry_database_operation


@pytest.fixture
def test_db():
    """Create in-memory test database."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    yield Session
    Base.metadata.drop_all(engine)


def test_consumer_inserts_data(test_db):
    """Test that consumer inserts Kafka messages into database."""
    mock_message = Mock()
    mock_message.value = {"neighborhood": "Downtown", "population": 5442}
    
    with patch("write_service.consumers.web_consumer.KafkaConsumer") as MockConsumer, \
        patch("write_service.consumers.web_consumer.get_db_engine") as mock_engine, \
        patch("write_service.consumers.web_consumer.create_tables"):
        
        # Setup mocks
        mock_consumer_instance = MockConsumer.return_value
        mock_consumer_instance.__iter__.return_value = [mock_message]
        
        # Use test database
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        mock_engine.return_value = engine

        # Run consumer
        consume_web_data(max_messages=1)
        
        # Verify data in database
        Session = sessionmaker(bind=engine)
        session = Session()
        records = session.query(StLouisCensusData).all()
        assert len(records) == 1
        assert records[0].raw_json["neighborhood"] == "Downtown"
        session.close()


def test_consumer_handles_database_error():
    """Test that consumer handles database errors gracefully."""
    mock_message = Mock()
    mock_message.value = {"test": "data"}
    
    with patch("write_service.consumers.web_consumer.KafkaConsumer") as MockConsumer, \
         patch("write_service.consumers.web_consumer.get_db_engine"), \
         patch("write_service.consumers.web_consumer.create_tables"), \
         patch("write_service.consumers.web_consumer.sessionmaker") as MockSession:
        
        mock_consumer_instance = MockConsumer.return_value
        mock_consumer_instance.__iter__.return_value = [mock_message]
        
        # Simulate database error
        mock_session_class = MockSession.return_value
        mock_session_instance = mock_session_class.return_value
        mock_session_instance.add.side_effect = Exception("Database connection failed")
        
        # Should not raise exception
        consume_web_data(max_messages=1)
        
        # Verify rollback was called
        mock_session_instance.rollback.assert_called()


def test_retry_logic():
    """Test retry mechanism for database operations."""
    attempt_count = [0]
    
    def failing_function():
        attempt_count[0] += 1
        if attempt_count[0] < 3:
            raise Exception("Temporary failure")
        return "success"
    
    from sqlalchemy.exc import SQLAlchemyError
    
    def wrapped_function():
        try:
            return failing_function()
        except Exception as e:
            raise SQLAlchemyError(str(e))
    
    result = retry_database_operation(wrapped_function, max_retries=3, delay=0.1)
    assert result == "success"
    assert attempt_count[0] == 3


@pytest.mark.integration
def test_consumer_with_real_kafka():
    """Integration test with real Kafka."""
    from kafka import KafkaProducer
    import json
    import time
    
    # Get count BEFORE sending
    from write_service.consumers.models import get_db_engine
    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    initial_count = session.query(StLouisCensusData).count()
    session.close()
    
    # Send test message
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    
    test_data = {"neighborhood": "Test Integration", "population": 9999}
    producer.send("processed.web.data", test_data)
    producer.flush()
    producer.close()
    
    # Consume it
    time.sleep(2)
    consume_web_data(topic="processed.web.data", max_messages=1)
    
    # Check count increased
    session = Session()
    final_count = session.query(StLouisCensusData).count()
    session.close()
    
    # Verify one more record was added
    assert final_count == initial_count + 1, f"Expected {initial_count + 1} records, got {final_count}"