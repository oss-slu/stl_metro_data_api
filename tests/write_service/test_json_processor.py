"""
Tests for the write service JSON Kafka processor Python app.
"""

from unittest.mock import patch, MagicMock

# Get the processor send_data function
from src.write_service.processing.json_processor import send_data

# Load environment variables (optional)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@patch('src.write_service.processing.json_processor.KafkaProducer')
def test_send_to_kafka_valid(mock_producer_class):
    """This test checks if a valid list of dictionary passes the schema and is 'sent' to Kafka (mocked)."""
    # Arrange: mock producer instance methods
    mock_producer = MagicMock()
    mock_producer.send.return_value = None
    mock_producer.flush.return_value = None
    mock_producer_class.return_value = mock_producer

    valid_records = [{"color": "blue"}, {"color": "red"}]

    # Act
    kafka_status = send_data(valid_records)

    # Assert: the function should report success when producer works
    assert kafka_status.startswith("Sent data to Kafka successfully!")


@patch('src.write_service.processing.json_processor.KafkaProducer')
def test_send_to_kafka_invalid(mock_producer_class):
    """This test checks if improperly formatted data is cleaned and still 'sent' to Kafka (mocked)."""
    # Arrange: mock producer instance methods
    mock_producer = MagicMock()
    mock_producer.send.return_value = None
    mock_producer.flush.return_value = None
    mock_producer_class.return_value = mock_producer

    invalid_records = [1, 2, 3]

    # Act
    kafka_status = send_data(invalid_records)

    # Assert: cleaned non-dict items should be wrapped and then "sent"
    assert kafka_status.startswith("Sent data to Kafka successfully!")