"""
Tests for the write service JSON Kafka processor Python app.
"""

# Get the Flask app
from src.write_service.processing.json_processor import send_data

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
    
def test_send_to_kafka_valid():
    """This test checks if a valid list of dictionary passes the schema, if Kafka was able to be connected to, and if the data was able to be sent to Kafka."""
    valid_records = [{"color": "blue"}, {"color": "red"}]

    kafka_status = send_data(valid_records)

    # See if matches expected response from JSON processor
    assert kafka_status.startswith("Sent data to Kafka successfully!") == True

def test_send_to_kafka_invalid():
    """This test checks if improperly formatted data is able to be cleaned, pass the schema, and be sent to Kafka."""
    invalid_records = [1, 2, 3]

    kafka_status = send_data(invalid_records)

    # See if matches expected response from JSON processor
    assert kafka_status.startswith("Sent data to Kafka successfully!") == True