from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json, time
from jsonschema import validate, ValidationError
import logging

# Ensure it is a list of dictionaries (array=list, object=dictionary)
schema = {
    "type": "array",
    "items": {
        "type": "object"
    }
}

def clean_data(raw_data):
    """
    This function ensures that the data to be sent is in a standardized format:
    a list of dictionaries, where each dictionary is an entity.
    """

    result = []

    # Convert into list of Python dictionaries
    if isinstance(raw_data, dict):
        # One entity = wrap dictionary in list
        result = [raw_data]
    elif isinstance(raw_data, list):
        # Otherwise if list of entities, make sure they are all dictionaries
        for item in raw_data:
            if isinstance(item, dict):
                result.append(item)
            else:
                # Wrap non-dictionaries into a dictionary
                result.append({"data": item})
    else:
        # Wrap single data value in a list of a dictionary
        result = [{"data": raw_data}]
    
    return result

def send_data(raw_data):
    """This function checks if the data passes the schema and then sends the data to Kafka."""

    # Clean the data first
    data = clean_data(raw_data)

    # No data received
    if not data:
        logging.error("No data to send!")
        return "No data to send to Kafka!"

    # Make sure data in right format (schema)
    try:
        validate(instance=data, schema=schema)
    except ValidationError as error:
        logging.error("Failed to send to Kafka. Data is not in valid format!\n" + str(error.message))
        return "Failed to send to Kafka. Data is not in valid format! <br> Error: <br>" + str(error.message)

    # Send to Kafka (we will try 3 times just in case Kafka isn't available yet)
    for attempt in range(3):
        # Connect to Kafka server, make sure the data is in bytes, and add timeouts
        # Localhost:9092 if ran locally, kafka:29092 for running on Docker
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092', 'kafka:29092'],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=3,
                request_timeout_ms=10000,
                reconnect_backoff_ms=1000
            )
            producer.send("JSON-data", data)
            producer.flush()
            logging.info("Sent JSON data to Kafka: " + str(data))
            return "Sent data to Kafka successfully!<br>" + "Topic: JSON data<br>" + "Data:<br>" + str(data)
        except NoBrokersAvailable:
            # Kafka may not be available yet, let's try again
            logging.error(f"Kafka producer attempt {attempt+1} failed (NoBrokersAvailable), retrying in 5s...")
            time.sleep(5)

        except Exception as error:
            # Something else went wrong when sending to Kafka!
            logging.error("Failed to send to Kafka!\n", str(error))
            return "Failed to send data to Kafka! <br> Error: " + str(error)
    
        logging.error("Failed to connect to Kafka after 3 attempts. Ensure that Kafka is running and accessible! Or maybe Kafka hasn't finished loading yet. Wait like 60 seconds and then refresh the page.")
        return "Failed to connect to Kafka after 3 attempts. Ensure that Kafka is running and accessible! Or maybe Kafka hasn't finished loading yet. Wait like 60 seconds and then refresh the page."
