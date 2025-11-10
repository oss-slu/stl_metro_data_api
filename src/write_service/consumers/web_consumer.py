# src/write_service/consumers/web_consumer.py
"""
Kafka consumer for web-scraped data.

Reads from 'processed.web.data' topic and stores in PostgreSQL.
"""

from kafka import KafkaConsumer
import json
import logging
from sqlalchemy.orm import sessionmaker
from .models import StLouisCensusData, get_db_engine, create_tables

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def consume_web_data(topic="processed.web.data", max_messages=None):
    """Consume messages from Kafka and store in PostgreSQL"""
    # 1. Setup database
    engine = get_db_engine()
    create_tables(engine)
    Session = sessionmaker(bind=engine)
    #2. Setup Kafka consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="eaerliest", # read from beginning
        group_id="web-consumer-group"
    )
    logger.info(f"Started consuming from topic: {topic}")

    #3. Consume messages
    message_count = 0
    try:
        for message in consumer:
            data = message.value
            logger.info(f"Received message: {data}")
            #4. Store in database
            session = Session()
            try:
                record = StLouisCensusData(raw_json=data)
                session.add(record)
                session.commit()
                logger.info(f"Inserted record into database: {data}")

                message_count += 1
                if max_messages and message_count >= max_messages:
                    logger.info(f"reached max_messages({max_messages}), stopping")
                    break
            except Exception as e:
                session.rollback()
                logger.error(f"Database error: {e}")
                # TODO: Send to dead=letter queue
            finally:
                session.close()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        consumer.close()

if __name__ == "__main__":
    # Run consumer
    consume_web_data()