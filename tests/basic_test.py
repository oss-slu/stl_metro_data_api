"""
Basic connectivity test for Kafka and PostgreSQL.
Simple, minimal version without complex error handling.
Updated with URL encoding for PG password, retries/API version for Kafka, and Pytest-compliant assertions.
"""

import os
import sys
import time
import urllib.parse
import pytest

pytestmark = pytest.mark.integration

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB = os.getenv('PG_DB', 'stl_data')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', "123456") # update with pg password if needed

def test_kafka():
    """Test basic Kafka connectivity with retries and explicit API version."""
    print("Testing Kafka...")
    
    try:
        if sys.version_info >= (3, 12):
            import six
            sys.modules['kafka.vendor.six.moves'] = six.moves
        
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.errors import NoBrokersAvailable
        
        print("Waiting 10s for Kafka to be ready...")
        time.sleep(10)
        
        producer_connected = False
        for attempt in range(3):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=[KAFKA_BROKER],
                    api_version=(3, 0, 0), 
                    retries=3,
                    request_timeout_ms=10000,
                    reconnect_backoff_ms=1000
                )
                producer.send('test_topic', b'hello')
                producer.flush()
                producer.close()
                print("Kafka producer: OK")
                producer_connected = True
                break
            except NoBrokersAvailable:
                print(f"Kafka producer attempt {attempt+1} failed (NoBrokersAvailable), retrying in 5s...")
                time.sleep(5)
            except Exception as e:
                print(f"Kafka producer unexpected error: {e}")
                raise
        assert producer_connected, "All Kafka producer attempts failed"
        
        consumer_connected = False
        for attempt in range(3):
            try:
                consumer = KafkaConsumer(
                    'test_topic',
                    bootstrap_servers=[KAFKA_BROKER],
                    api_version=(3, 0, 0),
                    auto_offset_reset='latest',
                    consumer_timeout_ms=5000 
                )
                consumer.poll(timeout_ms=1000)
                consumer.close()
                print("Kafka consumer: OK")
                consumer_connected = True
                break
            except NoBrokersAvailable:
                print(f"Kafka consumer attempt {attempt+1} failed (NoBrokersAvailable), retrying in 5s...")
                time.sleep(5)
            except Exception as e:
                print(f"Kafka consumer unexpected error: {e}")
                raise
        assert consumer_connected, "All Kafka consumer attempts failed"
        
    except Exception as e:
        print(f"Kafka failed: {e}")
        print("Tip: Run 'docker-compose logs kafka' to check if broker started. On Windows, ensure WSL2 in Docker Desktop settings.")
        raise

def test_postgresql():
    """Test basic PostgreSQL connectivity with URL-encoded password."""
    print("Testing PostgreSQL...")
    
    try:
        from sqlalchemy import create_engine, text
        
        encoded_password = urllib.parse.quote_plus(PG_PASSWORD)
        engine_url = f"postgresql+psycopg2://{PG_USER}:{encoded_password}@{PG_HOST}:{PG_PORT}/{PG_DB}"
        
        engine = create_engine(engine_url)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        
        engine.dispose()
        print("PostgreSQL: OK")
        
    except Exception as e:
        print(f"PostgreSQL failed: {e}")
        if "password authentication failed" in str(e).lower():
            print("Tip: Check password in .env and Docker logs. Try 'docker-compose down -v && docker-compose up -d' to reset.")
        elif "could not translate host name" in str(e).lower():
            print("Tip: This is usually due to special chars in password; encoding should fix it. Verify PG is running: docker ps")
        raise

def main():
    """Run connectivity tests."""
    print("Basic Connectivity Test")
    print("=" * 30)
    
    kafka_ok = False
    try:
        test_kafka()
        kafka_ok = True
    except AssertionError as e:
        print(f"Kafka test failed: {e}")
    except Exception as e:
        print(f"Kafka test failed with unexpected error: {e}")
    
    postgres_ok = False
    try:
        test_postgresql()
        postgres_ok = True
    except Exception as e:
        print(f"PostgreSQL test failed: {e}")
    
    print("\nResults:")
    print(f"Kafka: {'PASS' if kafka_ok else 'FAIL'}")
    print(f"PostgreSQL: {'PASS' if postgres_ok else 'FAIL'}")
    
    if kafka_ok and postgres_ok:
        print("\nAll tests passed!")
        return 0
    else:
        print("\nSome tests failed. Check tips above and Docker logs.")
        return 1

if __name__ == "__main__":
    sys.exit(main())