import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from kafka import KafkaProducer
import psycopg2
import urllib

# Use the same Python interpreter that's running the test (from venv)
CONSUMER_ENTRY = [sys.executable, "-m", "write_service.consumers.excel_consumer"]

@pytest.mark.integration
def test_end_to_end_kafka_to_postgres(tmp_path):
    """
    End-to-end smoke test:

    - Kafka message on processed.excel.data
    - Generic excel_consumer reads it
    - Row appears in Postgres table "stlouis_gov_crime"

    NOTE:
    - Detailed schema evolution (adding new columns) is covered
      by the unit test `test_schema_evolution_adds_column`.
    - This integration test just proves the overall pipeline works.
    """
    # ---------- ENV for consumer subprocess ----------
    env = os.environ.copy()

    # Kafka: talk to test broker on localhost (Docker-mapped)
    env["KAFKA_BOOTSTRAP"] = "127.0.0.1:9092"  # IPv4 explicitly on Windows

    # Unique consumer group per run so we always read from the beginning
    import uuid
    unique_group_id = f"it-consumer-group-{uuid.uuid4().hex[:8]}"
    env["KAFKA_GROUP_ID"] = unique_group_id

    env["PROCESSED_EXCEL_TOPIC"] = "processed.excel.data"
    env["DLQ_TOPIC"] = "processed.excel.data.dlq"

    # ---------- Postgres connection (matches docker-compose.test.yml) ----------
    KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
    PG_HOST = os.getenv('PG_HOST', 'localhost')
    PG_PORT = os.getenv('PG_PORT', '5432')
    PG_DB = os.getenv('PG_DB', 'stl_data')
    PG_USER = os.getenv('PG_USER', 'postgres')
    PG_PASSWORD = os.getenv('PG_PASSWORD', "123456")  # your local test password

    # URL-encoded password for SQLAlchemy DSN used inside the consumer
    encoded_password = urllib.parse.quote_plus(PG_PASSWORD)
    env["PG_DSN"] = (
        f"postgresql+psycopg2://{PG_USER}:{encoded_password}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )

    # Force routing by record["_table"], not SITE_NAME
    env["SITE_NAME"] = ""

    # Make sure the subprocess can import src/write_service
    project_root = Path(__file__).parent.parent.parent
    env["PYTHONPATH"] = str(project_root / "src")

    # ---------- Clean up table from previous runs ----------
    try:
        cleanup_conn = psycopg2.connect(
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
        )
        cleanup_conn.autocommit = True
        cleanup_cur = cleanup_conn.cursor()
        cleanup_cur.execute('DROP TABLE IF EXISTS "stlouis_gov_crime"')
        cleanup_cur.close()
        cleanup_conn.close()
    except Exception as e:
        print(f"Warning: Could not clean up test table: {e}")

    # ---------- Send ONE test message to Kafka ----------
    print("\n=== Sending test message to Kafka ===")
    producer = KafkaProducer(
        bootstrap_servers="127.0.0.1:9092",
        value_serializer=lambda d: json.dumps(d).encode("utf-8"),
    )

    rec1 = {
        "_table": "stlouis_gov_crime",
        "incident_number": "25-IT-0001",
        "offense": "THEFT",
        "ward": 5,
        "occurred_at": "2025-03-01T12:00:00",
    }

    producer.send("processed.excel.data", rec1)
    producer.flush()
    producer.close()
    print("Message sent!")

    # Let Kafka fully commit the message
    time.sleep(2)

    # ---------- Start consumer subprocess ----------
    proc = subprocess.Popen(
        CONSUMER_ENTRY,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        # Give consumer time to subscribe and process the message
        time.sleep(15)

        # If the consumer crashed, surface that
        proc.poll()
        if proc.returncode is not None:
            output = proc.stdout.read()
            raise RuntimeError(
                f"Consumer exited unexpectedly with code {proc.returncode}.\nOutput:\n{output}"
            )

        # ---------- Check Postgres for inserted row ----------
        conn = psycopg2.connect(
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
        )
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(
            'SELECT incident_number, offense, ward FROM "stlouis_gov_crime" ORDER BY id'
        )
        rows = cur.fetchall()

        print("\n=== Database Contents ===")
        print(f"Found {len(rows)} rows:")
        for row in rows:
            print("  ", row)

        # We just need to see that the pipeline worked end-to-end
        assert len(rows) >= 1, f"Expected at least 1 row, got {len(rows)}"

        inc, off, ward = rows[0]
        assert inc == "25-IT-0001"
        assert off == "THEFT"
        # depending on type, ward may be int or string; normalize to int
        assert int(ward) == 5

    finally:
        # ---------- Teardown & log consumer output ----------
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

        if proc.stdout:
            print("\n=== Full Consumer Output ===")
            print(proc.stdout.read())
            print("=== End Consumer Output ===\n")

        try:
            if "cur" in locals() and not cur.closed:
                cur.close()
        except Exception:
            pass

        try:
            if "conn" in locals() and conn:
                conn.close()
        except Exception:
            pass
