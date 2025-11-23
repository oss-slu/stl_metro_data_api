import json
import os
import subprocess
import time
from pathlib import Path

import pytest
from kafka import KafkaProducer
import psycopg2
import urllib


CONSUMER_ENTRY = ["python", "-m", "write_service.consumers.excel_consumer"]


@pytest.mark.integration
def test_end_to_end_kafka_to_postgres(tmp_path):
    """
    Requires: docker compose up -d using docker-compose.test.yml
    Steps:
      1) Start consumer as a subprocess (points at dockerized PG + Kafka)
      2) Produce messages to Kafka
      3) Assert rows landed in Postgres table
    """
    # Env for consumer to talk to dockerized services
    env = os.environ.copy()
    env["KAFKA_BOOTSTRAP"] = "localhost:9092"
    env["KAFKA_GROUP_ID"] = "it-consumer-group"
    env["PROCESSED_EXCEL_TOPIC"] = "processed.excel.data"
    env["DLQ_TOPIC"] = "processed.excel.data.dlq"

    # Postgres connection settings
    PG_HOST = os.getenv("PG_HOST", "localhost")
    PG_PORT = os.getenv("PG_PORT", "5433")  # default matches your previous DSN
    PG_DB = os.getenv("PG_DB", "stl_data")
    PG_USER = os.getenv("PG_USER", "postgres")
    PG_PASSWORD = os.getenv("PG_PASSWORD", "Welcome@123456")

    # URL-encoded password for SQLAlchemy-style DSN (env["PG_DSN"])
    encoded_password = urllib.parse.quote_plus(PG_PASSWORD)

    env["PG_DSN"] = (
        f"postgresql+psycopg2://{PG_USER}:{encoded_password}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    env["SITE_NAME"] = ""  # force routing via record _table

    # Start consumer
    proc = subprocess.Popen(
        CONSUMER_ENTRY,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        # give it a moment to subscribe
        time.sleep(3)

        # Produce two records (2nd has extra column → schema evolution)
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda d: json.dumps(d).encode("utf-8"),
        )

        rec1 = {
            "_table": "stlouis_gov_crime",
            "incident_number": "25-IT-0001",
            "offense": "THEFT",
            "ward": "5",
            "occurred_at": "2025-03-01T12:00:00",
        }
        rec2 = {
            "_table": "stlouis_gov_crime",
            "incident_number": "25-IT-0002",
            "offense": "ASSAULT",
            "ward": 6,
            "occurred_at": "2025-03-02 08:30:00",
            "new_col": "appeared later",
        }

        producer.send("processed.excel.data", rec1)
        producer.send("processed.excel.data", rec2)
        producer.flush()

        # wait for consumer to process
        time.sleep(12)

        # psycopg2 does NOT need URL-encoded password; use the raw one.
        conn = psycopg2.connect(
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
        )
        conn.autocommit = True

        cur = conn.cursor()
        cur.execute('SELECT incident_number, offense, ward FROM "stlouis_gov_crime" ORDER BY id')
        rows = cur.fetchall()
        assert len(rows) >= 2

        # verify schema evolution added new_col
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='stlouis_gov_crime'"
        )
        cols = {r[0] for r in cur.fetchall()}
        assert "new_col" in cols

    finally:
        # teardown
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

        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
