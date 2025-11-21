import json
import os
import subprocess
import time
from pathlib import Path

import pytest
from kafka import KafkaProducer
import psycopg2


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
    env["PG_DSN"] = "postgresql+psycopg2://postgres:postgres@localhost:5432/stl_data"
    env["SITE_NAME"] = ""  # force routing via record _table

    # Start consumer
    proc = subprocess.Popen(CONSUMER_ENTRY, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    # give it a moment to subscribe
    time.sleep(3)

    # Produce two records (2nd has extra column → schema evolution)
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda d: json.dumps(d).encode("utf-8"),
    )

    rec1 = {
        "_table": "it_stl_crime",
        "incident_number": "25-IT-0001",
        "offense": "THEFT",
        "ward": "5",
        "occurred_at": "2025-03-01T12:00:00"
    }
    rec2 = {
        "_table": "it_stl_crime",
        "incident_number": "25-IT-0002",
        "offense": "ASSAULT",
        "ward": 6,
        "occurred_at": "2025-03-02 08:30:00",
        "new_col": "appeared later"
    }

    producer.send("processed.excel.data", rec1)
    producer.send("processed.excel.data", rec2)
    producer.flush()

    # wait for consumer to process
    time.sleep(12)

    # Connect to PG and verify
    conn = psycopg2.connect("dbname=stl_data user=postgres password=postgres host=localhost port=5432")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute('SELECT incident_number, offense, ward FROM "it_stl_crime" ORDER BY id')
    rows = cur.fetchall()
    assert len(rows) >= 2
    # verify schema evolution added new_col
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='it_stl_crime'")
    cols = {r[0] for r in cur.fetchall()}
    assert "new_col" in cols

    # teardown
    cur.close()
    conn.close()
    # Let the consumer keep running (or kill it)
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
