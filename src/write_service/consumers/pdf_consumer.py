"""Kafka consumer that reads processed PDF payloads and writes them to per-site Postgres tables.

Expected message payload (JSON) matches the build_payload output:
{
  "pdf_id": "stlouis_gov_crime:12345",   # or another string with site prefix
  "entities": { "dates": [...], "names": [...] },
  "snippet": "...",
  "tables": [...],
  "schema_version": 1
}

Behavior:
- Derive site/table name from pdf_id prefix (before first ':'), or fallback to 'site'
- Insert row into per-site table with entities JSON, snippet, pages text
- Compute search_vector in SQL (to leverage Postgres's to_tsvector)
- Retry a few times on transient DB errors; on repeated failure, write the payload to a dead-letter table.
"""

import os
import json
import logging
import time
from typing import Any, Dict, Optional

from kafka import KafkaConsumer, KafkaError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text as sa_text

from ..models import (
    engine,
    SessionLocal,
    get_or_create_table_class,
    ensure_dead_letter_table,
    ensure_tsvector_support,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pdf.processor.consumer")


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("PROCESSED_TOPIC", "processed-data-topic")
GROUP_ID = os.getenv("CONSUMER_GROUP", "pdf-processor-group")
MAX_RETRIES = int(os.getenv("MAX_INSERT_RETRIES", "3"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF_SECONDS", "2.0"))

# Helpers
def derive_site_from_pdf_id(pdf_id: str) -> str:
    """Try to derive a site name from pdf_id string. Use part before ':' if available."""
    if not pdf_id:
        return "site"
    if ":" in pdf_id:
        return pdf_id.split(":", 1)[0]
    if "/" in pdf_id:
        return pdf_id.split("/", 1)[0]
    return pdf_id[:50]


def insert_payload(session, table_cls, payload: Dict[str, Any]) -> Any:
    """Insert payload into the mapped table and compute search_vector server-side."""
    pdf_id = payload.get("pdf_id")
    snippet = payload.get("snippet")
    entities = payload.get("entities")
    pages = payload.get("pages", payload.get("full_text", None))

    # create instance
    record = table_cls(
        pdf_id=pdf_id,
        snippet=snippet,
        entities=entities,
        pages=pages
    )
    session.add(record)
    session.flush()

    # set search_vector using to_tsvector on snippet + pages
    table_name = table_cls.__table__.name
    stmt = sa_text(
        f"UPDATE {table_name} "
        f"SET search_vector = to_tsvector('english', coalesce(snippet,'') || ' ' || coalesce(pages,'')) "
        f"WHERE id = :id"
    )
    session.execute(stmt, {"id": record.id})
    return record


def write_dead_letter(session, payload: Dict[str, Any], error_msg: str, attempts: int = 1):
    """Insert into dead-letter table (jsonb payload)."""
    ensure_dead_letter_table()
    stmt = sa_text(
        "INSERT INTO pdf_dead_letter (pdf_id, payload, error_msg, attempts, last_error_at) "
        "VALUES (:pdf_id, :payload::jsonb, :error_msg, :attempts, now())"
    )
    session.execute(stmt, {
        "pdf_id": payload.get("pdf_id"),
        "payload": json.dumps(payload),
        "error_msg": str(error_msg),
        "attempts": attempts
    })


def handle_message(session, msg_value: bytes) -> Dict[str, Any]:
    """Parse, derive site, get/create table, and insert the payload with retries."""
    payload = json.loads(msg_value.decode("utf-8") if isinstance(msg_value, (bytes, bytearray)) else msg_value)
    pdf_id = payload.get("pdf_id", None)
    site = derive_site_from_pdf_id(pdf_id or "")
    table_cls = get_or_create_table_class(site)

    last_exc: Optional[Exception] = None
    attempts = 0
    while attempts < MAX_RETRIES:
        attempts += 1
        try:
            session.begin()
            rec = insert_payload(session, table_cls, payload)
            session.commit()
            logger.info(f"Inserted pdf_id={pdf_id} into table={table_cls.__table__.name}")
            return {"ok": True, "pdf_id": pdf_id, "table": table_cls.__table__.name, "pk": str(rec.id)}
        except SQLAlchemyError as e:
            session.rollback()
            last_exc = e
            logger.exception("DB insert failed, retrying...")
            time.sleep(RETRY_BACKOFF * attempts)
        except Exception as e:
            session.rollback()
            last_exc = e
            logger.exception("Unexpected error during insert, not retrying further")
            break

    # write to dead letter
    try:
        session.begin()
        write_dead_letter(session, payload, str(last_exc or "unknown"), attempts=attempts)
        session.commit()
    except Exception:
        session.rollback()
        logger.exception("Failed to write to dead-letter table (giving up)")

    return {"ok": False, "pdf_id": pdf_id, "error": str(last_exc), "attempts": attempts}


def run_consumer():
    """Main loop to run the Kafka consumer continuously."""
    ensure_tsvector_support()
    ensure_dead_letter_table()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v,
        consumer_timeout_ms=1000
    )

    logger.info(f"Listening to {KAFKA_TOPIC} on {KAFKA_BROKER}...")

    try:
        while True:
            for msg in consumer:
                logger.info(f"Got message partition={msg.partition} offset={msg.offset}")
                with SessionLocal() as session:
                    result = handle_message(session, msg.value)
                    logger.info(f"Processed result: {result}")
            time.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()