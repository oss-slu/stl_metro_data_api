"""
excel_consumer.py
-------------------------
Kafka → Postgres bridge for ANY processed Excel dataset.

How it routes records to tables:
- Preferred: each record includes "_table" (or "_site") meta key
  e.g., {"_table":"stlouis_gov_crime", "incident_number": "...", ...}
- Fallback: use SITE_NAME env (e.g., SITE_NAME=stlouis_gov_crime)
- Optional: derive from topic via TOPIC_TABLE_MAP (JSON env), e.g.
  TOPIC_TABLE_MAP='{"processed.excel.data":"stlouis_gov_crime"}'

Schema handling:
- Infers column types from the data (int, float, bool, datetime, str).
- Creates table if it doesn't exist.
- If new keys appear later, auto-ALTER TABLE ADD COLUMN.
- Stores a simple in-memory cache of reflected Table objects.

DLQ:
- Bad JSON, missing routing info, or DB failures → DLQ topic.
"""

import os, json, time, logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, Float, String, Boolean, DateTime, text
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import insert
from sqlalchemy.engine import Engine

from kafka import KafkaConsumer, KafkaProducer

# ------------------------------
# ENV / CONFIG
# ------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_GROUP_ID  = os.getenv("KAFKA_GROUP_ID", "excel-consumer-group")
TOPIC           = os.getenv("PROCESSED_EXCEL_TOPIC", "processed.excel.data")
DLQ_TOPIC       = os.getenv("DLQ_TOPIC", "processed.excel.data.dlq")

# Optional: default table/site if messages don't carry routing metadata
DEFAULT_TABLE   = os.getenv("SITE_NAME")  # e.g., "stlouis_gov_crime"

# Optional: static mapping of topic -> table (JSON string)
# Example: TOPIC_TABLE_MAP='{"processed.excel.data":"stlouis_gov_crime"}'
TOPIC_TABLE_MAP = json.loads(os.getenv("TOPIC_TABLE_MAP", "{}") or "{}")

# DB
PG_DSN = os.getenv("PG_DSN", "postgresql+psycopg2://postgres:postgres@localhost:5432/stl_data")

# Retry
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("generic_excel_consumer")

# ------------------------------
# DB bootstrap (Core, not ORM)
# ------------------------------
engine: Engine = create_engine(PG_DSN, pool_pre_ping=True, future=True)
metadata = MetaData()
# Cache of already-reflected/created tables: {table_name: Table}
_table_cache: Dict[str, Table] = {}

# ------------------------------
# Type inference helpers
# ------------------------------
def _try_datetime(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, (int, float)):
        # don’t guess numeric → datetime (too risky)
        return None
    s = str(v).strip()
    # Try ISO first
    try:
        return datetime.fromisoformat(s)
    except Exception:
        pass
    # Try a couple common formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None

def infer_sqla_type(value: Any):
    """
    Infer a SQLAlchemy column type from a Python value.
    Keep it conservative; fallback to String for weird stuff.
    """
    if value is None:
        return String
    if isinstance(value, bool):
        return Boolean
    if isinstance(value, int):
        return Integer
    if isinstance(value, float):
        return Float
    # Try to interpret strings as numbers/datetimes/bools
    s = str(value).strip()
    # bool-ish
    if s.lower() in {"true","false"}:
        return Boolean
    # int-ish
    try:
        int(s)
        return Integer
    except Exception:
        pass
    # float-ish
    try:
        float(s)
        return Float
    except Exception:
        pass
    # datetime-ish
    if _try_datetime(s) is not None:
        return DateTime
    # default
    return String

def coerce_value(value: Any, coltype):
    """
    Coerce a value to match the target column type for safer inserts.
    """
    if value is None:
        return None
    if coltype is Boolean:
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        return s in {"true","1","t","y","yes"}
    if coltype is Integer:
        if isinstance(value, int): return value
        return int(str(value).strip())
    if coltype is Float:
        if isinstance(value, float): return value
        return float(str(value).strip())
    if coltype is DateTime:
        dt = _try_datetime(value)
        return dt
    # String fallback
    return str(value)

# ------------------------------
# Table management
# ------------------------------
META_KEYS = {"_site", "_table", "_schema", "_version"}  # reserved keys we never store as columns

def _resolve_table_name(topic: str, sample: Dict[str, Any]) -> Optional[str]:
    """
    Decide which table to write to.
    Priority:
      1) record['_table']
      2) record['_site']
      3) TOPIC_TABLE_MAP[topic]
      4) DEFAULT_TABLE
    """
    if "_table" in sample and sample["_table"]:
        return str(sample["_table"])
    if "_site" in sample and sample["_site"]:
        return str(sample["_site"])
    if topic in TOPIC_TABLE_MAP:
        return TOPIC_TABLE_MAP[topic]
    return DEFAULT_TABLE

def _reflect_or_create_table(table_name: str, sample_record: Dict[str, Any]) -> Table:
    """
    Ensure a Table exists for table_name with columns matching sample_record's keys.
    - If table is new: create with inferred columns.
    - If table exists: add any missing columns (ALTER TABLE).
    Caches the resulting Table for fast reuse.
    """
    # Return from cache if available
    tbl = _table_cache.get(table_name)
    if tbl is not None:
        # Also ensure any new columns are added if schema evolved
        _add_missing_columns(tbl, sample_record)
        return tbl

    # Reflect existing table if present
    metadata.reflect(bind=engine, only=[table_name])
    if table_name in metadata.tables:
        tbl = metadata.tables[table_name]
        _table_cache[table_name] = tbl
        _add_missing_columns(tbl, sample_record)
        return tbl

    # Build fresh set of Columns (always add an auto id + created_at)
    cols = [
        Column("id", Integer, primary_key=True),
        Column("created_at", DateTime, server_default=text("NOW()")),
    ]
    for key, val in sample_record.items():
        if key in META_KEYS:
            continue
        # infer type for each column from the sample
        coltype = infer_sqla_type(val)
        cols.append(Column(key, coltype))

    tbl = Table(table_name, metadata, *cols)
    metadata.create_all(bind=engine, tables=[tbl])
    _table_cache[table_name] = tbl
    log.info("Created table %s with %d columns", table_name, len(cols))
    return tbl

def _add_missing_columns(tbl: Table, record: Dict[str, Any]) -> None:
    """
    If new keys appear in future messages, add columns to the existing table.
    """
    existing = set(tbl.columns.keys())
    to_add = []
    for key, val in record.items():
        if key in META_KEYS:
            continue
        if key not in existing:
            to_add.append((key, infer_sqla_type(val)))
    if not to_add:
        return
    # ALTER TABLE for each new column
    with engine.begin() as conn:
        for name, typ in to_add:
            ddl = f'ALTER TABLE "{tbl.name}" ADD COLUMN "{name}" {typ().__visit_name__.upper()}'
            conn.exec_driver_sql(ddl)
            log.info("Added column %s.%s (%s)", tbl.name, name, typ.__name__)
    # Re-reflect updated table so SQLAlchemy sees the new columns
    metadata.remove(tbl)
    metadata.reflect(bind=engine, only=[tbl.name])
    _table_cache[tbl.name] = metadata.tables[tbl.name]

# ------------------------------
# Insert logic
# ------------------------------
def _insert_rows(table: Table, rows: List[Dict[str, Any]]) -> int:
    """
    Insert many rows. Coerces values to column types for safety.
    """
    if not rows:
        return 0

    # Build a normalized row list with coerced values per column
    prepared = []
    col_types = {c.name: c.type.__class__ for c in table.columns if c.name not in ("id", "created_at")}
    for rec in rows:
        clean = {}
        for k, v in rec.items():
            if k in META_KEYS:
                continue
            if k not in col_types:
                # Column might have been added mid-batch; refresh table and retry
                _add_missing_columns(table, rec)
                col_types = {c.name: c.type.__class__ for c in table.columns if c.name not in ("id", "created_at")}
            col_type = col_types.get(k, String)  # unknown → String
            try:
                clean[k] = coerce_value(v, col_type)
            except Exception:
                # If coercion fails, just stringify so we don't drop data
                clean[k] = str(v)
        prepared.append(clean)

    with engine.begin() as conn:
        conn.execute(table.insert(), prepared)
    return len(prepared)

# ------------------------------
# Kafka loop
# ------------------------------
def run():
    log.info("Starting generic consumer | topic=%s | group=%s", TOPIC, KAFKA_GROUP_ID)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: m.decode("utf-8"),
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda d: json.dumps(d).encode("utf-8"),
    )

    for msg in consumer:
        raw = msg.value

        # 1) Decode JSON
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Bad JSON → DLQ")
            producer.send(DLQ_TOPIC, {"error": "json_decode_error", "payload": raw})
            continue

        # 2) Normalize to list
        records = data if isinstance(data, list) else [data]
        if not records:
            continue

        # 3) Decide which table this batch belongs to
        table_name = _resolve_table_name(msg.topic, records[0])
        if not table_name:
            log.warning("No table routing info for record → DLQ")
            producer.send(DLQ_TOPIC, {"error": "routing_missing", "sample": records[0]})
            continue

        # 4) Ensure table exists and has needed columns
        try:
            table = _reflect_or_create_table(table_name, records[0])
        except SQLAlchemyError as e:
            log.error("Table init error → DLQ: %s", e)
            producer.send(DLQ_TOPIC, {"error": "table_init", "table": table_name, "sample": records[0]})
            continue

        # 5) Insert with retries
        attempt = 0
        while True:
            try:
                n = _insert_rows(table, records)
                log.info("Inserted %d rows into %s", n, table_name)
                break
            except SQLAlchemyError as e:
                attempt += 1
                if attempt >= MAX_RETRIES:
                    log.error("DB failure after %d attempts → DLQ", attempt)
                    producer.send(DLQ_TOPIC, {"error": "db_error", "table": table_name, "count": len(records)})
                    break
                backoff = min(2 ** attempt, 10)
                log.warning("DB error (%s). Retrying in %ss...", e, backoff)
                time.sleep(backoff)

if __name__ == "__main__":
    run()
