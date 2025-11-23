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
from sqlalchemy.exc import SQLAlchemyError, StatementError
from sqlalchemy import insert
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from kafka import KafkaConsumer, KafkaProducer
import urllib

# ------------------------------
# ENV / CONFIG
# ------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "excel-consumer-group")
TOPIC = os.getenv("PROCESSED_EXCEL_TOPIC", "processed.excel.data")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "processed.excel.data.dlq")

PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB = os.getenv('PG_DB', 'stl_data')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', "Welcome@123456")

# Optional: default table/site if messages don't carry routing metadata
DEFAULT_TABLE = os.getenv("SITE_NAME")  # e.g., "stlouis_gov_crime"

# Optional: static mapping of topic -> table (JSON string)
# Example: TOPIC_TABLE_MAP='{"processed.excel.data":"stlouis_gov_crime"}'
TOPIC_TABLE_MAP = json.loads(os.getenv("TOPIC_TABLE_MAP", "{}") or "{}")

# DB
encoded_password = urllib.parse.quote_plus(PG_PASSWORD)
PG_DSN = os.getenv("PG_DSN", "postgresql+psycopg2://{PG_USER}:{encoded_password}@{PG_HOST}:{PG_PORT}/{PG_DB}")

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

def _try_datetime(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, (int, float)):
        return None
    s = str(v).strip()
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

    For SQLite (used in unit tests), we avoid Boolean/DateTime column types
    and instead use INTEGER/FLOAT/TEXT so inserts are stable. For Postgres,
    we keep richer types.
    """
    
    if engine.dialect.name == "sqlite":
        if value is None:
            return String

        # raw ints stay INT
        if isinstance(value, int):
            return Integer

        # raw floats stay FLOAT
        if isinstance(value, float):
            return Float

        s = str(value).strip()

        # bool-ish strings stored as INTEGER (0/1)
        if s.lower() in {"true", "false", "t", "f", "yes", "no", "y", "n", "1", "0"}:
            return Integer

        # int-ish strings → INTEGER
        try:
            int(s)
            return Integer
        except Exception:
            pass

        # float-ish strings → FLOAT
        try:
            float(s)
            return Float
        except Exception:
            pass

        # everything else → TEXT
        return String

    # --- Default (Postgres / others) behavior ---
    if value is None:
        return String
    if isinstance(value, bool):
        return Boolean
    if isinstance(value, int):
        return Integer
    if isinstance(value, float):
        return Float

    s = str(value).strip()

    # bool-ish
    if s.lower() in {"true", "false"}:
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

    # fallback
    return String

def coerce_value(value: Any, coltype):
    """
    Coerce a value to match the target SQLAlchemy column type for safer inserts.
    Works with SQLAlchemy's concrete type classes (e.g. INTEGER, FLOAT)
    by using issubclass instead of direct identity checks.
    """
    if value is None:
        return None

    # Normalize coltype into a SQLAlchemy type *class*
    if isinstance(coltype, type):
        t = coltype
    else:
        t = coltype.__class__

    # ----- BOOLEAN / BOOL-ISH AS INTEGER -----
    # In our SQLite-friendly setup, bool-like columns are usually INTEGER.
    # But if we ever actually see a Boolean subclass, handle it here.
    if issubclass(t, Boolean):
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        return s in {"true", "1", "t", "y", "yes"}

    # ----- INTEGER (including bool-like strings → 0/1) -----
    if issubclass(t, Integer):
        # Real bools → 0/1
        if isinstance(value, bool):
            return 1 if value else 0

        s = str(value).strip()
        lower = s.lower()

        # Map common truthy/falsy strings to 1/0
        if lower in {"true", "t", "yes", "y"}:
            return 1
        if lower in {"false", "f", "no", "n"}:
            return 0

        # Normal integer-ish strings
        return int(s)

    # ----- FLOAT -----
    if issubclass(t, Float):
        if isinstance(value, float):
            return value
        return float(str(value).strip())

    # ----- DATETIME -----
    if issubclass(t, DateTime):
        dt = _try_datetime(value)
        return dt

    # ----- STRING / FALLBACK -----
    return str(value)


META_KEYS = {"_site", "_table", "_schema", "_version"} 

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
    # 1) Fast path: table is already in our cache
    tbl = _table_cache.get(table_name)
    if tbl is not None:
        # Make sure schema is up to date (in case new keys appeared)
        _add_missing_columns(tbl, sample_record)
        # _add_missing_columns may refresh metadata + cache, so always return fresh from cache
        return _table_cache.get(table_name, tbl)

    # 2) Check DB for an existing table *without* blowing up if it doesn't exist
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    if table_name in existing_tables:
        # Reflect just this table into metadata
        metadata.clear()
        metadata.reflect(bind=engine, only=[table_name])
        tbl = metadata.tables[table_name]
        _table_cache[table_name] = tbl

        # Ensure schema evolution (may ALTER TABLE + refresh cache)
        _add_missing_columns(tbl, sample_record)
        return _table_cache.get(table_name, tbl)

    # 3) Table does not exist yet → create it with inferred columns
    cols = [
        Column("id", Integer, primary_key=True),
        Column("created_at", DateTime, server_default=text("CURRENT_TIMESTAMP")),
    ]
    for key, val in sample_record.items():
        if key in META_KEYS:
            continue
        coltype = infer_sqla_type(val)
        cols.append(Column(key, coltype))

    tbl = Table(table_name, metadata, *cols)
    metadata.create_all(bind=engine, tables=[tbl])

    # Reflect back in to get a "canonical" Table object and cache it
    metadata.clear()
    metadata.reflect(bind=engine, only=[table_name])
    tbl = metadata.tables[table_name]
    _table_cache[table_name] = tbl
    log.info("Created table %s with %d columns", table_name, len(cols))
    return tbl

def _add_missing_columns(tbl: Table, record: Dict[str, Any]) -> None:
    """
    Add any new columns introduced by schema evolution.
    Handles SQLite's strict behavior around duplicate ALTERs.
    """
    # Always pull fresh inspector to avoid stale metadata
    inspector = inspect(engine)
    existing_cols = {col["name"] for col in inspector.get_columns(tbl.name)}

    to_add = []
    for key, val in record.items():
        if key in META_KEYS:
            continue
        # Only add if column truly missing (fresh from inspector!)
        if key not in existing_cols:
            to_add.append((key, infer_sqla_type(val)))

    if not to_add:
        return

    # Perform ALTER TABLE for missing columns
    with engine.begin() as conn:
        for name, typ in to_add:
            ddl = f'ALTER TABLE "{tbl.name}" ADD COLUMN "{name}" {typ().__visit_name__.upper()}'
            conn.exec_driver_sql(ddl)
            log.info("Added column %s.%s (%s)", tbl.name, name, typ.__name__)

    # Now fully refresh SQLAlchemy's metadata + cache
    metadata.clear()
    metadata.reflect(bind=engine)
    _table_cache[tbl.name] = metadata.tables[tbl.name]


# Insert logic
def _insert_rows(table: Table, rows: List[Dict[str, Any]]) -> int:
    """
    Insert many rows. Coerces values to column types for safety.
    If the DB rejects types (e.g., SQLite strict DateTime/Boolean), fall back
    to a second pass where all non-null values are stringified. This keeps the
    consumer from crashing on weird data while still preserving information.
    """
    if not rows:
        return 0

    def build_prepared(rows_to_use: List[Dict[str, Any]], force_str: bool = False):
        prepared_local = []
        col_types = {
            c.name: c.type.__class__
            for c in table.columns
            if c.name not in ("id", "created_at")
        }
        for rec in rows_to_use:
            clean = {}
            for k, v in rec.items():
                if k in META_KEYS:
                    continue

                if force_str:
                    # fallback mode: just stringify everything except None
                    clean[k] = None if v is None else str(v)
                    continue

                # normal mode: type-aware coercion
                if k not in col_types:
                    # Column might have been added mid-batch; refresh schema
                    _add_missing_columns(table, rec)
                    col_types = {
                        c.name: c.type.__class__
                        for c in table.columns
                        if c.name not in ("id", "created_at")
                    }

                col_type = col_types.get(k, String)
                try:
                    clean[k] = coerce_value(v, col_type)
                except Exception:
                    # If coercion fails, stringify as last resort for this value
                    clean[k] = None if v is None else str(v)

            prepared_local.append(clean)
        return prepared_local

    # First attempt: best-effort typed coercion
    prepared = build_prepared(rows, force_str=False)

    with engine.begin() as conn:
        try:
            conn.execute(table.insert(), prepared)
        except StatementError as e:
            # DB complained about types (e.g., SQLite DateTime/Boolean strictness).
            # Log and retry with all values stringified so we don't drop data.
            log.warning("Type error on insert (%s). Retrying with all-string payloads...", e)
            fallback = build_prepared(rows, force_str=True)
            conn.execute(table.insert(), fallback)

    return len(rows)


# Kafka loop

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
