# --- force src/ onto sys.path for reliable imports ---
import sys, pathlib, importlib
ROOT = pathlib.Path(__file__).resolve().parents[2]   # repo root
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
importlib.invalidate_caches()
# -----------------------------------------------------

import importlib
import os
import time
from datetime import datetime

import pytest
from sqlalchemy import text, inspect

# We will import AFTER setting PG_DSN to an in-memory SQLite, so the module creates a SQLite engine.
MODULE_PATH = "src.write_service.consumers.excel_consumer"

@pytest.fixture(autouse=True)
def sqlite_engine_env(monkeypatch):
    # Use isolated in-memory SQLite for unit tests
    monkeypatch.setenv("PG_DSN", "sqlite+pysqlite:///:memory:")
    # Reset caches between tests by reloading the module
    mod = importlib.import_module(MODULE_PATH)
    importlib.reload(mod)
    yield
    # nothing to tear down

def _reload():
    mod = importlib.import_module(MODULE_PATH)
    return importlib.reload(mod)

def test_table_routing_priority(monkeypatch):
    ec = _reload()
    # 1) _table on record wins
    assert ec._resolve_table_name("processed.excel.data", {"_table": "tbl1"}) == "tbl1"
    # 2) _site next
    assert ec._resolve_table_name("processed.excel.data", {"_site": "tbl2"}) == "tbl2"
    # 3) TOPIC_TABLE_MAP if provided
    monkeypatch.setenv("TOPIC_TABLE_MAP", '{"processed.excel.data":"tbl3"}')
    ec = _reload()
    assert ec._resolve_table_name("processed.excel.data", {}) == "tbl3"
    # 4) DEFAULT_TABLE last
    monkeypatch.setenv("TOPIC_TABLE_MAP", "{}")
    monkeypatch.setenv("SITE_NAME", "tbl4")
    ec = _reload()
    assert ec._resolve_table_name("processed.excel.data", {}) == "tbl4"

def test_create_table_and_insert_rows():
    ec = _reload()
    engine = ec.engine

    record = {
        "_table": "stlouis_gov_crime",
        "incident_number": "25-0001",
        "offense": "THEFT",
        "ward": "7",                          # string that should coerce to int
        "occurred_at": "2025-01-02T03:04:05", # iso datetime
        "latitude": "38.62",                  # string → float
        "longitude": -90.20,                  # already float-ish
        "flag": "true",                       # string → bool
    }
    tbl = ec._reflect_or_create_table("stlouis_gov_crime", record)
    assert tbl.name == "stlouis_gov_crime"

    inserted = ec._insert_rows(tbl, [record])
    assert inserted == 1

    with engine.begin() as conn:
        rows = conn.execute(text('SELECT incident_number, offense, ward, latitude, longitude, flag FROM "stlouis_gov_crime"')).fetchall()
    assert len(rows) == 1
    inc, off, ward, lat, lon, flag = rows[0]
    assert inc == "25-0001"
    assert off == "THEFT"
    assert ward == 7
    assert abs(lat - 38.62) < 1e-6
    assert abs(lon - (-90.20)) < 1e-6
    assert flag in (1, True)  # sqlite stores booleans as 0/1

def test_schema_evolution_adds_column():
    ec = _reload()
    engine = ec.engine
    table_name = "evolving_table"

    # first batch without new column
    rec1 = {"_table": table_name, "id_external": "A1", "value": "foo"}
    tbl = ec._reflect_or_create_table(table_name, rec1)
    ec._insert_rows(tbl, [rec1])

    # new batch introduces a brand-new column "new_field"
    rec2 = {"_table": table_name, "id_external": "A2", "value": "bar", "new_field": 123}

    # calling reflect/ensure should ALTER TABLE and add the column
    tbl = ec._reflect_or_create_table(table_name, rec2)
    ec._insert_rows(tbl, [rec2])

    insp = inspect(engine)
    cols = {c["name"] for c in insp.get_columns(table_name)}
    assert "new_field" in cols

    with engine.begin() as conn:
        rows = conn.execute(text(f'SELECT id_external, value, new_field FROM "{table_name}" ORDER BY id')).fetchall()
    assert len(rows) == 2
    assert rows[1][2] in (123, "123")  # coercion may stringify on sqlite, still present

def test_type_coercion_matrix():
    ec = _reload()
    engine = ec.engine
    t = "coercion_table"
    rec = {
        "_table": t,
        "as_int": "42",
        "as_float": "3.14",
        "as_bool_true": "true",
        "as_bool_false": "False",
        "as_dt": "2024-02-29 12:34:56",
        "as_str": 777,
    }
    tbl = ec._reflect_or_create_table(t, rec)
    ec._insert_rows(tbl, [rec])

    with engine.begin() as conn:
        rows = conn.execute(text(f'SELECT as_int, as_float, as_bool_true, as_bool_false, as_dt, as_str FROM "{t}"')).fetchall()
    (as_int, as_float, as_bool_true, as_bool_false, as_dt, as_str) = rows[0]

    # SQLite types are dynamic; we assert semantic coercion rather than strict PG types here
    assert as_int in (42, "42")
    assert float(as_float) == pytest.approx(3.14, 1e-6)
    # bools may be stored as 0/1
    assert as_bool_true in (1, True, "1")
    assert as_bool_false in (0, False, "0")
    # datetime stored as string by sqlite, but not empty
    assert as_dt is not None
    # numeric was coerced to string for as_str
    assert str(as_str) == "777"
