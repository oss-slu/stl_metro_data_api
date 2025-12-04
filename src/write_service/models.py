"""SQLAlchemy core models and helpers for dynamic per-site tables."""

from typing import Dict, Any
import os
import re
import uuid
import urllib.parse
from datetime import datetime

from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Text, DateTime, func
)
from sqlalchemy.dialects.postgresql import JSONB, UUID, TSVECTOR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text as sa_text

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    os.getenv("PG_URL", None)
) or (
    f"postgresql+psycopg2://{os.getenv('PG_USER','postgres')}:"
    f"{urllib.parse.quote_plus(os.getenv('PG_PASSWORD',''))}@{os.getenv('PG_HOST','localhost')}:"
    f"{os.getenv('PG_PORT','5433')}/{os.getenv('PG_DB','stl_data')}"
)

# SQLAlchemy engine and session
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

Base = declarative_base()

_metadata = MetaData()


def _normalize_table_name(name: str) -> str:
    """Make a safe table name from a site name: lowercase, alphanum + underscore."""
    name = name.lower()
    name = re.sub(r'[^a-z0-9_]+', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    if not name:
        name = "site"
    return f"{name}"


def get_or_create_table_class(site_name: str):
    """
    Return a dynamic SQLAlchemy mapped class for a per-site table.
    If the table doesn't exist, create it with columns:
      - id (UUID primary key)
      - pdf_id (text)
      - snippet (text)
      - entities (jsonb)
      - pages (text)
      - created_at (timestamp)
      - search_vector (tsvector)
    Also creates a GIN index on search_vector for full-text search.

    site_name: string (e.g. "stlouis_gov_crime" or derived from pdf_id)
    """
    normalized = _normalize_table_name(site_name)
    table_name = f"pdf_{normalized}"

    try:
        decl_registry = getattr(Base, "_decl_class_registry", None)
        if decl_registry:
            for cls in decl_registry.values():
                try:
                    if getattr(cls, "__tablename__", None) == table_name:
                        return cls
                except Exception:
                    continue

        if hasattr(Base, "registry") and hasattr(Base.registry, "mappers"):
            try:
                for mapper in Base.registry.mappers:
                    mapped_table = getattr(mapper, "local_table", None) or getattr(mapper, "persist_selectable", None)
                    if mapped_table is not None and getattr(mapped_table, "name", None) == table_name:
                        return mapper.class_
            except Exception:
                pass
    except Exception:
        pass

    # Build table dynamically
    tbl = Table(
        table_name,
        _metadata,
        Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        Column("pdf_id", String(255), nullable=False, index=True),
        Column("snippet", Text, nullable=True),
        Column("entities", JSONB, nullable=True),
        Column("pages", Text, nullable=True),
        Column("created_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
        Column("search_vector", TSVECTOR, nullable=True),
        extend_existing=True,
    )

    # create table if not exists
    if not engine.dialect.has_table(engine.connect(), table_name):
        tbl.create(bind=engine, checkfirst=True)

    # create gin index on search_vector (if not exists)
    index_name = f"idx_{table_name}_search_vector"
    with engine.begin() as conn:
        conn.execute(
            sa_text(
                f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} USING GIN (search_vector)"
            )
        )

    # Create a simple declarative wrapper class
    class_attrs: Dict[str, Any] = {
        "__table__": tbl,
        "__repr__": lambda self: f"<PDFRow(pdf_id={self.pdf_id})>"
    }

    DynamicClass = type(f"PDFRow_{normalized}", (Base,), class_attrs)

    return DynamicClass


def ensure_dead_letter_table():
    """
    Create a persistent dead_letter table where failed payloads go.
    Columns: id (uuid), pdf_id, payload (jsonb), error_msg, attempts, last_error_at
    """
    name = "pdf_dead_letter"
    if engine.dialect.has_table(engine.connect(), name):
        return

    ddl = sa_text(
        f"""
        CREATE TABLE IF NOT EXISTS {name} (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            pdf_id text,
            payload jsonb,
            error_msg text,
            attempts integer DEFAULT 0,
            last_error_at timestamptz DEFAULT now(),
            created_at timestamptz DEFAULT now()
        );
        """
    )
    with engine.begin() as conn:
        conn.execute(ddl)
        # create simple index on pdf_id
        conn.execute(sa_text(f"CREATE INDEX IF NOT EXISTS idx_{name}_pdf_id ON {name} (pdf_id);"))


def ensure_tsvector_support():
    with engine.begin() as conn:
        try:
            conn.execute(sa_text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
        except Exception:
            pass