# models.py
import os
import logging
import urllib
from datetime import datetime, timezone

from sqlalchemy import Column, Integer, DateTime, JSON, Boolean, create_engine
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv

load_dotenv(override=True)

Base = declarative_base()
logger = logging.getLogger(__name__)

PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5433')
PG_DB = os.getenv('PG_DB', 'stl_data')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'Welcome123456')


def get_db_engine():
    """Return a SQLAlchemy engine and ensure ORM tables exist."""
    encoded_password = urllib.parse.quote_plus(PG_PASSWORD)
    db_url = os.getenv(
        "DATABASE_URL",
        f"postgresql://{PG_USER}:{encoded_password}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    logger.info("Connecting: %s", db_url.split("@")[1] if "@" in db_url else db_url)

    engine = create_engine(db_url, echo=True)
    try:
        create_tables(engine)
    except Exception as e:
        logger.warning("Failed creating tables: %s", e)

    return engine


class StLouisCrimeStats(Base):
    """
    for NIBRS Crime Files sourced from https://slmpd.org/stats/

    Columns:
        id (PK)
        created_on      - record created timestamp
        data_posted_on  - timestamp from source
        is_active       - only active records should be returned
        raw_json        - JSON of original record
    """
    __tablename__ = "stlouis_gov_crime"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    data_posted_on = Column(DateTime, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    raw_json = Column(JSON, nullable=False)


class StLouisCensusData(Base):
    """Stores census data from St. Louis government website"""
    __tablename__ = "stlouis_gov_census"

    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_json = Column(JSON, nullable=False)
    ingested_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

def create_tables(engine):
    """Create all tables if not present."""
    Base.metadata.create_all(engine)
