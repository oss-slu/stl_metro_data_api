"""
session.py
-----------
Creates the database engine and session factory for SQLAlchemy.
This connects to PostgreSQL using the connection string from .env.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import urllib

PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB = os.getenv('PG_DB', 'stl_data')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', "123456")

encoded_password = urllib.parse.quote_plus(PG_PASSWORD)
# Connection string to PostgreSQL, like:
# postgresql+psycopg2://user:password@hostname:port/db_name
PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql+psycopg2://{PG_USER}:{encoded_password}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

# Create a SQLAlchemy engine — this manages the connection pool
engine = create_engine(PG_DSN, pool_pre_ping=True, future=True)

# Create a session factory — this generates new DB sessions on demand
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

# Base class for ORM models to inherit from (like STLouisGovCrime)
Base = declarative_base()

def get_session():
    """
    Returns a new SQLAlchemy session.
    Usage:
        with get_session() as session:
            session.query(...)
    """
    return SessionLocal()
