import time
from sqlalchemy import create_engine, Column, Integer, String, JSON, DateTime, ForeignKey, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import psycopg2
from datetime import datetime
import os
import urllib.parse

# 1. Configuration (Inside Docker uses port 5432)
DB_USER = os.getenv("PG_USER", "postgres")
DB_PASS = os.getenv("PG_PASSWORD", "123456")
DB_HOST = os.getenv("PG_HOST", "postgres")
DB_NAME = os.getenv("PG_DB", "stl_data")

Base = declarative_base()

# 2. Table Definitions (Models)
class Snapshot(Base):
    __tablename__ = 'snapshots'
    id = Column(Integer, primary_key=True)
    data = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class HistoricData(Base):
    __tablename__ = 'historic_data'
    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey('snapshots.id', ondelete='CASCADE'))
    old_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)

def initialize_database():
    # A. Wait for Postgres to be ready and create the DB
    conn = None
    while not conn:
        try:
            # Connect to default 'postgres' db to create the target db
            conn = psycopg2.connect(
                dbname='postgres', user=DB_USER, password=DB_PASS, host=DB_HOST, port=5432
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
                if not cur.fetchone():
                    cur.execute(f"CREATE DATABASE {DB_NAME}")
            conn.close()
        except Exception as e:
            print(f"Waiting for database... {e}")
            time.sleep(2)

    # B. Create Tables using SQLAlchemy
    safe_pass = urllib.parse.quote_plus(DB_PASS)
    
    connection_url = f"postgresql+psycopg2://{DB_USER}:{safe_pass}@{DB_HOST}:5432/{DB_NAME}"
    engine = create_engine(connection_url)
    
    print("Attempting to create tables...")
    Base.metadata.create_all(engine)
    print("Database and tables initialized successfully.")

if __name__ == "__main__":
    initialize_database()