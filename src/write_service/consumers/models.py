# src/write_service/consumers/models.py
"""
Database models for storing web-scraped data.
"""

from sqlalchemy import Column, Integer, String, DateTime, JSON, create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, UTC
import os
from dotenv import load_dotenv  

load_dotenv()


Base = declarative_base()

class StLouisCensusData(Base):
    """
    Stores census data from St. Louis government website
    Columns:
    - id: auto incrementing key
    - raw_json: complete Kafka message as JSON
    - ingested_at: timestamp of insertion
    """
    __tablename__ = "stlouis_gov_census" # each row/entry is a table

    id = Column(Integer, primary_key=True, autoincrement=True) 
    # title = Column(String, ) # pull out title from each table
    # isActive, default false, when add new stuff into table, deactivate old rows if new rows are succesfully added
    raw_json = Column(JSON, nullable=False) # Store the entire kafka message
    ingested_at = Column(DateTime, default=lambda: datetime.now(UTC))

def get_db_engine(): # create database engine from .env config
    #db_url = os.getenv("DATABASE_URL", "postgresql://user:password@stl_postgres:5434/stl_data") # might need to change port number later
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:bajarderp@localhost:5434/stl_data")
    logger = __import__('logging').getLogger(__name__)
    logger.info(f"Connecting to database: {db_url.split('@')[1]}")
    return create_engine(db_url)

def create_tables(engine): # create all tables if they don't exist"
    Base.metadata.create_all(engine)