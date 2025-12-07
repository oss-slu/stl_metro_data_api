# src/write_service/consumers/models.py
"""
Database models for storing web-scraped data.
"""

from sqlalchemy import Column, Integer, String, DateTime, JSON, create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
import urllib  

load_dotenv()


Base = declarative_base()
PG_HOST = os.getenv('PG_HOST', 'postgres')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB = os.getenv('PG_DB', 'stl_data')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', "Welcome@123456")

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
    ingested_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

# Add this to your existing models.py in write_service

class CSBServiceRequest(Base):
    """
    Stores CSB (Citizens' Service Bureau) 311 service request data.
    Source: https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5
    Data: https://www.stlouis-mo.gov/data/upload/data-files/csb.zip
    
    This dataset provides access to Citizens' Service Bureau (CSB) service
    requests using the Open311 spec. The CSB is the customer service 
    department for the City of St. Louis.
    
    Columns:
    - id: Primary key
    - created_on: Record creation timestamp
    - data_posted_on: When service request was initiated
    - is_active: Integer flag for active records (1=active, 0=inactive/closed)
    - service_name: Service request type (PROBLEMCODE)
    - contact_info: JSON field for caller info, address, ward, etc.
    - description: Service request description
    - source_url: Link to original dataset
    - raw_json: Complete data payload from CSV
    - ingested_at: When record was inserted into database
    """
    __tablename__ = "csb_service_requests" # Citizens' Service Bureau is customer service department for STL
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    data_posted_on = Column(DateTime, nullable=True)
    is_active = Column(Integer, default=1, nullable=False)  # 1=active, 0=inactive
    
    # Add fields that map to the actual site data from stlouis-mo.gov/services
    service_name = Column(String, nullable=True)
    contact_info = Column(JSON, nullable=True)  # Store phone, email, address as JSON
    description = Column(String, nullable=True)
    source_url = Column(String, default='https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5', nullable=True)
    
    raw_json = Column(JSON, nullable=False)  # Store complete data
    ingested_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

def get_db_engine():  # create database engine from .env config
    encoded_password = urllib.parse.quote_plus(PG_PASSWORD)
    db_url = os.getenv(
        "DATABASE_URL",
        f"postgresql://{PG_USER}:{encoded_password}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    )
    logger = __import__("logging").getLogger(__name__)
    logger.info("Connecting to database: %s", db_url.split("@")[1] if "@" in db_url else db_url)

    engine = create_engine(db_url)

    # Ensure tables exist (idempotent). If the DB isn't ready, surface a clear warning.
    try:
        create_tables(engine)
        logger.info("Ensured ORM tables exist (create_tables).")
    except Exception as e:
        # Keep the engine returnable even if create_tables failed; tests or caller
        # can retry or surface the connection error. Log a clear message.
        logger.warning("Failed to create tables during engine init: %s", e)

    return engine

def create_tables(engine): # create all tables if they don't exist"
    Base.metadata.create_all(engine)