# src/write_service/consumers/models.py
"""
Database models for storing web-scraped data.
"""

from sqlalchemy import Column, Integer, String, DateTime, JSON, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
from dotenv import load_dotenv  # ← ADD THIS

load_dotenv()


Base = declarative_base()

class StLouisCensusData(Base):
    """
    Stores census data from St. Louis government website
    """
    __tablename__ = "stlouis_gov_census"
    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_json = Column(JSON, nullable=False) # Store the entire kafka message
    ingested_at = Column(DateTime, default=datetime.utcnow)

def get_db_engine(): # create database engine from .env config
    db_url = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5434/stl_data") # might need to change port number later
    return create_engine(db_url)

def create_tables(engine): # create all tables if they don't exist"
    Base.metadata.create_all(engine)