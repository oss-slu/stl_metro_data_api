from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json, time
import os
import time
from sqlalchemy import Column, Integer, DateTime, JSON, String, create_engine
from sqlalchemy.orm import Session, DeclarativeBase
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
import urllib.parse

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB = os.getenv('PG_DB', 'stl_data')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', "Welcome@123456") # update with pg password if needed

# SQL Alchemy requires this base class thing
class Base(DeclarativeBase):
    pass

def get_table_class(table_name):
    """
    Get the table class with the given table name
    """
    class DataTable(Base):
            __tablename__ = table_name
            id = Column(Integer, primary_key=True, autoincrement=True)
            name = Column(String)
            content = Column(JSON, nullable=False)
            date_added = Column(DateTime, default=datetime.now(timezone.utc))
    return DataTable

def retrieve_from_database():
    """
    This function retrieves the ARPA fund data from the database.
    """

    try:
        # Connect to database
        encoded_password = urllib.parse.quote_plus(PG_PASSWORD)
        engine_url = f"postgresql+psycopg2://{PG_USER}:{encoded_password}@{PG_HOST}:{PG_PORT}/{PG_DB}"
        engine = create_engine(engine_url, echo=True)

        # Get the table class
        table = get_table_class("arpa")

        # Now let's grab stuff from the table
        result = session.execute(text("SELECT * FROM test")).fetchall()           

        # We are done!
        engine.dispose()

        return result

        print("PostgreSQL: OK")

    # Exceptions
    except SQLAlchemyError as e:
        print("An error occured when connecting to the database. \n " + str(e))
        session.rollback()

    except Exception as e:
        print("An error occured when retreiving from the database. \n " + str(e))
