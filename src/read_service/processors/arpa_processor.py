"""
arpa_processor.py
This code retrieves the ARPA funds data from the database and returns the
data.

Here is how you run it:
1. Start up the project's Docker containers.
2. Run python -m src.write_service.consumers.json_consumer from the project's root folder 
so ARPA data from the City of St. Louis is added to the database, so the database isn't empty.
3. Go to http://localhost:5001/api/arpa to see the ARPA endpoint (which uses this code).
4. Go to http://localhost:5001/swagger to see the Swagger U.I..
5. Go to http://localhost:5001/arpa.htm to see the ARPA frontend table U.I. (excellence project)
"""
import os
from sqlalchemy import Column, Integer, DateTime, JSON, String, Boolean, create_engine, select
from sqlalchemy.orm import Session, DeclarativeBase
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
import urllib.parse
from src.write_service.ingestion.json_fetcher import get_json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

# Code will choose between two different hosts: 
# localhost (for local runs) and postgres (when running in Docker)
PG_HOST = os.getenv('PG_HOST', 'localhost,postgres') + ',postgres'

# Code will choose between two different ports:
# PG_PORT (for local runs) and 5432 (when running in Docker)
PG_PORT = [os.getenv('PG_PORT', '5433'), '5432']

PG_DB = os.getenv('PG_DB', 'stl_data')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', "Welcome@123456") # update with pg password if needed

# SQL Alchemy requires this base class thing
class Base(DeclarativeBase):
    pass

# The ARPA funds table class
class DataTable(Base):
    __tablename__ = "ARPA_funds"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    content = Column(JSON, nullable=False)
    is_active = Column(Boolean)
    data_posted_on = Column(DateTime, default=datetime.now(timezone.utc))

def retrieve_from_database():
    """
    This function retrieves the ARPA fund data from the database and returns
    it as a list of dictionaries.
    """

    try:
        data = []

        # Connect to database
        encoded_password = urllib.parse.quote_plus(PG_PASSWORD)

        # Code will choose between two different ports:
        # PG_PORT (for local runs) and 5432 (when running in Docker)
        for port in PG_PORT:
            try:
                engine_url = f"postgresql://{PG_USER}:{encoded_password}@{PG_HOST}:{port}/{PG_DB}"
                engine = create_engine(engine_url, echo=True)

                # Test connection, if fail the exception will move on to the next port
                # If success, use this engine
                engine.connect()
                break
            except Exception as e:
                logger.error(f"An error occurred when connecting to the database at port {port}. \n " + str(e))
                continue

        with Session(engine) as session:
            # Now let's grab stuff from the table
            result = select(DataTable)
            
            for entity in session.scalars(result):
                # Only add active data
                if (entity.is_active):
                    data.append({"id": entity.id, "name": entity.name, 
                                "content": entity.content, "data_posted_on": entity.data_posted_on,
                                "is_active": entity.is_active})

        # We are done!
        engine.dispose()

        # Return the data (list of dictionaries)
        return data

    # Exceptions
    except SQLAlchemyError as e:
        logger.error("An error occurred when retrieving data from the database. \n " + str(e))
        return "An error occurred when retrieving data from the database."

    except Exception as e:
        logger.error("An error occurred when connecting to the database. \n " + str(e))
        return "An error occurred when connecting to the database."

# Test saving data into database
def save_into_database(data):
    """
    This is just a test function to put sample data into the database.
    """

    try:
        # Connect to the database
        encoded_password = urllib.parse.quote_plus(PG_PASSWORD)
        
        # Code will choose between two different ports:
        # PG_PORT (for local runs) and 5432 (when running in Docker)
        for port in PG_PORT:
            try:
                engine_url = f"postgresql://{PG_USER}:{encoded_password}@{PG_HOST}:{port}/{PG_DB}"
                engine = create_engine(engine_url, echo=True)

                # Test connection, if fail the exception will move on to the next port
                # If success, use this engine
                engine.connect()
                break
            except Exception as e:
                logger.error(f"An error occurred when connecting to the database at port {port}. \n " + str(e))
                continue

        # Create the table if not created yet
        Base.metadata.create_all(engine)

        # Start a session
        with Session(engine) as session:
            entity_counter = 1

            # Save the data in the database
            for entity in data:
                new_row = DataTable(
                    name = "ARPA Funds Entity #" + str(entity_counter),
                    content = entity,
                    is_active = True)
                session.add(new_row)
                entity_counter += 1

            # Push all changes
            session.commit() 

    # Exceptions
    except SQLAlchemyError as e:
        logger.error("An error occurred when saving to the database. \n " + str(e))

    except Exception as e:
        logger.error("An error occurred when connecting to the database. \n " + str(e))

# Test function that saves real City of St. Louis data (ARPA funds) to the database
# and then retrieves from the database
if __name__ == "__main__":
    testURL = "https://www.stlouis-mo.gov/customcf/endpoints/arpa/expenditures.cfm?format=json"
    result = get_json(testURL)
    save_into_database(result)
    data = retrieve_from_database()
    # print(data)
