"""
app.py: Main Flask application for the CQRS Query Side (Read Service) in STL Data API.

This microservice handles read-optimized operations:
- Event processors for short-term queries from PostgreSQL.
- RESTful API endpoints for unique processes (e.g., querying processed data).
- Open API (Swagger) integration for documentation.
- Uses SQLAlchemy for PG interactions; focuses on optimized reads (e.g., indexes, caching stubs).

Run with: python app.py (starts on port 5001).
Integrates with write_service via shared PG (CQRS separation).
"""

import os
import webbrowser
import threading
from flask import Flask, jsonify, render_template, render_template_string
from flask_restful import Api
from flask_swagger_ui import get_swaggerui_blueprint
from pytest import Session
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from flask_cors import CORS
import requests

from dotenv import load_dotenv

try: 
    load_dotenv()
except ImportError:
    pass
from src.read_service.processors.arpa_processor import retrieve_from_database, save_into_database

# Initialize Flask app
app = Flask(__name__)
api = Api(app)

# Use Flask CORS to allow connections from other sites
CORS(app)

# Environment vars
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5433')
PG_DB = os.getenv('PG_DB', 'stl_data')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'example_pass')

# Database engine (shared across queries)
from urllib.parse import quote_plus
password = quote_plus(PG_PASSWORD) # wraps the password in quotes so it doesn't mistake it for the host
engine_url = f"postgresql+psycopg2://{PG_USER}:{password}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(engine_url, echo=False)  # Set echo=True for debug
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# Make sure INFO-level logs (like from the mock consumer) show up
app.logger.setLevel("INFO")

# Swagger UI configuration
SWAGGER_URL = '/swagger'  # URL for Swagger UI (e.g., http://localhost:5001/swagger)
API_URL = '/swagger.json'

# Swagger UI config (passed to blueprint)
SWAGGER_CONFIG = {
    'app_name': "STL Data API - Read Service",
    'deepLinking': True,
    'defaultModelsExpandDepth': -1,
    'showExtensions': True,
    'showCommonExtensions': True
}

# Create Swagger UI blueprint
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config=SWAGGER_CONFIG
)

# Register the blueprint
app.register_blueprint(swaggerui_blueprint)

@app.route('/api/arpa', strict_slashes = False)
def arpa():
    """
    This function returns ALL ARPA funds data from the ARPA Processor in JSON format
    """
    
    result = retrieve_from_database()
    print(result)
    
    # Return data / message with response code
    if result is None:
        return jsonify([{"Response": "Data is empty"}]), 200
    else:
        return jsonify(result), 200

@app.route('/api/arpa/<number_of_entries>', strict_slashes = False)
def arpa_number_of_entries(number_of_entries):
    """
    This function returns the ARPA funds data from the ARPA Processor in JSON format
    Returns entries with number of most recent entries specified in URL path
    """
    try:
        number_of_entries = int(number_of_entries)

        result = retrieve_from_database()

        # If number of entries bigger than dataset's length or negative, return all entries
        if (number_of_entries is None or number_of_entries > len(result) or number_of_entries < 0):
            number_of_entries = 0

        # Get most recent n entries
        result = result[-number_of_entries:]
        print(result)
        
        # Return data / message with response code
        if result is None:
            return jsonify([{"Response": "Data is empty"}]), 200
        else:
            return jsonify(result), 200
        
    # Handle errors
    except ValueError:
        print("You did not input a valid number of entries. Please try again.")
        return jsonify([{"Error": "You did not input a valid number of entries. Please try again."}]), 500
    except Exception as e:
        print("Some other error occurred! \n" + str(e))
        return jsonify([{"Error": "Some other error occurred."}]), 500

@app.route('/')
@app.route('/index.htm')
def main():
    """
    The landing page for the front-end
    """
    return render_template("index.htm")

@app.route('/arpa.htm')
def arpa_frontend_page():
    """
    Show the ARPA table frontend U.I.
    """
    return render_template("arpa.htm")

# Basic health check endpoint (Query side: Check PG connection)
@app.route('/health', methods=['GET'])
def health():
    """
    Health check for read_service: Verifies PG connection for queries.
    Returns: {"status": "ok", "postgres": true}
    """
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        postgres_status = True
    except OperationalError as e:
        app.logger.error(f"PG health check failed: {e}")
        postgres_status = False
    
    return jsonify({
        "status": "ok" if postgres_status else "error",
        "service": "read_service",
        "postgres": postgres_status
    })

# Example event processor endpoint (stub for short-term read optimization)
@app.route('/events/<event_type>', methods=['GET'])
def get_events(event_type):
    """
    Query optimized events (e.g., aggregations, snapshots) from PG.
    For now, stub for processed data (e.g., transit routes from Kafka).
    TODO: Implement caching (e.g., Redis stub) for short-term reads.
    Returns: List of events/aggregations.
    """
    # Dependency: Assume 'events' table exists (from write_service or migrations)
    try:
        db = SessionLocal()
        result = db.execute(text("""
            SELECT * FROM events 
            WHERE event_type = :type 
            ORDER BY timestamp DESC 
            LIMIT 10
        """), {"type": event_type})
        events = [dict(row._mapping) for row in result]
        db.close()
        return jsonify({"events": events})
    except Exception as e:
        app.logger.error(f"Event query failed: {e}")
        return jsonify({"error": str(e)}), 500

# Basic API endpoint for unique processes (e.g., query processed raw data)
@app.route('/data/<data_type>', methods=['GET'])
def get_processed_data(data_type):
    """
    Retrieve processed data (e.g., 'routes', 'schedules') from PG.
    Optimized for reads: Uses indexes (assume created in migrations).
    Provision for subscriptions: Stub for future (e.g., ?subscribe=true).
    Returns: List of processed records.
    """
    try:
        db = SessionLocal()
        if data_type == 'routes':
            result = db.execute(text("SELECT * FROM routes ORDER BY name"))
            data = [dict(row._mapping) for row in result]
        else:
            data = [] 
        db.close()
        return jsonify({"data_type": data_type, "data": data})
    except Exception as e:
        app.logger.error(f"Data query failed: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/swagger.json', methods=['GET'])
def swagger_spec():
    """
    Open API spec for read_service endpoints.
    """
    return jsonify({
        "openapi": "3.0.0",
        "info": {"title": "STL Data API Read Service", "version": "1.0.0"},
        "paths": {
            "/health": {
                "get": {
                    "summary": "Health check",
                    "responses": {"200": {"description": "OK"}}
                }
            },
            "/api/arpa": {
                "get": {
                    "summary": "Get data about ARPA funds usage (all entries)",
                    "tags": ["City Budget and Funding"],
                    "description": "This endpoint retrieves information on how the City of St. Louis used ARPA (American Rescue Plan Act) funds. Data is originally from the St. Louis Open Data portal. All entries are returned.",
                    "responses": {
                        "200": {
                            "description": "The data is a list of projects the City of St. Louis used ARPA funds on. All entries are returned (may be slow on some devices).",
                            "content": {
                                "application/json": {
                                    "example": [
                                        {
                                            "id": 1,
                                            "name": "ARPA Funds Entity #1",
                                            "content": {
                                                "ACCOUNT": "1000000",
                                                "AMOUNT": 181,
                                                "CENTER": "7000000",
                                                "CREDIT": None,
                                                "DATE": "August 30, 2025 00:00:00",
                                                "DESC1": "Parking costs",
                                                "DESC2": "",
                                                "DESC3": "",
                                                "FUND": "1170",
                                                "ID": 9000000,
                                                "ORDINANCE": 71000,
                                                "PROJECTID": 42,
                                                "PROJECTTITLE": "Community Health Workers",
                                                "VENDOR": "Example Company"
                                            },
                                            "is_active": True,
                                            "data_posted_on": "Sat, 06 Dec 2025 01:17:47 GMT"
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            "/api/arpa/{number_of_entries}": {
                "get": {
                    "summary": "Get data about ARPA funds usage (specify number of entries)",
                    "parameters": [{"name": "number_of_entries", "in": "path", "required": True, "schema": {"type": "integer"}}],
                    "tags": ["City Budget and Funding"],
                    "description": "This endpoint retrieves information on how the City of St. Louis used ARPA (American Rescue Plan Act) funds. Data is originally from the St. Louis Open Data portal. You can specify how many recent entries you want returned.",
                    "responses": {
                        "200": {
                            "description": "The data is a list of projects the City of St. Louis used ARPA funds on. You can specify how many recent entries you want returned. If you provide a number that is bigger than the dataset's length or a negative number, all results will be returned.",
                            "content": {
                                "application/json": {
                                    "example": [
                                        {
                                            "id": 1,
                                            "name": "ARPA Funds Entity #1",
                                            "content": {
                                                "ACCOUNT": "1000000",
                                                "AMOUNT": 181,
                                                "CENTER": "7000000",
                                                "CREDIT": None,
                                                "DATE": "August 30, 2025 00:00:00",
                                                "DESC1": "Parking costs",
                                                "DESC2": "",
                                                "DESC3": "",
                                                "FUND": "1170",
                                                "ID": 9000000,
                                                "ORDINANCE": 71000,
                                                "PROJECTID": 42,
                                                "PROJECTTITLE": "Community Health Workers",
                                                "VENDOR": "Example Company"
                                            },
                                            "is_active": True,
                                            "data_posted_on": "Sat, 06 Dec 2025 01:17:47 GMT"
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            "/csb": {
                "get": {
                    "summary": "Get active CSB service requests",
                    "tags": ["Business/Citizen Services"],
                    "description": "Retrieves all active (is_active=1) Citizens' Service Bureau (311) service requests from the St. Louis Open Data portal.",
                    "responses": {
                        "200": {
                            "description": "Array of active service requests",
                            "content": {
                                "application/json": {
                                    "example": [
                                        {
                                            "id": 1,
                                            "service_name": "Refuse Collection-Missed Pickup",
                                            "description": "Trash was not collected",
                                            "is_active": 1,
                                            "contact_info": {
                                                "caller_type": "Resident",
                                                "neighborhood": "Downtown",
                                                "ward": "7"
                                            },
                                            "source_url": "https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5",
                                            "data_posted_on": "2025-01-15T08:30:00"
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            "/events/{event_type}": {
                "get": {
                    "summary": "Get events by type",
                    "parameters": [{"name": "event_type", "in": "path", "required": True, "schema": {"type": "string"}}],
                    "responses": {"200": {"description": "Events list"}}
                }
            },
            "/data/{data_type}": {
                "get": {
                    "summary": "Get processed data by type",
                    "parameters": [{"name": "data_type", "in": "path", "required": True, "schema": {"type": "string"}}],
                    "responses": {"200": {"description": "Data list"}}
                }
            }
        }
    })


@app.route('/arpa_direct_retrieval')
def get_arpa_directly_from_City_website():
    """
    Function that retrieves ARPA funds directly from City website and returns as JSON.
    For testing purposes.
    """

    print("Getting JSON data from City website...")
    response = requests.get("https://www.stlouis-mo.gov/customcf/endpoints/arpa/expenditures.cfm?format=json")
    response.raise_for_status()

    print("Parsing the JSON data from City website...")
    # Parse the data
    data = response.json()

    # Return the data
    print(f"Data received successfully: \n {data}")
    return data

@app.route('/query-stub', methods=['GET'])
def query_stub():
    """
    Placeholder query endpoint required by assignment spec.
    This does not connect to the database.
    It just proves that the read_service has a query stub.
    """
    return jsonify({"message": "This is a query stub endpoint"})
    
# Error handler for 404
@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

# Error handler for 500
@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

@app.route("/csb", methods=["GET"])
def get_csb_services():
    """
    GET endpoint for active CSB (Citizens' Service Bureau) service requests.
    
    Retrieves all active records (is_active=1) from csb_service_requests table.
    Each record includes a source URL linking back to the original data source.
    
    Returns:
        JSON array of active CSB 311 service request records
        
    Example response:
        [
            {
                "id": 1,
                "service_name": "Refuse Collection-Missed Pickup",
                "contact_info": {
                    "caller_type": "Resident",
                    "neighborhood": "Downtown",
                    "ward": "7"
                },
                "source_url": "https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5",
                ...
            }
        ]
        
    Source: CSB Service Requests (311) Dataset
            https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5
    Data: https://www.stlouis-mo.gov/data/upload/data-files/csb.zip
    """
    from processors.csb_service_processor import get_csb_service_data
    
    try:
        db = SessionLocal()
        data = get_csb_service_data(db)
        db.close()
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"CSB Service API error: {e}")
        return jsonify({"error": "Internal server error"}), 500
    
def open_browser():
    """Open Swagger UI in browser."""
    import time
    time.sleep(1.5)
    webbrowser.open('http://127.0.0.1:5001/swagger') 

if __name__ == '__main__':
    # Import and start the mock Kafka consumer before running Flask.
    # This ensures that when the read-service starts, it also begins
    # simulating event consumption in the background.
    from src.read_service.processors.events import start_mock_consumer
    start_mock_consumer(app.logger)

    threading.Thread(target=open_browser, daemon=True).start()

    # Run the Flask app (debug mode = True for development only).
    debug_mode = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    app.run(host='0.0.0.0', port=5001, debug=debug_mode)
