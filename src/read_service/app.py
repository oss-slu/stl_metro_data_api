"""
app.py: Main Flask application for the CQRS Query Side (Read Service) in STL Data API.

This microservice handles read-optimized operations:
- Event processors for short-term queries from PostgreSQL.
- RESTful API endpoints for unique processes (e.g., querying processed data).
- Open API (Swagger) integration for documentation.
- Uses SQLAlchemy for PG interactions; focuses on optimized reads (e.g., indexes, caching stubs).

Run with: python app.py (starts on port 5003).
Integrates with write_service via shared PG (CQRS separation).
"""

import os
import webbrowser
import threading
from flask import Flask, jsonify, render_template, send_from_directory
from flask_restful import Api
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
import requests
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Load environment variables
try: 
    load_dotenv()
except ImportError:
    pass

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5433')
PG_DB = os.getenv('PG_DB', 'stl_data')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'example_pass')

password = quote_plus(PG_PASSWORD)
engine_url = f"postgresql+psycopg2://{PG_USER}:{password}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(engine_url, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ============================================================================
# FLASK APP INITIALIZATION
# ============================================================================

app = Flask(__name__)
CORS(app)
api = Api(app)
app.logger.setLevel("INFO")

# ============================================================================
# SWAGGER UI CONFIGURATION
# ============================================================================

SWAGGER_URL = '/swagger'
API_URL = '/swagger.json'
SWAGGER_CONFIG = {
    'app_name': "STL Data API - Read Service",
    'deepLinking': True,
    'defaultModelsExpandDepth': -1,
    'showExtensions': True,
    'showCommonExtensions': True
}

swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL, config=SWAGGER_CONFIG)
app.register_blueprint(swaggerui_blueprint)

# ============================================================================
# FRONTEND PAGES (For Non-Technical Users)
# ============================================================================

@app.route('/')
def main():
    """
    Main landing page - shows project info and links to all dashboards.
    Renders: frontend/index.htm
    """
    frontend_path = os.path.join(os.path.dirname(__file__), '..', '..', 'frontend')
    return send_from_directory(frontend_path, 'index.htm')

@app.route('/csb-dashboard')
def csb_dashboard():
    """
    CSB 311 Service Requests Dashboard - interactive visualization for residents.
    Displays: Active service requests with filtering by ward, service type, and search.
    For: Non-technical users (residents, city officials, community organizers)
    """
    frontend_path = os.path.join(os.path.dirname(__file__), '..', '..', 'frontend')
    return send_from_directory(frontend_path, 'csb.htm')

# ============================================================================
# API ENDPOINTS (For Developers)
# ============================================================================

@app.route('/health', methods=['GET'])
def health():
    """
    Health check endpoint - verifies database connection.
    Returns: {"status": "ok"/"error", "service": "read_service", "postgres": true/false}
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

@app.route("/csb", methods=["GET"])
def get_csb_services():
    """
    CSB API Endpoint - returns active 311 service requests as JSON.
    Used by: Developers, CSB dashboard frontend
    Returns: Array of active CSB service request objects
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

@app.route('/api/arpa', strict_slashes=False)
def arpa():
    """
    ARPA Funds API Endpoint - returns ARPA funding usage data.
    Data: American Rescue Plan Act expenditures by the City of St. Louis
    Returns: Array of ARPA project funding records
    """
    from processors.arpa_processor import retrieve_from_database
    
    result = retrieve_from_database()
    if result is None:
        return jsonify([{"Response": "Data is empty"}]), 200
    else:
        return jsonify(result), 200

@app.route('/events/<event_type>', methods=['GET'])
def get_events(event_type):
    """
    Events API Endpoint - queries optimized event data from PostgreSQL.
    TODO: Implement caching (e.g., Redis) for short-term reads.
    """
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

@app.route('/data/<data_type>', methods=['GET'])
def get_processed_data(data_type):
    """
    Generic Data API Endpoint - retrieves processed data by type.
    Example types: 'routes', 'schedules'
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

@app.route('/query-stub', methods=['GET'])
def query_stub():
    """
    Placeholder query endpoint (required by assignment spec).
    Does not connect to database - just proves read_service has query capability.
    """
    return jsonify({"message": "This is a query stub endpoint"})

# ============================================================================
# TESTING/DEBUG ENDPOINTS
# ============================================================================

@app.route('/arpa_direct_retrieval')
def get_arpa_directly_from_City_website():
    """
    Test endpoint - retrieves ARPA data directly from City website (bypasses database).
    For: Testing and debugging purposes only
    """
    print("Getting JSON data from City website...")
    response = requests.get("https://www.stlouis-mo.gov/customcf/endpoints/arpa/expenditures.cfm?format=json")
    response.raise_for_status()
    data = response.json()
    print(f"Data received successfully: \n {data}")
    return data

# ============================================================================
# SWAGGER API DOCUMENTATION
# ============================================================================

@app.route('/swagger.json', methods=['GET'])
def swagger_spec():
    """
    OpenAPI specification for all API endpoints.
    Consumed by: Swagger UI at /swagger
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
            "/csb": {
                "get": {
                    "summary": "Get active CSB service requests",
                    "tags": ["Citizens Services"],
                    "description": "Retrieves all active (is_active=1) Citizens' Service Bureau (311) service requests from the St. Louis Open Data portal.",
                    "responses": {
                        "200": {
                            "description": "Array of active service requests",
                            "content": {
                                "application/json": {
                                    "example": [{
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
                                    }]
                                }
                            }
                        }
                    }
                }
            },
            "/api/arpa": {
                "get": {
                    "summary": "Get ARPA funds usage data",
                    "tags": ["City Budget"],
                    "description": "Retrieves information on how the City of St. Louis used ARPA (American Rescue Plan Act) funds.",
                    "responses": {
                        "200": {
                            "description": "The data is a list of projects the City of St. Louis used ARPA funds on.",
                            "content": {
                                "application/json": {
                                    "example": [{
                                        "id": 1,
                                        "name": "ARPA Funds Entity #1",
                                        "content": {
                                            "PROJECTTITLE": "Community Health Workers",
                                            "AMOUNT": 181
                                        }
                                    }]
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

# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

# ============================================================================
# STARTUP
# ============================================================================

def open_browser():
    """Open Swagger UI in browser after server starts."""
    import time
    time.sleep(1.5)
    webbrowser.open('http://127.0.0.1:5003/swagger')

if __name__ == '__main__':
    # Start mock Kafka consumer in background
    from processors.events import start_mock_consumer
    start_mock_consumer(app.logger)

    # Open browser to Swagger UI
    threading.Thread(target=open_browser, daemon=True).start()

    # Run Flask app
    debug_mode = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    app.run(host='0.0.0.0', port=5003, debug=debug_mode)
