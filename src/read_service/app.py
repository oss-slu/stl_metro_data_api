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
from flask import Flask, jsonify
from flask_restful import Api
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from flask import send_from_directory

# Environment vars
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB = os.getenv('PG_DB', 'stl_data')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'example_pass')

# Database engine (shared across queries)
engine_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(engine_url, echo=False)  # Set echo=True for debug
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Initialize Flask app
app = Flask(__name__)
CORS(app)
api = Api(app)

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
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

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

@app.route('/query-stub', methods=['GET'])
def query_stub():
    """
    Placeholder query endpoint required by assignment spec.
    This does not connect to the database.
    It just proves that the read_service has a query stub.
    """
    return jsonify({"message": "This is a query stub endpoint"})

@app.route('/')
def index():
    return send_from_directory('.', 'csb-dashboard.html')
    
# Error handler for 404
@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

# Error handler for 500
@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    # Import and start the mock Kafka consumer before running Flask.
    # This ensures that when the read-service starts, it also begins
    # simulating event consumption in the background.
    from processors.events import start_mock_consumer
    start_mock_consumer(app.logger)

    # Run the Flask app (debug mode can be enabled via FLASK_DEBUG env var)
    debug_mode = os.getenv('FLASK_DEBUG', 'False').lower() in ('1', 'true', 'yes')
    app.run(host='0.0.0.0', port=5003, debug=debug_mode)