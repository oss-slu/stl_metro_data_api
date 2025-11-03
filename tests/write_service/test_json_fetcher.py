"""
Tests for the write service fetch Python app.
"""

# Get the Flask app
from src.write_service.ingestion.json_fetcher import get_json

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
    
def test_city_json():
    """This test checks if the program is able to retrieve and parse through real City data."""
    url = "https://www.stlouis-mo.gov/customcf/endpoints/arpa/expenditures.cfm?format=json"
    result = get_json(url)

    # If the response is a valid Python list or dictionary, then the JSON data was sucessfully retrieved and parsed
    assert isinstance(result, (list, dict)), "Parsed result is not a list or dictionary"