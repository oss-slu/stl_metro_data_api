# tests/test_business_citizen.py

"""
Tests for business/citizen API endpoint.
Includes unit tests with mocks and end-to-end integration tests.

Source data: https://www.stlouis-mo.gov/services/
"""

import pytest
from unittest.mock import MagicMock, patch
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'read_service'))

from app import app


@pytest.fixture
def client():
    """Flask test client fixture."""
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


def test_business_citizen_api_returns_active_only(client, mocker):
    """
    Unit test: Verify only active records (is_active=1) are returned.
    Uses mocked database to avoid real DB dependency.
    """
    # Mock the processor function
    mock_data = [
        {
            "id": 1,
            "service_name": "Business License Application",
            "is_active": 1,
            "source_url": "https://www.stlouis-mo.gov/services/"
        },
        {
            "id": 2,
            "service_name": "Permit Services",
            "is_active": 1,
            "source_url": "https://www.stlouis-mo.gov/services/"
        }
    ]
    
    mocker.patch(
        'processors.business_citizen_processor.get_business_citizen_data',
        return_value=mock_data
    )
    
    response = client.get('/api/business')
    
    assert response.status_code == 200
    data = response.get_json()
    assert len(data) == 2
    assert all(record['is_active'] == 1 for record in data)


def test_business_citizen_api_includes_source_url(client, mocker):
    """
    Unit test: Verify each record includes the correct source URL.
    Ensures attribution to https://www.stlouis-mo.gov/services/
    """
    mock_data = [
        {
            "id": 1,
            "service_name": "Test Service",
            "source_url": "https://www.stlouis-mo.gov/services/"
        }
    ]
    
    mocker.patch(
        'processors.business_citizen_processor.get_business_citizen_data',
        return_value=mock_data
    )
    
    response = client.get('/api/business')
    data = response.get_json()
    
    assert all('source_url' in record for record in data)
    assert data[0]['source_url'] == "https://www.stlouis-mo.gov/services/"


def test_business_citizen_api_returns_json_array(client, mocker):
    """
    Unit test: Verify API returns a JSON array structure.
    """
    mock_data = [
        {
            "id": 1,
            "service_name": "Test Service",
            "contact_info": {"phone": "314-555-1234"},
            "source_url": "https://www.stlouis-mo.gov/services/"
        }
    ]
    
    mocker.patch(
        'processors.business_citizen_processor.get_business_citizen_data',
        return_value=mock_data
    )
    
    response = client.get('/api/business')
    data = response.get_json()
    
    assert isinstance(data, list)
    assert len(data) > 0
    assert 'service_name' in data[0]


@pytest.mark.integration
def test_business_citizen_e2e_with_active_data(client):
    """
    End-to-end integration test with real database.
    Requires test database with is_active=1 records.
    
    This test validates the full flow:
    1. Database connection via SessionLocal
    2. Query execution with is_active=1 filter
    3. JSON serialization
    4. HTTP 200 response
    5. Source URL attribution to stlouis-mo.gov/services
    """
    response = client.get('/api/business')
    
    assert response.status_code == 200
    data = response.get_json()
    
    # Verify response structure
    assert isinstance(data, list)
    
    # If data exists, verify all records are active
    if len(data) > 0:
        for record in data:
            assert record.get('is_active') == 1
            assert 'source_url' in record
            assert 'stlouis-mo.gov/services' in record['source_url']
            # Verify required fields exist
            assert 'id' in record
            assert 'created_on' in record


def test_business_citizen_api_error_handling(client, mocker):
    """
    Unit test: Verify proper error handling when database query fails.
    Should return 500 status with error message.
    """
    mocker.patch(
        'processors.business_citizen_processor.get_business_citizen_data',
        side_effect=Exception("Database connection failed")
    )
    
    response = client.get('/api/business')
    
    assert response.status_code == 500
    response_data = response.get_json()
    assert 'error' in response_data
    assert 'Database connection failed' in response_data['error']


def test_business_citizen_api_empty_result(client, mocker):
    """
    Unit test: Verify API handles empty results gracefully.
    Should return empty array with 200 status when no active records exist.
    """
    mocker.patch(
        'processors.business_citizen_processor.get_business_citizen_data',
        return_value=[]
    )
    
    response = client.get('/api/business')
    
    assert response.status_code == 200
    data = response.get_json()
    assert data == []
    assert isinstance(data, list)
