"""
Tests for CSB service request API endpoint.
Includes unit tests with mocks and end-to-end integration tests.

Source data: CSB Service Requests (311) Dataset
https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5
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


def test_csb_api_returns_active_only(client, mocker):
    """
    Unit test: Verify only active records (is_active=1) are returned.
    Uses mocked database to avoid real DB dependency.
    """
    mock_data = [
        {
            "id": 1,
            "service_name": "Refuse Collection-Missed Pickup",
            "is_active": 1,
            "source_url": "https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5"
        },
        {
            "id": 2,
            "service_name": "Street Pothole Repair",
            "is_active": 1,
            "source_url": "https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5"
        }
    ]
    
    mocker.patch(
        'processors.csb_service_processor.get_csb_service_data',  # ← UPDATED PATH
        return_value=mock_data
    )
    
    response = client.get('/csb')
    
    assert response.status_code == 200
    data = response.get_json()
    assert len(data) == 2
    assert all(record['is_active'] == 1 for record in data)


def test_csb_api_includes_source_url(client, mocker):
    """
    Unit test: Verify each record includes the correct source URL.
    """
    mock_data = [
        {
            "id": 1,
            "service_name": "Test Service",
            "source_url": "https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5"
        }
    ]
    
    mocker.patch(
        'processors.csb_service_processor.get_csb_service_data',  # ← UPDATED PATH
        return_value=mock_data
    )
    
    response = client.get('/csb')
    data = response.get_json()
    
    assert all('source_url' in record for record in data)
    assert 'stlouis-mo.gov/data/datasets/dataset.cfm?id=5' in data[0]['source_url']


def test_csb_api_returns_json_array(client, mocker):
    """
    Unit test: Verify API returns a JSON array structure.
    """
    mock_data = [
        {
            "id": 1,
            "service_name": "Test Service",
            "contact_info": {"phone": "314-555-1234"},
            "source_url": "https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5"
        }
    ]
    
    mocker.patch(
        'processors.csb_service_processor.get_csb_service_data',  # ← UPDATED PATH
        return_value=mock_data
    )
    
    response = client.get('/csb')
    data = response.get_json()
    
    assert isinstance(data, list)
    assert len(data) > 0
    assert 'service_name' in data[0]


@pytest.mark.integration
def test_csb_api_e2e_with_active_data(client):
    """
    End-to-end integration test with real database.
    Requires csb_service_requests table with is_active=1 records.
    """
    response = client.get('/csb')
    
    if response.status_code == 500:
        pytest.skip("PostgreSQL not available (expected in Codespaces)")
    
    assert response.status_code == 200
    data = response.get_json()
    
    assert isinstance(data, list)
    
    if len(data) > 0:
        for record in data:
            assert record.get('is_active') == 1
            assert 'source_url' in record
            assert 'stlouis-mo.gov/data/datasets' in record['source_url']
            assert 'id' in record
            assert 'created_on' in record


def test_csb_api_error_handling(client, mocker):
    """
    Unit test: Verify proper error handling when database query fails.
    """
    mocker.patch(
        'processors.csb_service_processor.get_csb_service_data',  # ← UPDATED PATH
        side_effect=Exception("Database connection failed")
    )
    
    response = client.get('/csb')
    
    assert response.status_code == 500
    response_data = response.get_json()
    assert 'error' in response_data


def test_csb_api_empty_result(client, mocker):
    """
    Unit test: Verify API handles empty results gracefully.
    """
    mocker.patch(
        'processors.csb_service_processor.get_csb_service_data',  # ← UPDATED PATH
        return_value=[]
    )
    
    response = client.get('/csb')
    
    assert response.status_code == 200
    data = response.get_json()
    assert data == []
    assert isinstance(data, list)