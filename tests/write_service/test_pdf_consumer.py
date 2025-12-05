# test_pdf_consumer.py
import json
import pytest
from unittest.mock import MagicMock

from src.write_service.processing import pdf_processor
from src.write_service.ingestion import pdf_fetcher

PDF_URL = "https://www.stlouis-mo.gov/government/departments/human-services/homeless-services/documents/upload/Revised-2012-ESG-Action-Plan.pdf"
PDF_ID = "stlouis_gov_crime:12345"

@pytest.fixture
def fake_producer():
    """Mock Kafka producer"""
    producer = MagicMock()
    producer.send = MagicMock(return_value="ok")
    return producer

@pytest.fixture
def fake_session():
    """Mock SQLAlchemy session"""
    session = MagicMock()
    session.begin = MagicMock()
    session.commit = MagicMock()
    session.rollback = MagicMock()
    return session

@pytest.mark.integration
def test_process_pdf_and_insert_tables(fake_session):
    # Extract text pages from the PDF
    pages = pdf_fetcher.extract_text_from_pdf(PDF_URL, is_url=True)
    full_text = "\n".join(pages)

    # Parse tables
    tables = pdf_processor.parse_tables_from_text(full_text)
    print(f"Found {len(tables)} tables")
    for i, table in enumerate(tables, start=1):
        print(f"\nTable {i}:")
        for row in table:
            print(row)

    # Build payload
    entities = pdf_processor.extract_entities(full_text)
    payload_bytes = pdf_processor.build_payload(PDF_ID, entities, snippet=full_text[:300], tables=tables)

    # Handle message with mocked session
    from src.write_service.consumers.pdf_consumer import handle_message as consumer_handle_message
    result = consumer_handle_message(fake_session, payload_bytes)
    print("Consumer insert result:", result)

    # Assertions
    assert result["ok"] is True
    assert result["pdf_id"] == PDF_ID
    assert "table" in result
    assert fake_session.commit.called