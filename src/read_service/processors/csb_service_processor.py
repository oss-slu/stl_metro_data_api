# src/read_service/processors/csb_service_processor.py

"""
CSB Service Request processor for read operations.
Queries active records from csb_service_requests table.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session


def get_csb_service_data(session: Session):
    """
    Retrieve active CSB (Citizens' Service Bureau) 311 service requests.
    
    Queries only records where is_active=1 (active service requests).
    
    Args:
        session: SQLAlchemy database session
        
    Returns:
        List of dictionaries containing active CSB service request records
        with source URLs included.
        
    Source: https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5
    Data: https://www.stlouis-mo.gov/data/upload/data-files/csb.zip
    """
    try:
        # Query only active records from csb_service_requests table
        result = session.execute(text("""
            SELECT 
                id,
                created_on,
                data_posted_on,
                is_active,
                service_name,
                contact_info,
                description,
                source_url,
                ingested_at
            FROM csb_service_requests
            WHERE is_active = 1
            ORDER BY data_posted_on DESC
        """))
        
        # Convert rows to dictionaries and ensure source attribution
        records = []
        for row in result:
            record = dict(row._mapping)
            # Ensure source URL is included
            if not record.get('source_url'):
                record['source_url'] = 'https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5'
            records.append(record)
        
        return records
        
    except Exception as e:
        # Log error and re-raise
        raise Exception(f"Failed to query CSB service data: {str(e)}")