# src/read_service/processors/business_citizen_processor.py

"""
Business/Citizen data processor for read operations.
Queries active records from stlouis_business_citizen table.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session


def get_business_citizen_data(session: Session):
    """
    Retrieve active business/citizen service records from St. Louis services.
    
    Queries only records where is_active=1 (active services).
    
    Args:
        session: SQLAlchemy database session
        
    Returns:
        List of dictionaries containing active business/citizen records
        with source URLs included.
        
    Source: https://www.stlouis-mo.gov/services/
    """
    try:
        # Query only active records
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
            FROM stlouis_business_citizen
            WHERE is_active = 1
            ORDER BY data_posted_on DESC
        """))
        
        # Convert rows to dictionaries and add source attribution
        records = []
        for row in result:
            record = dict(row._mapping)
            # Ensure source URL is included
            if not record.get('source_url'):
                record['source_url'] = 'https://www.stlouis-mo.gov/services/'
            records.append(record)
        
        return records
        
    except Exception as e:
        # Log error and re-raise
        raise Exception(f"Failed to query business/citizen data: {str(e)}")