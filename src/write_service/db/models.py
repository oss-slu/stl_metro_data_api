"""
models.py
----------
Defines the PostgreSQL tables for the write service using SQLAlchemy ORM.
Each class here represents one table in the database.
"""

from sqlalchemy import Column, Integer, String, Float, DateTime, text, UniqueConstraint
from .session import Base

class STLouisGovCrime(Base):
    """
    Represents a table of St. Louis crime data imported from Excel.

    Columns roughly match the standardized schema used by STL data portal.
    The table is automatically created if it doesn't exist yet.
    """
    __tablename__ = "stlouis_gov_crime"

    # Primary key: automatically incremented ID
    id = Column(Integer, primary_key=True)

    # Unique incident number (we use this to avoid duplicate inserts)
    incident_number = Column(String(64), unique=True, nullable=False)

    # Type of crime or offense
    offense = Column(String(128))

    # When it occurred (datetime format)
    occurred_at = Column(DateTime)

    # Neighborhood or region
    neighborhood = Column(String(128))

    # Ward number (integer, can be NULL)
    ward = Column(Integer)

    # Coordinates for map plotting
    latitude = Column(Float)
    longitude = Column(Float)

    # Where it came from (useful for tracking data lineage)
    raw_source = Column(String(128))

    # Timestamp automatically set when record is created
    created_at = Column(DateTime, server_default=text("NOW()"))

    # Ensures uniqueness on incident_number to avoid duplicates
    __table_args__ = (
        UniqueConstraint("incident_number", name="uq_stlcrime_incident_number"),
    )
