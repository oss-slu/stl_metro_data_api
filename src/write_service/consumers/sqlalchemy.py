from sqlalchemy import (
    create_engine, Column, Integer, String, JSON, Text, TIMESTAMP, func
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

Base = declarative_base()

DATABASE_URL = (
    f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
    f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
)

engine = create_engine(DATABASE_URL, echo=True, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

def get_or_create_table_class(table_name: str):
    """create a table class if it doesn't exist"""
    class DynamicTable(Base):
        __tablename__ = table_name
        id = Column(Integer, primary_key=True, autoincrement=True)
        pdf_id = Column(String, nullable=False)
        entities = Column(JSON, nullable=False)
        snippet = Column(Text)
        tables = Column(JSON)
        created_at = Column(TIMESTAMP, server_default=func.now())
    return DynamicTable



_dynamic_models = {}

def get_or_create_table_class(table_name: str):
    if table_name in _dynamic_models:
        return _dynamic_models[table_name]

    class DynamicTable(Base):
        __tablename__ = table_name
        id = Column(Integer, primary_key=True)
        pdf_id = Column(String, nullable=False)
        entities = Column(JSON, nullable=False)
        snippet = Column(Text)
        tables = Column(JSON)
        created_at = Column(TIMESTAMP, server_default=func.now())

    _dynamic_models[table_name] = DynamicTable
    return DynamicTable
