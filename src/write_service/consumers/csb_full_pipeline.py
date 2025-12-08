"""
Complete CSB data pipeline: Download → Process → Store in one command.
Marks old data inactive and adds new data as active.
"""

import csv
import json
import requests
import zipfile
import os
import tempfile
import shutil
from datetime import datetime, timezone
from models import get_db_engine, CSBServiceRequest
from sqlalchemy.orm import sessionmaker
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ZIP_URL = "https://www.stlouis-mo.gov/data/upload/data-files/csb.zip"

def download_and_extract_csv(url):
    """Download ZIP and extract CSV to temporary directory."""
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp(prefix='csb_data_')
    logger.info(f"📁 Created temp directory: {temp_dir}")
    logger.info(f"📥 Downloading ZIP from {url}")
    
    try:
        # Download ZIP to temp directory
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        zip_path = os.path.join(temp_dir, "csb_data.zip")
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"✓ Downloaded to {zip_path}")
        
        # Extract CSV from ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            logger.info(f"📦 Files in ZIP: {file_list}")
            
            # Extract all files to temp directory
            zip_ref.extractall(temp_dir)
            
            # Find the CSV file
            csv_file = next((f for f in file_list if f.endswith('.csv')), None)
            
            if not csv_file:
                raise Exception("No CSV file found in ZIP")
            
            csv_path = os.path.join(temp_dir, csv_file)
            logger.info(f"✓ Extracted CSV: {csv_path}")
            
            return temp_dir, csv_path
            
    except Exception as e:
        # Clean up temp directory on error
        shutil.rmtree(temp_dir, ignore_errors=True)
        logger.error(f"❌ Download failed, cleaned up temp directory")
        raise

def parse_datetime(date_str):
    """Parse datetime from CSB data."""
    if not date_str or date_str.strip() == '':
        return None
    
    date_str = date_str.strip()
    if '.' in date_str:
        date_str = date_str.split('.')[0]
    
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%m/%d/%Y %H:%M:%S',
        '%Y-%m-%d',
        '%m/%d/%Y'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None

def process_and_store(csv_file, session):
    """Process CSV and store in database."""
    logger.info(f"📊 Processing CSV: {csv_file}")
    
    # Mark all existing records as inactive
    logger.info("⏸️  Marking existing records as inactive...")
    updated = session.query(CSBServiceRequest).update({"is_active": 0})
    session.commit()
    logger.info(f"   Marked {updated} records inactive")
    
    inserted = 0
    skipped = 0
    batch = []
    BATCH_SIZE = 1000
    
    with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.DictReader(f)
        
        for idx, row in enumerate(reader, 1):
            try:
                # Determine if active
                date_cancelled = row.get('DATECANCELLED', '').strip()
                date_closed = row.get('DATETIMECLOSED', '').strip()
                status = row.get('STATUS', '').strip().upper()
                
                is_active = 1 if not (date_cancelled or date_closed or status in ['CLOSED', 'CANCELLED', 'COMPLETED']) else 0
                
                service_name = row.get('PROBLEMCODE', '') or row.get('GROUP', 'Unknown Service')
                if not service_name.strip() or service_name.strip() == 'Unknown Service':
                    skipped += 1
                    continue
                
                contact_info = {
                    "caller_type": row.get('CALLERTYPE', '').strip(),
                    "neighborhood": row.get('NEIGHBORHOOD', '').strip(),
                    "ward": row.get('WARD', '').strip(),
                    "address": row.get('REQADDRESS', '').strip(),
                    "status": row.get('STATUS', '').strip(),
                    "request_id": row.get('REQUESTID', '').strip()
                }
                
                record = CSBServiceRequest(
                    created_on=datetime.now(timezone.utc),
                    data_posted_on=parse_datetime(row.get('DATETIMEINIT', '')),
                    is_active=is_active,
                    service_name=service_name.strip()[:255],
                    description=(row.get('DESCRIPTION', '') or row.get('EXPLANATION', '')).strip()[:1000],
                    contact_info=contact_info,
                    source_url='https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5',
                    raw_json=dict(row)
                )
                
                batch.append(record)
                inserted += 1
                
                if len(batch) >= BATCH_SIZE:
                    session.bulk_save_objects(batch)
                    session.commit()
                    batch = []
                    if idx % 5000 == 0:
                        logger.info(f"   Processed {idx:,} rows...")
                    
            except Exception as e:
                logger.error(f"Error on row {idx}: {e}")
                skipped += 1
        
        # Insert remaining
        if batch:
            session.bulk_save_objects(batch)
            session.commit()
    
    active_count = session.query(CSBServiceRequest).filter_by(is_active=1).count()
    inactive_count = session.query(CSBServiceRequest).filter_by(is_active=0).count()
    
    logger.info(f"✓ Pipeline complete!")
    logger.info(f"  Total inserted: {inserted:,}")
    logger.info(f"  Skipped: {skipped:,}")
    logger.info(f"  Active records: {active_count:,}")
    logger.info(f"  Inactive records: {inactive_count:,}")

def run_pipeline():
    """Run the complete CSB data pipeline."""
    temp_dir = None
    try:
        engine = get_db_engine()
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Download and extract to temp directory
        temp_dir, csv_file = download_and_extract_csv(ZIP_URL)
        
        # Process and store
        process_and_store(csv_file, session)
        
        session.close()
        logger.info("🎉 Pipeline complete!")
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise
    finally:
        # Always clean up temp directory
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.info(f"🧹 Cleaned up temp directory: {temp_dir}")

if __name__ == "__main__":
    run_pipeline()