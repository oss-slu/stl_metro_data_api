"""
CSB Data Producer: Parses CSV files and sends records to Kafka.

Responsibility: Parse CSV, validate data, send to Kafka topic.
Input: CSV file paths from fetcher.
Output: Messages sent to Kafka topic 'csb-service-requests'.
"""

import csv
import json
import logging
from datetime import datetime
from kafka import KafkaProducer
from pathlib import Path

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'csb-service-requests'

def create_processor():
    """Create Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def parse_datetime(date_str):
    """Parse datetime from CSV."""
    if not date_str or not date_str.strip():
        return None
    
    date_str = date_str.strip()
    if '.' in date_str:
        date_str = date_str.split('.')[0]
    
    formats = ['%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M:%S', '%Y-%m-%d', '%m/%d/%Y']
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).isoformat()
        except ValueError:
            continue
    return None

def produce_csb_data(csv_files):
    """
    Parse CSV files and send records to Kafka.
    
    Args:
        csv_files: List of CSV file paths
    """
    processor = create_processor()
    total_sent = 0
    
    for csv_file in csv_files:
        logger.info(f"Processing: {csv_file}")
        
        with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            
            for idx, row in enumerate(reader, 1):
                try:
                    # Determine if active
                    is_active = 1
                    if (row.get('DATECANCELLED', '').strip() or 
                        row.get('DATETIMECLOSED', '').strip() or
                        row.get('STATUS', '').strip().upper() in ['CLOSED', 'CANCELLED', 'COMPLETED']):
                        is_active = 0
                    
                    # Create message payload
                    message = {
                        'request_id': row.get('REQUESTID', '').strip(),
                        'service_name': row.get('PROBLEMCODE', '') or row.get('GROUP', 'Unknown'),
                        'description': row.get('DESCRIPTION', '') or row.get('EXPLANATION', ''),
                        'is_active': is_active,
                        'data_posted_on': parse_datetime(row.get('DATETIMEINIT', '')),
                        'contact_info': {
                            'caller_type': row.get('CALLERTYPE', '').strip(),
                            'neighborhood': row.get('NEIGHBORHOOD', '').strip(),
                            'ward': row.get('WARD', '').strip(),
                            'address': row.get('REQADDRESS', '').strip(),
                            'status': row.get('STATUS', '').strip()
                        },
                        'source_url': 'https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5',
                        'raw_data': dict(row)
                    }
                    
                    # Send to Kafka
                    processor.send(KAFKA_TOPIC, value=message)
                    total_sent += 1
                    
                    if total_sent % 5000 == 0:
                        processor.flush()
                        logger.info(f"Sent {total_sent:,} messages to Kafka...")
                        
                except Exception as e:
                    logger.error(f"Error processing row {idx}: {e}")
                    continue
        
        logger.info(f"✓ Completed: {csv_file.name}")
    
    processor.flush()
    processor.close()
    logger.info(f"✓ Total messages sent to Kafka: {total_sent:,}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from pathlib import Path
    
    # Example: process files from fetcher
    csv_files = list(Path("data/downloads").glob("*.csv"))
    if csv_files:
        produce_csb_data(csv_files)
    else:
        print("No CSV files found. Run fetcher first!")