"""
CSB Data Fetcher: Downloads ZIP file from St. Louis Open Data portal.

Responsibility: Download raw data files only.
Output: List of CSV file paths.
"""

import requests
import zipfile
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

ZIP_URL = "https://www.stlouis-mo.gov/data/upload/data-files/csb.zip"
DOWNLOAD_DIR = Path("data/downloads")

def fetch_csb_data():
    """
    Download and extract CSB service request data.
    
    Returns:
        list: Paths to extracted CSV files
    """
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Fetching CSB data from {ZIP_URL}")
    
    # Download ZIP
    zip_path = DOWNLOAD_DIR / "csb_data.zip"
    response = requests.get(ZIP_URL, stream=True)
    response.raise_for_status()
    
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    logger.info(f"Downloaded to {zip_path}")
    
    # Extract CSVs
    csv_files = []
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            if file_name.endswith('.csv'):
                zip_ref.extract(file_name, DOWNLOAD_DIR)
                csv_path = DOWNLOAD_DIR / file_name
                csv_files.append(csv_path)
                logger.info(f"Extracted: {file_name}")
    
    logger.info(f"✓ Fetched {len(csv_files)} CSV files")
    return csv_files

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    files = fetch_csb_data()
    print(f"Downloaded files: {files}")