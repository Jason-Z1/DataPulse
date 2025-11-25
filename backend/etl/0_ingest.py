
"""
0_ingest.py - Unzip archives and create manifest

This script will look for the raw data directory `FirstData`.
It resolves the data root in the following order:
 1. Command-line argument `--data-root`
 2. Environment variable `FIRSTDATA_PATH`
 3. Repository-relative `FirstData` (two parents up from this file)
 4. Current working directory `FirstData` (legacy behavior)

If none of these exist the script will exit with a helpful message.
"""

from pathlib import Path
import zipfile
import json
import logging
import os
import argparse
from datetime import datetime
import pandas as pd


def resolve_raw_root(cli_path: str | None) -> Path:
    # 1) CLI argument
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        return p  # return even if missing so caller can show helpful msg

    # 2) Environment variable
    env = os.environ.get('FIRSTDATA_PATH')
    if env:
        p = Path(env)
        if p.exists():
            return p
        return p

    # 3) Repository-relative: file -> etl -> backend -> repo
    here = Path(__file__).resolve()
    repo_root = here.parents[2] if len(here.parents) >= 3 else here.parent
    p = repo_root / 'FirstData'
    if p.exists():
        return p

    # 4) Legacy: CWD-relative
    p = Path('FirstData')
    return p


# We'll compute RAW_ROOT inside main() after CLI parsing so the script is
# tolerant of different invocation working directories.

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
RAW_ROOT = Path('./backend/FirstData')
TMP = Path("./etl_tmp")
TMP.mkdir(exist_ok=True)
MANIFEST_PATH = TMP / "manifest.json"

OUTPUT_DIR = Path('etl_tmp/') # Directory to store the Parquet files
PARQUET_FILE = OUTPUT_DIR / "file_metadata.parquet" # The Parquet file to store metadata

def unzip_and_extract():
    # List to hold metadata for all the files
    file_metadata = []

    # Iterate over the interval folders (1hr, 5min, 1d)
    for interval_archive in RAW_ROOT.glob("*.zip"):
        # Extracts the contents of the archived
        with zipfile.ZipFile(interval_archive, 'r') as zip_ref:
            # Extract the contents of the archive
            interval_name = interval_archive.stem # e.g. "1hour", "5min"
            extracted_dir = OUTPUT_DIR / interval_name # Target extraction directory
            os.makedirs(extracted_dir, exist_ok=True)
            zip_ref.extractall(extracted_dir) # Extract the archived content

            download_dir = extracted_dir / "FirstData" / "Downloaded_March15"
            if(download_dir.exists() and download_dir.is_dir()):
                # Process all the stock company zip files inside "Download_march15"
                for company_zip in download_dir.glob("*zip"):
                    with zipfile.ZipFile(company_zip, 'r') as company_zip_ref:
                        # Extract the company files
                        company_target_dir = extracted_dir / "FirstData" / company_zip.stem
                        os.makedirs(company_target_dir, exist_ok=True)
                        company_zip_ref.extractall(company_target_dir)

                        # Process each extracted CSV file (assuming CSV format)
                        for extracted_file in company_target_dir.glob("*.csv"):
                            # Extract metadata: time interval, symbol, path, and file size
                            symbol = extracted_file.stem.split('_')[0]  # e.g., AAPL from AAPL_1hr.csv
                            path_from_root = extracted_file.relative_to(RAW_ROOT)  # Relative path from ROOT
                            byte_size = extracted_file.stat().st_size  # File size in bytes
                            
                            # Append metadata to the list
                            file_metadata.append({
                                "time_interval": interval_name,  # "1hr", "5min", etc.
                                "symbol": symbol,
                                "path_from_root": str(path_from_root),
                                "byte_size": byte_size
                            })
                            print(f"Processed: {symbol}, {interval_name}, {path_from_root}, {byte_size} bytes")
    
     # Convert the metadata list to a DataFrame
    df = pd.DataFrame(file_metadata)
    
    # Save the metadata DataFrame to a Parquet file
    df.to_parquet(PARQUET_FILE, index=False)
    print(f"Metadata saved to {PARQUET_FILE}")

"""
def discover_files():
    # Find all TXT, CSV files and ZIP archives in all interval subfolders, recursively.
    items = []
    for interval_dir in RAW_ROOT.iterdir():
        if not interval_dir.is_dir():
            continue
        # Find ZIP archives directly under interval dir
        for archive in interval_dir.rglob("*.zip"):
            items.append(('archive', interval_dir.name, archive))
        # Find .txt and .csv files recursively
        for file_path in interval_dir.rglob("*"):
            if file_path.name.lower().endswith((".csv", ".txt", ".csv.gz", ".txt.gz")):
                items.append(('raw', interval_dir.name, file_path))
    return items

def unzip_archive(archive_path, outdir):
    # Extract ZIP archive with error handling 
    try:
        with zipfile.ZipFile(archive_path) as z:
            bad_file = z.testzip()
            if bad_file:
                logger.error(f"Corrupted file in archive: {bad_file}")
                return False
            z.extractall(outdir)
            logger.info(f"Extracted {archive_path.name} to {outdir}")
            return True
    except zipfile.BadZipFile:
        logger.error(f"Bad ZIP file: {archive_path}")
        return False
    except Exception as e:
        logger.error(f"Error extracting {archive_path}: {e}")
        return False
"""

def main():
    """
    logger.info("="*60)
    logger.info("Starting data ingestion process")
    logger.info("="*60)

    items = discover_files()
    if not items:
        logger.warning("No TXT/CSV or ZIP files found!")
        return

    extracted_data = []
    manifest_files = []

    for item_type, interval, path in items:
        if item_type == 'archive':
            outdir = TMP / interval / path.stem
            outdir.mkdir(parents=True, exist_ok=True)
            success = unzip_archive(path, outdir)
            extracted_data.append((interval, path.name, outdir, success))
            if success:
                for file in Path(outdir).rglob("*"):
                    if file.suffix.lower() in [".csv", ".txt"]:
                        manifest_files.append({
                            "path": str(file),
                            "size_bytes": file.stat().st_size,
                            "interval": interval,
                            "archive": path.name,
                            "filename": file.name
                        })
        elif item_type == 'raw':
            manifest_files.append({
                "path": str(path),
                "size_bytes": path.stat().st_size,
                "interval": interval,
                "archive": None,
                "filename": path.name
            })

    # Write manifest
    manifest = {
        "created_at": datetime.now().isoformat(),
        "files": manifest_files
    }
    with open(MANIFEST_PATH, 'w') as f:
        json.dump(manifest, f, indent=2)
    logger.info(f"Manifest created with {len(manifest['files'])} files")
    logger.info(f"Manifest saved to: {MANIFEST_PATH}")

    logger.info("="*60)
    logger.info(f"Ingestion complete: {len(manifest_files)} files indexed")
    logger.info("="*60)
    """
    unzip_and_extract()

if __name__ == "__main__":
    main()
