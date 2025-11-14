"""
1_normalize.py - Standardize columns and parse timestamps
"""
from pathlib import Path
import json
import logging
import pandas as pd
import dask
import dask.dataframe as dd
print("Dask version:", dask.__version__)

print("Pandas version:", pd.__version__)

from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
TMP = Path("./etl_tmp")
MANIFEST_PATH = TMP / "manifest.json"
NORMALIZED_ROOT = TMP / "normalized"
NORMALIZED_ROOT.mkdir(exist_ok=True)

# Column definitions
EXPECTED_COLUMNS = ["time", "open", "high", "low", "close", "volume"]

def extract_symbol_from_filename(filename):
    """
    Extract symbol from filename.
    Examples: 
        "AAPL_1m.csv" -> "AAPL"
        "TSLA_5m_data.csv" -> "TSLA"
    """
    stem = Path(filename).stem
    parts = stem.split('_')
    symbol = parts[0].upper()
    return symbol

def normalize_txt_or_csv(file_path, interval):
    """
    Read and normalize a single TXT/CSV file using Dask.
    Returns normalized Dask DataFrame or None on error.
    """
    try:
        symbol = extract_symbol_from_filename(file_path.name)
        logger.info(f"Processing {symbol} from {file_path.name}")
        df = dd.read_csv(
            str(file_path),
            header=None,
            names=EXPECTED_COLUMNS,
            parse_dates=["time"],
            assume_missing=True,
            blocksize="64MB",
            delimiter=","
        )
        df['symbol'] = symbol
        df['interval'] = interval
        df['time'] = dd.to_datetime(df['time'], utc=True, errors='coerce')
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = dd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['time', 'close'])
        df = df[df['volume'] >= 0]
        df = df[df['open'] > 0]
        df = df[df['high'] >= df['low']]
        df = df.sort_values('time')
        df = df.drop_duplicates(subset=['time', 'symbol'], keep='last')
        logger.info(f"Successfully normalized {symbol}")
        return df
    except Exception as e:
        logger.error(f"Error normalizing {file_path}: {e}")
        return None

def process_all_files():
    """Process all files from manifest."""
    if not MANIFEST_PATH.exists():
        logger.error(f"Manifest not found at {MANIFEST_PATH}")
        logger.error("Please run 0_ingest.py first")
        return []
    with open(MANIFEST_PATH) as f:
        manifest = json.load(f)
    logger.info(f"Processing {len(manifest['files'])} files from manifest")
    normalized_dfs = []
    stats = {'total': len(manifest['files']), 'success': 0, 'failed': 0}
    for file_info in manifest['files']:
        data_path = Path(file_info['path'])
        interval = file_info['interval']
        if not data_path.exists():
            logger.warning(f"File not found: {data_path}")
            stats['failed'] += 1
            continue
        df = normalize_txt_or_csv(data_path, interval)
        if df is not None:
            normalized_dfs.append(df)
            stats['success'] += 1
        else:
            stats['failed'] += 1
    logger.info("="*60)
    logger.info(f"Normalization complete:")
    logger.info(f"  Success: {stats['success']}/{stats['total']}")
    logger.info(f"  Failed: {stats['failed']}/{stats['total']}")
    logger.info("="*60)
    return normalized_dfs

def combine_and_save(normalized_dfs):
    """Combine all normalized DataFrames and save."""
    if not normalized_dfs:
        logger.warning("No DataFrames to combine!")
        return None
    logger.info("Combining all normalized DataFrames...")
    combined = dd.concat(normalized_dfs, axis=0, interleave_partitions=True)
    combined = combined.sort_values(['symbol', 'time'])
    output_path = NORMALIZED_ROOT / "combined"
    logger.info(f"Saving combined data to {output_path}")
    combined.to_parquet(
        str(output_path),
        engine='pyarrow',
        compression='snappy',
        write_index=False
    )
    row_count = len(combined)
    logger.info(f"Total rows in combined dataset: {row_count}")
    return combined

def main():
    """Main normalization pipeline."""
    logger.info("="*60)
    logger.info("Starting data normalization process")
    logger.info("="*60)
    normalized_dfs = process_all_files()
    if not normalized_dfs:
        logger.error("No data to process!")
        return
    combined = combine_and_save(normalized_dfs)
    logger.info("="*60)
    logger.info("Normalization complete!")
    logger.info("="*60)

if __name__ == "__main__":
    main()
