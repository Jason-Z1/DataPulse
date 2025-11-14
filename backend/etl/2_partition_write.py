"""
2_partition_write.py - Write partitioned Parquet files
"""
from pathlib import Path
import logging
import pandas as pd
import dask
print("Pandas version:", pd.__version__)
print("Dask version:", dask.__version__)
import pyarrow as pa
import pyarrow.parquet as pq

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
TMP = Path("./etl_tmp")
NORMALIZED_ROOT = TMP / "normalized" / "combined"
OUT_ROOT = Path("../data/parquet")
OUT_ROOT.mkdir(parents=True, exist_ok=True)

def add_partition_columns(df):
    """Add year, month, day columns for partitioning."""
    logger.info("Adding partition columns...")
    
    df['year'] = df['time'].dt.year
    df['month'] = df['time'].dt.month
    df['day'] = df['time'].dt.day
    
    return df

def write_partitioned_data(df):
    """Write data with Hive-style partitioning."""
    logger.info("Writing partitioned Parquet files...")
    logger.info(f"Output directory: {OUT_ROOT}")
    
    try:
        # Write partitioned parquet
        df.to_parquet(
            str(OUT_ROOT),
            partition_on=['symbol', 'year', 'month'],
            engine='pyarrow',
            compression='snappy',
            write_index=False,
            overwrite=False,  # Prevent accidental overwrites
            name_function=lambda i: f"part-{i:05d}.parquet"
        )
        
        logger.info("Partitioned data written successfully")
        
        # Log partition structure
        partitions = list(OUT_ROOT.rglob("*.parquet"))
        logger.info(f"Created {len(partitions)} partition files")
        
        # Sample partition paths
        sample_partitions = sorted(partitions)[:5]
        logger.info("Sample partition structure:")
        for p in sample_partitions:
            relative = p.relative_to(OUT_ROOT)
            logger.info(f"  {relative}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error writing partitioned data: {e}")
        return False

def verify_partitions():
    """Verify partition structure and read sample data."""
    logger.info("Verifying partitions...")
    
    try:
        # Read back the partitioned data
        df = dd.read_parquet(
            str(OUT_ROOT),
            engine='pyarrow'
        )
        
        # Get statistics
        symbols = df['symbol'].unique().compute()
        date_range = df['time'].agg(['min', 'max']).compute()
        total_rows = len(df)
        
        logger.info("="*60)
        logger.info("Partition Verification:")
        logger.info(f"  Total symbols: {len(symbols)}")
        logger.info(f"  Date range: {date_range['min']} to {date_range['max']}")
        logger.info(f"  Total rows: {total_rows:,}")
        logger.info(f"  Sample symbols: {sorted(symbols)[:10]}")
        logger.info("="*60)
        
        return True
        
    except Exception as e:
        logger.error(f"Error verifying partitions: {e}")
        return False

def create_partition_summary():
    """Create a summary of all partitions."""
    logger.info("Creating partition summary...")
    
    summary = {
        'symbols': {},
        'total_files': 0,
        'total_size_bytes': 0
    }
    
    for parquet_file in OUT_ROOT.rglob("*.parquet"):
        # Parse partition info from path
        # Example: symbol=AAPL/year=2023/month=1/part-00000.parquet
        parts = parquet_file.relative_to(OUT_ROOT).parts
        
        symbol = None
        year = None
        month = None
        
        for part in parts:
            if part.startswith('symbol='):
                symbol = part.split('=')[1]
            elif part.startswith('year='):
                year = int(part.split('=')[1])
            elif part.startswith('month='):
                month = int(part.split('=')[1])
        
        if symbol:
            if symbol not in summary['symbols']:
                summary['symbols'][symbol] = {
                    'files': 0,
                    'size_bytes': 0,
                    'date_ranges': []
                }
            
            summary['symbols'][symbol]['files'] += 1
            file_size = parquet_file.stat().st_size
            summary['symbols'][symbol]['size_bytes'] += file_size
            
            if year and month:
                summary['symbols'][symbol]['date_ranges'].append(f"{year}-{month:02d}")
        
        summary['total_files'] += 1
        summary['total_size_bytes'] += parquet_file.stat().st_size
    
    # Log summary
    logger.info("="*60)
    logger.info("Partition Summary:")
    logger.info(f"  Total files: {summary['total_files']}")
    logger.info(f"  Total size: {summary['total_size_bytes'] / (1024**3):.2f} GB")
    logger.info(f"  Symbols: {len(summary['symbols'])}")
    logger.info("="*60)
    
    # Save summary
    import json
    summary_path = OUT_ROOT / "partition_summary.json"
    with open(summary_path, 'w') as f:
        # Convert sets to lists for JSON serialization
        for symbol_data in summary['symbols'].values():
            symbol_data['date_ranges'] = sorted(list(set(symbol_data['date_ranges'])))
        json.dump(summary, f, indent=2)
    
    logger.info(f"Summary saved to: {summary_path}")
    
    return summary

def main():
    """Main partitioning pipeline."""
    logger.info("="*60)
    logger.info("Starting data partitioning process")
    logger.info("="*60)
    
    # Check if normalized data exists
    if not NORMALIZED_ROOT.exists():
        logger.error(f"Normalized data not found at {NORMALIZED_ROOT}")
        logger.error("Please run 1_normalize.py first")
        return
    
    # Read normalized data
    logger.info(f"Reading normalized data from {NORMALIZED_ROOT}")
    df = dd.read_parquet(str(NORMALIZED_ROOT), engine='pyarrow')
    
    # Add partition columns
    df = add_partition_columns(df)
    
    # Write partitioned data
    success = write_partitioned_data(df)
    
    if not success:
        logger.error("Failed to write partitioned data!")
        return
    
    # Verify partitions
    verify_partitions()
    
    # Create summary
    create_partition_summary()
    
    logger.info("="*60)
    logger.info("Partitioning complete!")
    logger.info("="*60)

if __name__ == "__main__":
    main()