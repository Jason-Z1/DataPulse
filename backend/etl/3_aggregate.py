"""
3_aggregate.py - Precompute aggregates at different intervals
"""
from pathlib import Path
import logging
import pandas as pd
import dask
print("Pandas version:", pd.__version__)
print("Dask version:", dask.__version__)
from datetime import datetime
import dask.dataframe as dd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
PARQUET_ROOT = Path("../data/parquet")
AGG_ROOT = Path("../data/parquet_agg")
AGG_ROOT.mkdir(parents=True, exist_ok=True)

# Aggregation intervals to precompute
AGGREGATION_INTERVALS = {
    '5min': '5T',   # 5 minutes
    '15min': '15T', # 15 minutes
    '1h': '1H',     # 1 hour
    '4h': '4H',     # 4 hours
    '1d': '1D',     # 1 day
    '1w': '1W',     # 1 week
}

def resample_partition(partition_df, interval_str):
    """
    Resample a single partition (in-memory pandas DataFrame).
    This function is applied to each Dask partition.
    """
    if partition_df.empty:
        return partition_df
    
    # Set time as index
    partition_df = partition_df.set_index('time')
    
    # Resample using pandas
    resampled = partition_df.groupby('symbol').resample(interval_str).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).reset_index()
    
    # Drop rows where all OHLC values are NaN (no data in that period)
    resampled = resampled.dropna(subset=['open', 'high', 'low', 'close'], how='all')
    
    return resampled

def aggregate_symbol_data(symbol_df, target_interval_name, target_interval_str):
    """
    Aggregate data for a specific symbol at a target interval.
    """
    try:
        logger.info(f"  Aggregating to {target_interval_name}...")
        
        # Apply resampling to each partition
        aggregated = symbol_df.map_partitions(
            resample_partition,
            interval_str=target_interval_str,
            meta={
                'time': 'datetime64[ns, UTC]',
                'symbol': 'object',
                'open': 'float64',
                'high': 'float64',
                'low': 'float64',
                'close': 'float64',
                'volume': 'float64'
            }
        )
        
        # Add partition columns
        aggregated['year'] = aggregated['time'].dt.year
        aggregated['month'] = aggregated['time'].dt.month
        
        return aggregated
        
    except Exception as e:
        logger.error(f"Error aggregating to {target_interval_name}: {e}")
        return None

def process_all_aggregations():
    """Process all aggregation intervals."""
    logger.info("="*60)
    logger.info("Starting aggregation process")
    logger.info("="*60)
    
    # Check if partitioned data exists
    if not PARQUET_ROOT.exists():
        logger.error(f"Partitioned data not found at {PARQUET_ROOT}")
        logger.error("Please run 2_partition_write.py first")
        return
    
    # Read all data
    logger.info(f"Reading data from {PARQUET_ROOT}")
    df = dd.read_parquet(str(PARQUET_ROOT), engine='pyarrow')
    
    # Get list of symbols
    symbols = df['symbol'].unique().compute()
    logger.info(f"Found {len(symbols)} symbols to aggregate")
    
    # Process each aggregation interval
    for interval_name, interval_str in AGGREGATION_INTERVALS.items():
        logger.info("="*60)
        logger.info(f"Processing {interval_name} aggregation ({interval_str})")
        logger.info("="*60)
        
        output_path = AGG_ROOT / f"interval={interval_name}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        try:
            # Aggregate all symbols at once
            aggregated = aggregate_symbol_data(df, interval_name, interval_str)
            
            if aggregated is None:
                logger.error(f"Failed to aggregate {interval_name}")
                continue
            
            # Write aggregated data with partitioning
            logger.info(f"Writing {interval_name} data to {output_path}")
            aggregated.to_parquet(
                str(output_path),
                partition_on=['symbol', 'year', 'month'],
                engine='pyarrow',
                compression='snappy',
                write_index=False,
                name_function=lambda i: f"part-{i:05d}.parquet"
            )
            
            # Log statistics
            row_count = len(aggregated)
            logger.info(f"✓ {interval_name} aggregation complete: {row_count:,} rows")
            
        except Exception as e:
            logger.error(f"Error processing {interval_name}: {e}")
            continue
    
    logger.info("="*60)
    logger.info("All aggregations complete!")
    logger.info("="*60)

def verify_aggregations():
    """Verify all aggregations were created successfully."""
    logger.info("="*60)
    logger.info("Verifying aggregations...")
    logger.info("="*60)
    
    verification_results = {}
    
    for interval_name in AGGREGATION_INTERVALS.keys():
        interval_path = AGG_ROOT / f"interval={interval_name}"
        
        if not interval_path.exists():
            logger.warning(f"✗ {interval_name}: Directory not found")
            verification_results[interval_name] = False
            continue
        
        try:
            # Read and get basic stats
            df = dd.read_parquet(str(interval_path), engine='pyarrow')
            row_count = len(df)
            symbols = len(df['symbol'].unique().compute())
            date_range = df['time'].agg(['min', 'max']).compute()
            
            logger.info(f"✓ {interval_name}:")
            logger.info(f"    Rows: {row_count:,}")
            logger.info(f"    Symbols: {symbols}")
            logger.info(f"    Date range: {date_range['min']} to {date_range['max']}")
            
            verification_results[interval_name] = True
            
        except Exception as e:
            logger.error(f"✗ {interval_name}: Error reading data - {e}")
            verification_results[interval_name] = False
    
    # Summary
    total = len(AGGREGATION_INTERVALS)
    successful = sum(verification_results.values())
    
    logger.info("="*60)
    logger.info(f"Verification complete: {successful}/{total} aggregations successful")
    logger.info("="*60)
    
    return verification_results

def create_aggregation_summary():
    """Create summary of all aggregations."""
    import json
    
    logger.info("Creating aggregation summary...")
    
    summary = {
        'created_at': datetime.now().isoformat(),
        'intervals': {}
    }
    
    for interval_name in AGGREGATION_INTERVALS.keys():
        interval_path = AGG_ROOT / f"interval={interval_name}"
        
        if not interval_path.exists():
            continue
        
        try:
            df = dd.read_parquet(str(interval_path), engine='pyarrow')
            
            summary['intervals'][interval_name] = {
                'path': str(interval_path),
                'row_count': int(len(df)),
                'symbols': int(len(df['symbol'].unique().compute())),
                'file_count': len(list(interval_path.rglob("*.parquet"))),
                'total_size_mb': sum(
                    f.stat().st_size for f in interval_path.rglob("*.parquet")
                ) / (1024**2)
            }
            
        except Exception as e:
            logger.error(f"Error processing {interval_name} for summary: {e}")
    
    # Save summary
    summary_path = AGG_ROOT / "aggregation_summary.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"Summary saved to: {summary_path}")
    
    # Log summary
    logger.info("="*60)
    logger.info("Aggregation Summary:")
    for interval_name, data in summary['intervals'].items():
        logger.info(f"  {interval_name}:")
        logger.info(f"    Rows: {data['row_count']:,}")
        logger.info(f"    Files: {data['file_count']}")
        logger.info(f"    Size: {data['total_size_mb']:.2f} MB")
    logger.info("="*60)

def main():
    """Main aggregation pipeline."""
    # Process all aggregations
    process_all_aggregations()
    
    # Verify results
    verify_aggregations()
    
    # Create summary
    create_aggregation_summary()

if __name__ == "__main__":
    main()