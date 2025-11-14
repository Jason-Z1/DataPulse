"""
4_metadata.py - Build partition metadata and catalog
"""
from pathlib import Path
import logging
import sqlite3
import json
import pyarrow.parquet as pq
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
PARQUET_ROOT = Path("../data/parquet")
AGG_ROOT = Path("../data/parquet_agg")
CATALOG_PATH = Path("../data/catalog.sqlite")
METADATA_ROOT = Path("../data/metadata")
METADATA_ROOT.mkdir(parents=True, exist_ok=True)

def initialize_catalog_db():
    """Create SQLite catalog database with schema."""
    logger.info("Initializing catalog database...")
    
    conn = sqlite3.connect(CATALOG_PATH)
    cursor = conn.cursor()
    
    # Drop existing tables to ensure clean state
    cursor.execute("DROP TABLE IF EXISTS partitions")
    cursor.execute("DROP TABLE IF EXISTS symbols")
    cursor.execute("DROP TABLE IF EXISTS aggregations")
    
    # Main partitions table
    cursor.execute("""
        CREATE TABLE partitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            path TEXT NOT NULL,
            min_time TEXT NOT NULL,
            max_time TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            file_size_bytes INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(symbol, year, month, path)
        )
    """)
    
    # Symbols summary table
    cursor.execute("""
        CREATE TABLE symbols (
            symbol TEXT PRIMARY KEY,
            min_time TEXT NOT NULL,
            max_time TEXT NOT NULL,
            total_rows INTEGER NOT NULL,
            partition_count INTEGER NOT NULL,
            total_size_bytes INTEGER NOT NULL,
            last_updated TEXT NOT NULL
        )
    """)
    
    # Aggregations table
    cursor.execute("""
        CREATE TABLE aggregations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            interval TEXT NOT NULL,
            symbol TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            path TEXT NOT NULL,
            min_time TEXT NOT NULL,
            max_time TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            file_size_bytes INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(interval, symbol, year, month, path)
        )
    """)
    
    # Create indexes for fast lookups
    cursor.execute("CREATE INDEX idx_partitions_symbol ON partitions(symbol)")
    cursor.execute("CREATE INDEX idx_partitions_time ON partitions(min_time, max_time)")
    cursor.execute("CREATE INDEX idx_aggregations_interval ON aggregations(interval)")
    cursor.execute("CREATE INDEX idx_aggregations_symbol ON aggregations(symbol)")
    
    conn.commit()
    conn.close()
    
    logger.info(f"Catalog database created at: {CATALOG_PATH}")

def compute_metadata_for_parquet_file(parquet_file):
    """Extract metadata from a single Parquet file."""
    try:
        # Read only time column for efficiency
        table = pq.read_table(parquet_file, columns=['time'])
        times = table.column('time').to_pandas()
        
        if len(times) == 0:
            return None
        
        metadata = {
            'path': str(parquet_file),
            'min_time': str(times.min()),
            'max_time': str(times.max()),
            'row_count': len(times),
            'file_size_bytes': parquet_file.stat().st_size
        }
        
        return metadata
        
    except Exception as e:
        logger.error(f"Error reading {parquet_file}: {e}")
        return None

def parse_partition_path(parquet_file, root_path):
    """Parse symbol, year, month from Hive-style partition path."""
    try:
        relative = parquet_file.relative_to(root_path)
        parts = relative.parts
        
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
        
        return symbol, year, month
        
    except Exception as e:
        logger.error(f"Error parsing partition path {parquet_file}: {e}")
        return None, None, None

def catalog_main_partitions():
    """Catalog all main (raw) partitions."""
    logger.info("="*60)
    logger.info("Cataloging main partitions...")
    logger.info("="*60)
    
    if not PARQUET_ROOT.exists():
        logger.error(f"Parquet root not found: {PARQUET_ROOT}")
        return
    
    conn = sqlite3.connect(CATALOG_PATH)
    cursor = conn.cursor()
    
    parquet_files = list(PARQUET_ROOT.rglob("*.parquet"))
    logger.info(f"Found {len(parquet_files)} partition files")
    
    processed = 0
    for parquet_file in parquet_files:
        # Parse partition info
        symbol, year, month = parse_partition_path(parquet_file, PARQUET_ROOT)
        
        if not all([symbol, year, month]):
            logger.warning(f"Could not parse partition info: {parquet_file}")
            continue
        
        # Get metadata
        metadata = compute_metadata_for_parquet_file(parquet_file)
        
        if not metadata:
            continue
        
        # Insert into database
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO partitions 
                (symbol, year, month, path, min_time, max_time, row_count, file_size_bytes, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol,
                year,
                month,
                metadata['path'],
                metadata['min_time'],
                metadata['max_time'],
                metadata['row_count'],
                metadata['file_size_bytes'],
                datetime.now().isoformat()
            ))
            
            processed += 1
            
            if processed % 100 == 0:
                logger.info(f"  Processed {processed}/{len(parquet_files)} files...")
            
        except sqlite3.Error as e:
            logger.error(f"Database error for {parquet_file}: {e}")
    
    conn.commit()
    conn.close()
    
    logger.info(f"✓ Cataloged {processed} partition files")

def catalog_aggregations():
    """Catalog all aggregated partitions."""
    logger.info("="*60)
    logger.info("Cataloging aggregations...")
    logger.info("="*60)
    
    if not AGG_ROOT.exists():
        logger.warning(f"Aggregation root not found: {AGG_ROOT}")
        return
    
    conn = sqlite3.connect(CATALOG_PATH)
    cursor = conn.cursor()
    
    # Process each interval directory
    for interval_dir in AGG_ROOT.glob("interval=*"):
        interval = interval_dir.name.split('=')[1]
        logger.info(f"Processing interval: {interval}")
        
        parquet_files = list(interval_dir.rglob("*.parquet"))
        
        for parquet_file in parquet_files:
            # Parse partition info
            symbol, year, month = parse_partition_path(parquet_file, interval_dir)
            
            if not all([symbol, year, month]):
                continue
            
            # Get metadata
            metadata = compute_metadata_for_parquet_file(parquet_file)
            
            if not metadata:
                continue
            
            # Insert into database
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO aggregations 
                    (interval, symbol, year, month, path, min_time, max_time, row_count, file_size_bytes, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    interval,
                    symbol,
                    year,
                    month,
                    metadata['path'],
                    metadata['min_time'],
                    metadata['max_time'],
                    metadata['row_count'],
                    metadata['file_size_bytes'],
                    datetime.now().isoformat()
                ))
                
            except sqlite3.Error as e:
                logger.error(f"Database error for {parquet_file}: {e}")
        
        logger.info(f"  ✓ Cataloged {len(parquet_files)} files for {interval}")
    
    conn.commit()
    conn.close()

def build_symbol_summaries():
    """Build per-symbol summary statistics."""
    logger.info("="*60)
    logger.info("Building symbol summaries...")
    logger.info("="*60)
    
    conn = sqlite3.connect(CATALOG_PATH)
    cursor = conn.cursor()
    
    # Aggregate partition data by symbol
    cursor.execute("""
        INSERT OR REPLACE INTO symbols (symbol, min_time, max_time, total_rows, partition_count, total_size_bytes, last_updated)
        SELECT 
            symbol,
            MIN(min_time) as min_time,
            MAX(max_time) as max_time,
            SUM(row_count) as total_rows,
            COUNT(*) as partition_count,
            SUM(file_size_bytes) as total_size_bytes,
            ? as last_updated
        FROM partitions
        GROUP BY symbol
    """, (datetime.now().isoformat(),))
    
    conn.commit()
    
    # Get summary stats
    cursor.execute("SELECT COUNT(*) FROM symbols")
    symbol_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT SUM(total_rows), SUM(total_size_bytes) FROM symbols")
    total_rows, total_size = cursor.fetchone()
    
    conn.close()
    
    logger.info(f"✓ Created summaries for {symbol_count} symbols")
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info(f"  Total size: {total_size / (1024**3):.2f} GB")

def create_json_catalog():
    """Create JSON version of catalog for easy access."""
    logger.info("Creating JSON catalog...")
    
    conn = sqlite3.connect(CATALOG_PATH)
    
    # Export symbols
    symbols_df = conn.execute("SELECT * FROM symbols").fetchall()
    symbols_cols = [desc[0] for desc in conn.execute("SELECT * FROM symbols").description]
    
    symbols = []
    for row in symbols_df:
        symbols.append(dict(zip(symbols_cols, row)))
    
    catalog = {
        'created_at': datetime.now().isoformat(),
        'catalog_db': str(CATALOG_PATH),
        'symbols': symbols,
        'statistics': {
            'total_symbols': len(symbols),
            'total_partitions': conn.execute("SELECT COUNT(*) FROM partitions").fetchone()[0],
            'total_aggregations': conn.execute("SELECT COUNT(*) FROM aggregations").fetchone()[0]
        }
    }
    
    conn.close()
    
    # Save JSON
    catalog_json_path = METADATA_ROOT / "catalog.json"
    with open(catalog_json_path, 'w') as f:
        json.dump(catalog, f, indent=2)
    
    logger.info(f"JSON catalog saved to: {catalog_json_path}")

def print_catalog_summary():
    """Print human-readable catalog summary."""
    logger.info("="*60)
    logger.info("CATALOG SUMMARY")
    logger.info("="*60)
    
    conn = sqlite3.connect(CATALOG_PATH)
    cursor = conn.cursor()
    
    # Symbols
    cursor.execute("SELECT COUNT(*), SUM(total_rows), SUM(total_size_bytes) FROM symbols")
    symbol_count, total_rows, total_size = cursor.fetchone()
    
    logger.info(f"Symbols: {symbol_count}")
    logger.info(f"Total rows: {total_rows:,}")
    logger.info(f"Total size: {total_size / (1024**3):.2f} GB")
    
    # Partitions
    cursor.execute("SELECT COUNT(*) FROM partitions")
    partition_count = cursor.fetchone()[0]
    logger.info(f"Main partitions: {partition_count}")
    
    # Aggregations
    cursor.execute("SELECT interval, COUNT(*) FROM aggregations GROUP BY interval")
    agg_counts = cursor.fetchall()
    if agg_counts:
        logger.info("Aggregations:")
        for interval, count in agg_counts:
            logger.info(f"  {interval}: {count} partitions")
    
    # Sample symbols
    cursor.execute("SELECT symbol, total_rows FROM symbols ORDER BY total_rows DESC LIMIT 10")
    top_symbols = cursor.fetchall()
    logger.info("Top 10 symbols by row count:")
    for symbol, rows in top_symbols:
        logger.info(f"  {symbol}: {rows:,} rows")
    
    conn.close()
    
    logger.info("="*60)

def main():
    """Main metadata pipeline."""
    logger.info("="*60)
    logger.info("Starting metadata catalog creation")
    logger.info("="*60)
    
    # Initialize database
    initialize_catalog_db()
    
    # Catalog main partitions
    catalog_main_partitions()
    
    # Catalog aggregations
    catalog_aggregations()
    
    # Build summaries
    build_symbol_summaries()
    
    # Create JSON catalog
    create_json_catalog()
    
    # Print summary
    print_catalog_summary()
    
    logger.info("="*60)
    logger.info("Metadata catalog complete!")
    logger.info(f"Catalog database: {CATALOG_PATH}")
    logger.info("="*60)

if __name__ == "__main__":
    main()