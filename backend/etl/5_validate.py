"""
5_validate.py - Data quality and correctness validation
"""
from pathlib import Path
import logging
import sqlite3
import pandas as pd
import dask
print("Pandas version:", pd.__version__)
print("Dask version:", dask.__version__)
from datetime import datetime
import json

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
VALIDATION_ROOT = Path("../data/validation")
VALIDATION_ROOT.mkdir(parents=True, exist_ok=True)

class ValidationReport:
    """Track validation results."""
    
    def __init__(self):
        self.checks = []
        self.errors = []
        self.warnings = []
        
    def add_check(self, name, passed, message=""):
        self.checks.append({
            'name': name,
            'passed': passed,
            'message': message,
            'timestamp': datetime.now().isoformat()
        })
        
        if not passed:
            self.errors.append({'check': name, 'message': message})
        
        status = "✓" if passed else "✗"
        level = logging.INFO if passed else logging.ERROR
        logger.log(level, f"{status} {name}: {message}")
    
    def add_warning(self, check, message):
        self.warnings.append({'check': check, 'message': message})
        logger.warning(f"⚠ {check}: {message}")
    
    def save(self, filename):
        """Save report as JSON."""
        report = {
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_checks': len(self.checks),
                'passed': sum(1 for c in self.checks if c['passed']),
                'failed': sum(1 for c in self.checks if not c['passed']),
                'warnings': len(self.warnings)
            },
            'checks': self.checks,
            'errors': self.errors,
            'warnings': self.warnings
        }
        
        path = VALIDATION_ROOT / filename
        with open(path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Validation report saved to: {path}")
        return report

def validate_catalog_exists(report):
    """Check if catalog database exists and is accessible."""
    logger.info("="*60)
    logger.info("Validating catalog...")
    logger.info("="*60)
    
    if not CATALOG_PATH.exists():
        report.add_check('catalog_exists', False, f"Catalog not found at {CATALOG_PATH}")
        return False
    
    try:
        conn = sqlite3.connect(CATALOG_PATH)
        cursor = conn.cursor()
        
        # Check tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        expected_tables = ['partitions', 'symbols', 'aggregations']
        missing_tables = [t for t in expected_tables if t not in tables]
        
        if missing_tables:
            report.add_check('catalog_schema', False, f"Missing tables: {missing_tables}")
            conn.close()
            return False
        
        report.add_check('catalog_exists', True, f"Found {len(tables)} tables")
        conn.close()
        return True
        
    except Exception as e:
        report.add_check('catalog_access', False, f"Error accessing catalog: {e}")
        return False

def validate_row_counts(report):
    """Validate row counts match between catalog and actual data."""
    logger.info("="*60)
    logger.info("Validating row counts...")
    logger.info("="*60)
    
    try:
        conn = sqlite3.connect(CATALOG_PATH)
        cursor = conn.cursor()
        
        # Get catalog row counts
        cursor.execute("SELECT symbol, SUM(row_count) FROM partitions GROUP BY symbol")
        catalog_counts = dict(cursor.fetchall())
        
        conn.close()
        
        # Read actual data
        df = dd.read_parquet(str(PARQUET_ROOT), engine='pyarrow')
        actual_counts = df.groupby('symbol').size().compute().to_dict()
        
        # Compare
        mismatches = []
        for symbol in catalog_counts:
            catalog_count = catalog_counts.get(symbol, 0)
            actual_count = actual_counts.get(symbol, 0)
            
            if catalog_count != actual_count:
                mismatches.append({
                    'symbol': symbol,
                    'catalog': catalog_count,
                    'actual': actual_count,
                    'diff': actual_count - catalog_count
                })
        
        if mismatches:
            report.add_check('row_counts_match', False, 
                           f"{len(mismatches)} symbols have mismatched counts")
            for m in mismatches[:5]:  # Log first 5
                report.add_warning('row_count_mismatch', 
                                 f"{m['symbol']}: catalog={m['catalog']}, actual={m['actual']}")
        else:
            report.add_check('row_counts_match', True, 
                           f"All {len(catalog_counts)} symbols match")
        
        return len(mismatches) == 0
        
    except Exception as e:
        report.add_check('row_counts_validation', False, f"Error: {e}")
        return False

def validate_time_monotonicity(report):
    """Check that timestamps are monotonically increasing per symbol."""
    logger.info("="*60)
    logger.info("Validating time monotonicity...")
    logger.info("="*60)
    
    try:
        df = dd.read_parquet(str(PARQUET_ROOT), engine='pyarrow')
        
        # Sample symbols to check
        symbols = df['symbol'].unique().compute()
        sample_size = min(10, len(symbols))
        sample_symbols = symbols[:sample_size]
        
        violations = []
        
        for symbol in sample_symbols:
            symbol_df = df[df['symbol'] == symbol][['time']].compute()
            
            # Check if sorted
            if not symbol_df['time'].is_monotonic_increasing:
                violations.append(symbol)
                
                # Find where it breaks
                diffs = symbol_df['time'].diff()
                negative_diffs = diffs[diffs < pd.Timedelta(0)]
                
                if len(negative_diffs) > 0:
                    report.add_warning('time_monotonicity', 
                                     f"{symbol}: {len(negative_diffs)} out-of-order timestamps")
        
        if violations:
            report.add_check('time_monotonicity', False, 
                           f"{len(violations)}/{sample_size} sampled symbols have non-monotonic times")
        else:
            report.add_check('time_monotonicity', True, 
                           f"All {sample_size} sampled symbols have monotonic times")
        
        return len(violations) == 0
        
    except Exception as e:
        report.add_check('time_monotonicity', False, f"Error: {e}")
        return False

def validate_no_duplicates(report):
    """Check for duplicate (symbol, time) pairs."""
    logger.info("="*60)
    logger.info("Validating no duplicates...")
    logger.info("="*60)
    
    try:
        df = dd.read_parquet(str(PARQUET_ROOT), engine='pyarrow')
        
        # Count total rows
        total_rows = len(df)
        
        # Count unique (symbol, time) pairs
        unique_pairs = len(df[['symbol', 'time']].drop_duplicates())
        
        duplicates = total_rows - unique_pairs
        
        if duplicates > 0:
            report.add_check('no_duplicates', False, 
                           f"Found {duplicates} duplicate (symbol, time) pairs")
        else:
            report.add_check('no_duplicates', True, 
                           f"No duplicates in {total_rows:,} rows")
        
        return duplicates == 0
        
    except Exception as e:
        report.add_check('no_duplicates', False, f"Error: {e}")
        return False

def validate_data_ranges(report):
    """Validate data is within reasonable ranges."""
    logger.info("="*60)
    logger.info("Validating data ranges...")
    logger.info("="*60)
    
    try:
        df = dd.read_parquet(str(PARQUET_ROOT), engine='pyarrow')
        
        # Sample data for validation
        sample = df.head(100000, npartitions=-1)
        
        issues = []
        
        # Check prices are positive
        if (sample['open'] <= 0).any().compute():
            issues.append("Found non-positive open prices")
        
        if (sample['high'] <= 0).any().compute():
            issues.append("Found non-positive high prices")
        
        if (sample['low'] <= 0).any().compute():
            issues.append("Found non-positive low prices")
        
        if (sample['close'] <= 0).any().compute():
            issues.append("Found non-positive close prices")
        
        # Check volume is non-negative
        if (sample['volume'] < 0).any().compute():
            issues.append("Found negative volume")
        
        # Check high >= low
        if (sample['high'] < sample['low']).any().compute():
            issues.append("Found high < low violations")
        
        # Check OHLC relationships
        if (sample['high'] < sample['open']).any().compute():
            issues.append("Found high < open violations")
        
        if (sample['high'] < sample['close']).any().compute():
            issues.append("Found high < close violations")
        
        if (sample['low'] > sample['open']).any().compute():
            issues.append("Found low > open violations")
        
        if (sample['low'] > sample['close']).any().compute():
            issues.append("Found low > close violations")
        
        if issues:
            report.add_check('data_ranges', False, f"Found {len(issues)} issues")
            for issue in issues:
                report.add_warning('data_range_violation', issue)
        else:
            report.add_check('data_ranges', True, "All data ranges valid")
        
        return len(issues) == 0
        
    except Exception as e:
        report.add_check('data_ranges', False, f"Error: {e}")
        return False

def validate_partition_coverage(report):
    """Check that all expected partitions exist."""
    logger.info("="*60)
    logger.info("Validating partition coverage...")
    logger.info("="*60)
    
    try:
        conn = sqlite3.connect(CATALOG_PATH)
        cursor = conn.cursor()
        
        # Get date range per symbol from catalog
        cursor.execute("""
            SELECT symbol, min_time, max_time, COUNT(*) as partition_count
            FROM partitions
            GROUP BY symbol
        """)
        
        symbols_data = cursor.fetchall()
        conn.close()
        
        gaps = []
        
        for symbol, min_time, max_time, partition_count in symbols_data:
            # Parse dates
            min_date = pd.to_datetime(min_time)
            max_date = pd.to_datetime(max_time)
            
            # Calculate expected number of month partitions
            months_diff = (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month) + 1
            
            # Allow some tolerance (gaps are okay in real data)
            if partition_count < months_diff * 0.5:  # Less than 50% coverage
                gaps.append({
                    'symbol': symbol,
                    'expected_months': months_diff,
                    'actual_partitions': partition_count,
                    'coverage': f"{(partition_count/months_diff*100):.1f}%"
                })
        
        if gaps:
            report.add_warning('partition_coverage', 
                             f"{len(gaps)} symbols have low partition coverage")
            for gap in gaps[:5]:  # Log first 5
                logger.warning(f"  {gap['symbol']}: {gap['coverage']} coverage")
        
        report.add_check('partition_coverage', len(gaps) == 0, 
                       f"Checked {len(symbols_data)} symbols")
        
        return len(gaps) == 0
        
    except Exception as e:
        report.add_check('partition_coverage', False, f"Error: {e}")
        return False

def validate_aggregations(report):
    """Validate aggregated data consistency."""
    logger.info("="*60)
    logger.info("Validating aggregations...")
    logger.info("="*60)
    
    if not AGG_ROOT.exists():
        report.add_warning('aggregations', "Aggregation directory not found")
        return True
    
    try:
        # Check each interval directory
        intervals = [d.name.split('=')[1] for d in AGG_ROOT.glob("interval=*")]
        
        if not intervals:
            report.add_check('aggregations_exist', False, "No aggregations found")
            return False
        
        for interval in intervals:
            interval_path = AGG_ROOT / f"interval={interval}"
            
            try:
                df = dd.read_parquet(str(interval_path), engine='pyarrow')
                row_count = len(df)
                symbol_count = len(df['symbol'].unique().compute())
                
                report.add_check(f'aggregation_{interval}', True, 
                               f"{row_count:,} rows, {symbol_count} symbols")
                
            except Exception as e:
                report.add_check(f'aggregation_{interval}', False, f"Error: {e}")
        
        return True
        
    except Exception as e:
        report.add_check('aggregations_validation', False, f"Error: {e}")
        return False

def validate_file_integrity(report):
    """Check for corrupted or empty Parquet files."""
    logger.info("="*60)
    logger.info("Validating file integrity...")
    logger.info("="*60)
    
    try:
        parquet_files = list(PARQUET_ROOT.rglob("*.parquet"))
        
        corrupted = []
        empty = []
        
        for pf in parquet_files:
            # Check file size
            if pf.stat().st_size == 0:
                empty.append(str(pf))
                continue
            
            # Try to read
            try:
                import pyarrow.parquet as pq
                table = pq.read_table(pf)
                if table.num_rows == 0:
                    empty.append(str(pf))
            except Exception:
                corrupted.append(str(pf))
        
        if corrupted:
            report.add_check('file_integrity', False, 
                           f"{len(corrupted)} corrupted files")
            for cf in corrupted[:5]:
                report.add_warning('corrupted_file', cf)
        elif empty:
            report.add_check('file_integrity', False, 
                           f"{len(empty)} empty files")
        else:
            report.add_check('file_integrity', True, 
                           f"All {len(parquet_files)} files are valid")
        
        return len(corrupted) == 0 and len(empty) == 0
        
    except Exception as e:
        report.add_check('file_integrity', False, f"Error: {e}")
        return False

def print_validation_summary(report):
    """Print human-readable validation summary."""
    logger.info("="*60)
    logger.info("VALIDATION SUMMARY")
    logger.info("="*60)
    
    total = len(report.checks)
    passed = sum(1 for c in report.checks if c['passed'])
    failed = total - passed
    
    logger.info(f"Total checks: {total}")
    logger.info(f"Passed: {passed} ({passed/total*100:.1f}%)")
    logger.info(f"Failed: {failed} ({failed/total*100:.1f}%)")
    logger.info(f"Warnings: {len(report.warnings)}")
    
    if failed > 0:
        logger.error("="*60)
        logger.error("FAILED CHECKS:")
        for error in report.errors:
            logger.error(f"  • {error['check']}: {error['message']}")
    
    if report.warnings:
        logger.warning("="*60)
        logger.warning("WARNINGS:")
        for warning in report.warnings[:10]:  # Show first 10
            logger.warning(f"  • {warning['check']}: {warning['message']}")
    
    logger.info("="*60)
    
    if failed == 0:
        logger.info("✓ All validations passed!")
    else:
        logger.error("✗ Some validations failed. Review errors above.")
    
    logger.info("="*60)

def main():
    """Main validation pipeline."""
    logger.info("="*60)
    logger.info("Starting data validation")
    logger.info("="*60)
    
    report = ValidationReport()
    
    # Run all validations
    validate_catalog_exists(report)
    validate_file_integrity(report)
    validate_row_counts(report)
    validate_time_monotonicity(report)
    validate_no_duplicates(report)
    validate_data_ranges(report)
    validate_partition_coverage(report)
    validate_aggregations(report)
    
    # Print summary
    print_validation_summary(report)
    
    # Save report
    report.save('validation_report.json')
    
    logger.info("="*60)
    logger.info("Validation complete!")
    logger.info("="*60)

if __name__ == "__main__":
    main()