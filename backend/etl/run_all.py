"""
run_all.py - Master ETL pipeline runner
Execute all ETL steps in sequence with error handling and monitoring
"""
import subprocess
import sys
import logging
from pathlib import Path
from datetime import datetime
import time


# Setup logging
log_dir = Path("./logs")
log_dir.mkdir(exist_ok=True)

log_file = log_dir / f"etl_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ETL Scripts in execution order
ETL_SCRIPTS = [
    ('0_ingest.py', 'Data Ingestion'),
    ('1_normalize.py', 'Data Normalization'),
    ('2_partition_write.py', 'Partition Writing'),
    ('3_aggregate.py', 'Data Aggregation'),
    ('4_metadata.py', 'Metadata Catalog'),
    ('5_validate.py', 'Data Validation')
]

class ETLRunner:
    """Manages ETL pipeline execution."""
    
    def __init__(self):
        self.results = []
        self.start_time = None
        self.end_time = None
    
    def run_script(self, script_name, description):
        """Execute a single ETL script."""
        logger.info("="*80)
        logger.info(f"STARTING: {description} ({script_name})")
        logger.info("="*80)
        
        script_path = Path(__file__).parent / script_name
        
        if not script_path.exists():
            logger.error(f"Script not found: {script_path}")
            return False
        
        start_time = time.time()
        
        try:
            # Run the script
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                check=True
            )
            
            # Log output
            if result.stdout:
                logger.info(result.stdout)
            
            elapsed = time.time() - start_time
            logger.info(f"✓ {description} completed in {elapsed:.2f}s")
            
            self.results.append({
                'script': script_name,
                'description': description,
                'success': True,
                'elapsed_seconds': elapsed,
                'error': None
            })
            
            return True
            
        except subprocess.CalledProcessError as e:
            elapsed = time.time() - start_time
            logger.error(f"✗ {description} failed after {elapsed:.2f}s")
            logger.error(f"Exit code: {e.returncode}")
            
            if e.stdout:
                logger.error(f"STDOUT:\n{e.stdout}")
            if e.stderr:
                logger.error(f"STDERR:\n{e.stderr}")
            
            self.results.append({
                'script': script_name,
                'description': description,
                'success': False,
                'elapsed_seconds': elapsed,
                'error': str(e)
            })
            
            return False
        
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"✗ {description} encountered unexpected error: {e}")
            
            self.results.append({
                'script': script_name,
                'description': description,
                'success': False,
                'elapsed_seconds': elapsed,
                'error': str(e)
            })
            
            return False
    
    def run_all(self, stop_on_error=True):
        """Run all ETL scripts in sequence."""
        logger.info("="*80)
        logger.info("ETL PIPELINE STARTING")
        logger.info(f"Log file: {log_file}")
        logger.info("="*80)
        
        self.start_time = time.time()
        
        for script_name, description in ETL_SCRIPTS:
            success = self.run_script(script_name, description)
            
            if not success and stop_on_error:
                logger.error("="*80)
                logger.error(f"Pipeline stopped due to error in {description}")
                logger.error("="*80)
                break
            
            # Small pause between scripts
            time.sleep(1)
        
        self.end_time = time.time()
        self.print_summary()
    
    def print_summary(self):
        """Print execution summary."""
        total_time = self.end_time - self.start_time
        
        logger.info("="*80)
        logger.info("ETL PIPELINE SUMMARY")
        logger.info("="*80)
        logger.info(f"Total execution time: {total_time:.2f}s ({total_time/60:.2f} minutes)")
        logger.info("")
        
        for result in self.results:
            status = "✓" if result['success'] else "✗"
            logger.info(f"{status} {result['description']:30s} - {result['elapsed_seconds']:6.2f}s")
            
            if not result['success']:
                logger.error(f"    Error: {result['error']}")
        
        logger.info("="*80)
        
        successful = sum(1 for r in self.results if r['success'])
        total = len(self.results)
        
        logger.info(f"Completed: {successful}/{total} scripts successful")
        
        if successful == total:
            logger.info("✓ ETL PIPELINE COMPLETED SUCCESSFULLY!")
        else:
            logger.error("✗ ETL PIPELINE COMPLETED WITH ERRORS")
        
        logger.info("="*80)
        logger.info(f"Full log available at: {log_file}")
        logger.info("="*80)

def run_specific_steps(step_numbers):
    """Run specific ETL steps by number (0-5)."""
    runner = ETLRunner()
    
    logger.info("="*80)
    logger.info(f"Running specific steps: {step_numbers}")
    logger.info("="*80)
    
    runner.start_time = time.time()
    
    for step_num in step_numbers:
        if 0 <= step_num < len(ETL_SCRIPTS):
            script_name, description = ETL_SCRIPTS[step_num]
            runner.run_script(script_name, description)
        else:
            logger.error(f"Invalid step number: {step_num}")
    
    runner.end_time = time.time()
    runner.print_summary()

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run ETL pipeline')
    parser.add_argument(
        '--steps',
        nargs='+',
        type=int,
        help='Specific steps to run (0-5). Example: --steps 0 1 2'
    )
    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continue running even if a step fails'
    )
    
    args = parser.parse_args()
    
    if args.steps:
        run_specific_steps(args.steps)
    else:
        runner = ETLRunner()
        runner.run_all(stop_on_error=not args.continue_on_error)

if __name__ == "__main__":
    main()