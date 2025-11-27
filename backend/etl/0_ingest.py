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
import shutil
from datetime import datetime



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


   # 3) Same directory as script
   here = Path(__file__).resolve()
   p = here.parent / 'FirstData'
   if p.exists():
       return p


   # 4) Repository-relative: file -> etl -> backend -> repo
   repo_root = here.parents[2] if len(here.parents) >= 3 else here.parent
   p = repo_root / 'FirstData'
   if p.exists():
       return p


   # 5) Legacy: CWD-relative
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
TMP = Path("./etl_tmp")
TMP.mkdir(exist_ok=True)
MANIFEST_PATH = TMP / "manifest.json"


"""
This function checks for both zip and unzipped folders in the directory of ./FirstData, so make sure that if there
are zip/unzipped duplicates you remove them before running.
"""

DATA_EXT = {".txt", ".csv", ".parquet", ".pq"}

def discover_files(raw_root):
   # Find all TXT, CSV files and ZIP archives in all interval subfolders, recursively.
    items = []
    for interval_dir in sorted(raw_root.iterdir()):
        interval = interval_dir.name
        if interval_dir.is_dir():
             # Find ZIP archives directly under interval dir
            for archive in interval_dir.rglob("*.zip"):
                items.append(('archive', interval, archive))
            # Finds any nested archives anywhere under the interval dir
            for nested_archive in interval_dir.rglob("*.zip"):
                if nested_archive.parent == interval_dir:
                    pass # Already added the top-level zip
                items.append(("archive", interval, nested_archive))
            # Find .txt and .csv files recursively
            for file_path in interval_dir.rglob("*"):
                if file_path.name.lower().endswith((".csv", ".txt", ".csv.gz", ".txt.gz")):
                    items.append(('raw', interval, file_path))
        
        elif interval_dir.is_file():
            # Top-level file. If it's a zip, treat it as an archive with interval = stem
            if interval_dir.suffix.lower() == ".zip" or zipfile.is_zipfile(interval_dir):
                interval = interval_dir.stem
                items.append(("archive", interval, interval_dir))
            # Top-level raw files e.g. AAPL_full_1hour_adjsplitdiv.txt directly at raw_root
            elif interval_dir.suffix.lower() in DATA_EXT:
                # Interval unknown from filename -- set to empty for now
                items.append(("raw", "", interval_dir))
        
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


def main():
   parser = argparse.ArgumentParser(description='Ingest stock data and create manifest')
   parser.add_argument('--data-root', type=str, help='Path to FirstData directory')
   args = parser.parse_args()
  
   # Resolve the raw data root
   RAW_ROOT = resolve_raw_root(args.data_root)
  
   if not RAW_ROOT.exists():
       logger.error(f"❌ Data directory not found: {RAW_ROOT}")
       logger.error("Please ensure FirstData exists in one of these locations:")
       logger.error("  1. Specify with --data-root argument")
       logger.error("  2. Set FIRSTDATA_PATH environment variable")
       logger.error("  3. Place in repository root as 'FirstData'")
       logger.error("  4. Place in current directory as 'FirstData'")
       return
  
   logger.info(f"Using data root: {RAW_ROOT}")
   logger.info("="*60)
   logger.info("Starting data ingestion process")
   logger.info("="*60)


   items = discover_files(RAW_ROOT)
   if not items:
       logger.warning("No TXT/CSV or ZIP files found!")
       return

   extracted_data = []
   manifest_files = []


   for item_type, interval, path in items:
       if item_type == 'archive':
           # If the archive stem matches the interval (e.g. '1hour')
           if path.stem == interval or not path.stem:
               outdir = TMP / interval
           else:
               outdir = TMP / interval / path.stem
           outdir.mkdir(parents=True, exist_ok=True)
           success = unzip_archive(path, outdir)
           extracted_data.append((interval, path.name, outdir, success))
           if success:
                def _is_data_file(p: Path) -> bool:
                   n = p.name.lower()
                   return n.endswith('.csv') or n.endswith('.txt') or n.endswith('.csv.gz') or n.endswith('.txt.gz')

                # 1) Collect any data files directly present after extracting the archive
                for file in Path(outdir).rglob("*"):
                   if _is_data_file(file):
                       manifest_files.append({
                           "path": str(file),
                           "size_bytes": file.stat().st_size,
                           "interval": interval,
                           "archive": path.name,
                           "filename": file.name
                       })
                
                # 2) Extract any nested company ZIP files and collect their data files
                for company_zip in Path(outdir).rglob("*.zip"):
                    try:
                       company_target_dir = Path(outdir) / company_zip.stem
                       company_target_dir.mkdir(parents=True, exist_ok=True)
                       nested_success = unzip_archive(company_zip, company_target_dir)
                       if nested_success:
                           for f in company_target_dir.rglob("*"):
                               if _is_data_file(f):
                                   manifest_files.append({
                                       "path": str(f),
                                       "size_bytes": f.stat().st_size,
                                       "interval": interval,
                                       "archive": company_zip.name,
                                       "filename": f.name
                                   })
                    except Exception:
                       logger.exception(f"Failed processing nested zip: {company_zip}")
                # Removes leftover FirstData folder that contains the initial zip copies
                firstdata_dir = outdir / "FirstData"
                if firstdata_dir.exists():
                    shutil.rmtree(firstdata_dir)
                    logger.info(f"Removed duplicate archive tree: {firstdata_dir}")
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


if __name__ == "__main__":
   main()

