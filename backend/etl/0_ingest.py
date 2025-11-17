"""
0_ingest.py - Unzip archives and create manifest
"""
from pathlib import Path
import zipfile
import json
import logging
from datetime import datetime

# Debug outputs to confirm structure before running main logic
print("Current working directory:", Path.cwd())
print("RAW_ROOT points to:", Path('FirstData').resolve())
print("Contents of RAW_ROOT:", list(Path('FirstData').iterdir()) if Path('FirstData').exists() else "MISSING DIR")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
RAW_ROOT = Path('FirstData')
TMP = Path("./etl_tmp")
TMP.mkdir(exist_ok=True)
MANIFEST_PATH = TMP / "manifest.json"

def discover_files():
    """Find all TXT, CSV files and ZIP archives in all interval subfolders, recursively."""
    items = []
    for interval_dir in RAW_ROOT.iterdir():
        if not interval_dir.is_dir():
            continue
        # Find ZIP archives directly under interval dir
        for archive in interval_dir.glob("*.zip"):
            items.append(('archive', interval_dir.name, archive))
        # Find .txt and .csv files recursively
        for file_path in interval_dir.rglob("*"):
            if file_path.suffix.lower() in [".csv", ".txt"]:
                items.append(('raw', interval_dir.name, file_path))
    return items

def unzip_archive(archive_path, outdir):
    """Extract ZIP archive with error handling."""
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

if __name__ == "__main__":
    main()
