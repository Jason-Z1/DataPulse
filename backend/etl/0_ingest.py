"""
0_ingest.py - Unzip archives and create manifest with ticker tags
"""

from pathlib import Path
import zipfile
import json
import logging
import os
import argparse
from datetime import datetime
from collections import Counter


def resolve_raw_root(cli_path: str | None) -> Path:
    # 1) CLI argument
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        return p

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


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
TMP = Path("etl_tmp")
TMP.mkdir(exist_ok=True)
MANIFEST_PATH = TMP / "manifest.json"


def extract_tags_from_filename(filename: str):
    """
    Extract ticker symbol (everything before first underscore) as the tag.
    Example: AACT_full_5min_adjsplitdiv.txt -> 'AACT'
    """
    name = filename.rsplit('.', 1)[0]
    tag = name.split('_')[0]
    return [tag] if tag else []


def discover_files(raw_root: Path):
    # Find all TXT, CSV files and ZIP archives in all interval subfolders, recursively.
    items = []
    for interval_dir in raw_root.iterdir():
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


def unzip_archive(archive_path: Path, outdir: Path) -> bool:
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
    logger.info("=" * 60)
    logger.info("Starting data ingestion process")
    logger.info("=" * 60)

    items = discover_files(RAW_ROOT)
    if not items:
        logger.warning("No TXT/CSV or ZIP files found!")
        return

    extracted_data = []
    manifest_files = []
    all_tags = []

    for item_type, interval, path in items:
        if item_type == 'archive':
            outdir = TMP / interval / path.stem
            outdir.mkdir(parents=True, exist_ok=True)
            success = unzip_archive(path, outdir)
            extracted_data.append((interval, path.name, outdir, success))
            if success:
                for file in Path(outdir).rglob("*"):
                    if file.suffix.lower() in [".csv", ".txt"]:
                        tags = extract_tags_from_filename(file.name)
                        all_tags.extend(tags)

                        manifest_files.append({
                            "path": str(file),
                            "size_bytes": file.stat().st_size,
                            "interval": interval,
                            "archive": path.name,
                            "filename": file.name,
                            "tags": tags,
                        })
        elif item_type == 'raw':
            tags = extract_tags_from_filename(path.name)
            all_tags.extend(tags)

            manifest_files.append({
                "path": str(path),
                "size_bytes": path.stat().st_size,
                "interval": interval,
                "archive": None,
                "filename": path.name,
                "tags": tags,
            })

    # Get unique tags and counts
    tag_counts = Counter(all_tags)
    unique_tags = sorted(tag_counts.keys())

    # Write manifest
    manifest = {
        "created_at": datetime.now().isoformat(),
        "files": manifest_files,
        "all_tags": unique_tags,
        "tag_counts": dict(tag_counts),
    }

    with open(MANIFEST_PATH, 'w') as f:
        json.dump(manifest, f, indent=2)

    logger.info(f"Manifest created with {len(manifest['files'])} files")
    logger.info(f"Found {len(unique_tags)} unique tags (ticker symbols)")
    logger.info(f"Top 10 tickers: {tag_counts.most_common(10)}")
    logger.info(f"Manifest saved to: {MANIFEST_PATH}")

    logger.info("=" * 60)
    logger.info(f"Ingestion complete: {len(manifest_files)} files indexed")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
