"""
0_ingest.py - Read archives and create manifest with ticker tags (no extract)
"""

from pathlib import Path
import zipfile
import json
import logging
import os
import argparse
from datetime import datetime
from collections import Counter
import io


def resolve_raw_root(cli_path: str | None) -> Path:
    # 1) CLI argument
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        return p

    # 2) Environment variable
    env = os.environ.get("FIRSTDATA_PATH")
    if env:
        p = Path(env)
        if p.exists():
            return p
        return p

    # 3) Same directory as script
    here = Path(__file__).resolve()
    p = here.parent / "FirstData"
    if p.exists():
        return p

    # 4) Repository-relative: file -> etl -> backend -> repo
    repo_root = here.parents[2] if len(here.parents) >= 3 else here.parent
    p = repo_root / "FirstData"
    if p.exists():
        return p

    # 5) Legacy: CWD-relative
    return Path("FirstData")


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Configuration
TMP = Path("etl_tmp")
TMP.mkdir(exist_ok=True)
MANIFEST_PATH = TMP / "manifest.json"

DATA_EXT = {".txt", ".csv", ".parquet", ".pq"}


def extract_tags_from_filename(filename: str):
    """
    Extract ticker symbol (everything before first underscore) as the tag.
    Example: AACT_full_5min_adjsplitdiv.txt -> 'AACT'
    """
    name = filename.rsplit(".", 1)[0]
    tag = name.split("_")[0]
    return [tag] if tag else []


def discover_files(raw_root):
    # Find all TXT, CSV, etc and ZIP archives in all interval subfolders, recursively.
    items = []
    for interval_dir in sorted(raw_root.iterdir()):
        interval = interval_dir.name
        if interval_dir.is_dir():
            for file_path in interval_dir.rglob("*"):
                path = str(file_path)
                fileType = "." + path.split(".")[-1]
                if fileType in DATA_EXT:
                    items.append(("raw", interval, file_path))
                elif fileType == ".zip":
                    items.append(("archive", interval, file_path))

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


def _is_data_name(name: str) -> bool:
    n = name.lower()
    return (
        n.endswith(".csv")
        or n.endswith(".txt")
        or n.endswith(".csv.gz")
        or n.endswith(".txt.gz")
        or n.endswith(".parquet")
    )


def _looks_like_real_zip(name: str) -> bool:
    """
    True only for actual nested company zips, false for macOS resource files etc.
    """
    n = name.lower()
    if "__macosx" in n:
        return False
    if n.startswith("._"):
        return False
    return n.endswith(".zip")


def process_zip_archive(
    archive_path: Path,
    interval: str,
    manifest_files: list[dict],
    all_tags: list[str],
) -> None:
    """
    Read inner files directly from a ZIP archive, without extracting to disk.
    Handles:
      - Data files directly in the archive
      - Nested company ZIP files inside the archive
    """
    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            for info in zf.infolist():
                name = info.filename

                # Skip directories
                if name.endswith("/"):
                    continue

                # Nested company zip inside interval zip
                if _looks_like_real_zip(name):
                    try:
                        nested_bytes = zf.read(info)
                        with zipfile.ZipFile(io.BytesIO(nested_bytes), "r") as nested_zf:
                            for nested_info in nested_zf.infolist():
                                nested_name = nested_info.filename
                                if nested_name.endswith("/"):
                                    continue
                                if not _is_data_name(nested_name):
                                    continue

                                tags = extract_tags_from_filename(Path(nested_name).name)
                                all_tags.extend(tags)
                                manifest_files.append(
                                    {
                                        # Virtual path: outer.zip!inner.zip!file
                                        "path": f"{archive_path}!{name}!{nested_name}",
                                        "size_bytes": nested_info.file_size,
                                        "interval": interval,
                                        "archive": archive_path.name,
                                        "filename": Path(nested_name).name,
                                        "tags": tags,
                                    }
                                )
                    except Exception:
                        logger.exception(
                            f"Failed processing nested zip in {archive_path}: {name}"
                        )
                    continue

                # Regular data file directly inside the archive
                if not _is_data_name(name):
                    continue

                tags = extract_tags_from_filename(Path(name).name)
                all_tags.extend(tags)
                manifest_files.append(
                    {
                        # Virtual path: outer.zip!file
                        "path": f"{archive_path}!{name}",
                        "size_bytes": info.file_size,
                        "interval": interval,
                        "archive": archive_path.name,
                        "filename": Path(name).name,
                        "tags": tags,
                    }
                )
    except zipfile.BadZipFile:
        logger.error(f"Bad ZIP file: {archive_path}")
    except Exception as e:
        logger.error(f"Error reading archive {archive_path}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Ingest stock data and create manifest")
    parser.add_argument("--data-root", type=str, help="Path to FirstData directory")
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
    logger.info("Starting data ingestion process (no unzip to disk)")
    logger.info("=" * 60)

    items = discover_files(RAW_ROOT)

    if not items:
        logger.warning("No TXT/CSV or ZIP files found!")
        return

    manifest_files = []
    all_tags = []

    for item_type, interval, path in items:
        if item_type == "archive":
            # Read archive contents directly (no extract to TMP)
            process_zip_archive(path, interval, manifest_files, all_tags)

        elif item_type == "raw":
            tags = extract_tags_from_filename(path.name)
            all_tags.extend(tags)

            manifest_files.append(
                {
                    "path": str(path),
                    "size_bytes": path.stat().st_size,
                    "interval": interval,
                    "archive": None,
                    "filename": path.name,
                    "tags": tags,
                }
            )

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
    with open(MANIFEST_PATH, "w") as f:
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
