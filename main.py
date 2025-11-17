import os

from parser import parse
from pathlib import Path
import pyarrow as pa
import pyarrow.csv as csv


def find_file(symbol: str, increm: str) -> str:
      """Resolve the extracted file path in `etl_tmp`.

      Expected layout (existing in this workspace):
         etl_tmp/Download_March15/Historical_stock_full_{ALPHA}_{increm}_adj_splitdiv/{SYMBOL}_full_{increm}_adjsplitdiv.txt

      Returns an absolute path string.
      """
      alpha = symbol[0].upper()
      # For a file at repo root `Path(__file__).resolve().parent` is the repo root.
      base = Path(__file__).resolve().parent
      # folder under etl_tmp in this repo
      rel = Path("etl_tmp") / "Download_March15" / f"Historical_stock_full_{alpha}_{increm}_adj_splitdiv"
      p = (base / rel / f"{symbol}_full_{increm}_adjsplitdiv.txt").resolve()
      return str(p)


if __name__ == "__main__":
      # path = find_file("AAPL", "1hour")

      path = find_file("A", "5min")
      print("Resolved path:", path)

      if not Path(path).exists():
         raise FileNotFoundError(f"Data file not found at resolved path: {path}")

      # Use parser.parse which already handles reading and filtering
      table = parse(path, start_time="2023-06-01 00:00:00", end_time="2023-06-29 15:50:00")
      print(f"Loaded {table.num_rows} rows in the filtered version")





