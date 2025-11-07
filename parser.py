"""
Current stock data parser that reads from CSV/TXT Only (parquet will be used later)

"""

import os 
from datetime import datetime
from typing import Optional
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc

def find_file(symbol: str, increm: str, base_path: str = "../") -> str:
    """
    Supposed to find the file path for the given stock symbol and time increment in the current file structure

    symbol = ticker (AAPL)
    increm = time increment (5min, 1hr, etc)
    base_path = root directory
    """


    alpha = symbol[0].upper()
    
    path = os.path.join(base_path, increm, "FirstData", "Downloaded_March15", f"Historical_stock_full_{alpha}_{increm}_adj_splitdiv.zip",
                        f"{symbol}_full_{increm}_adjsplitdiv.txt")
    
    return path

def parse(file_path: str, start_time: Optional[str] = None, end_time: Optional[str] = None) -> pa.Table:
    """
    returns a PyArrow table with the parsed data
    """

    print(f"Processing... {file_path}")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Defines the data schema
    # Read volume as float first (some source files contain values like '128.0')
    # and then cast to int64 after parsing to avoid CSV conversion errors.
    schema = pa.schema([ ("timestamp", pa.timestamp("s")),
                        ("open", pa.float64()),
                        ("high", pa.float64()),
                        ("low", pa.float64()),
                        ("close", pa.float64()),
                        ("volume", pa.float64()) ])

    table = csv.read_csv(
        file_path,
        read_options=csv.ReadOptions(column_names=["timestamp", "open", "high", "low", "close", "volume"]),
        parse_options=csv.ParseOptions(delimiter=","),
        convert_options=csv.ConvertOptions(column_types=schema,
                                           timestamp_parsers=["%Y-%m-%d %H:%M:%S"])
    )


    # If start and end_time are given, filter it
    if start_time or end_time:
        filters = []

        if start_time:
            start_ts = pa.scalar(
                datetime.fromisoformat(start_time),
                type=pa.timestamp("s")
            )
            filters.append(pc.greater_equal(table["timestamp"], start_ts))

        if end_time:
            end_ts = pa.scalar(
                datetime.fromisoformat(end_time),
                type=pa.timestamp("s")
            )
            filters.append(pc.less_equal(table["timestamp"], end_ts))

        
        # Combining
        if filters:
            combined_filter = filters[0]
            for f in filters[1:]:
                combined_filter = pc.and_(combined_filter, f)
            table = table.filter(combined_filter)

    return table
    




