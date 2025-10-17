import polars as pl
from pathlib import Path
import zipfile
import io

# Test case, using a single directory.
# Different on different machines
dir = "D:\Projects\FirstData\Download_March15\Historical_stock_full_A_5min_adj_splitdiv.zip/" 
inner_name = "A_full_5min_adjsplitdiv.txt"

# Step 1: Read raw CSV file with no header and give column names
with zipfile.ZipFile(dir) as zf:
    with zf.open(inner_name) as fh:
        data = fh.read()

df = pl.read_csv(io.BytesIO(data), has_header=False, new_columns=["ts", "open", "high", "low", "close", "volume"],) # Absolute path

# Step 2: Parse the timestamp (ts) string -> timezone-aware datetime, cast numbers

df = (
    df.with_columns([
        pl.col("ts").str.strptime(pl.Datetime, fmt="%Y-%m-%d %H:%M:%S", strict=True, tz="UTC"),
        pl.col(["open", "high", "low", "close"]).cast(pl.Float64, strict=False),
        pl.col("volume").cast(pl.Int64, strict=False),
    ])
    .filter( # Filters out rows that are dupes of another
        (pl.col("low") <= pl.col("open")) &
        (pl.col("low") <= pl.col("close")) &
        (pl.col("high") >= pl.col("open")) &
        (pl.col("high") >= pl.col("close")) &
        (pl.col("volume") >= 0)
    )
    .groupby("ts").agg([ # Removes any duplicates
        pl.col("open").first().alias("open"),
        pl.col("high").max().alias("high"),
        pl.col("low").min().alias("low"),
        pl.col("close").last().alias("close"),
        pl.col("volume").sum().alias("volume"),
    ])
)

symbol = inner_name.split("_", 1)[0] # First string set in inner_name
df = df.with_columns(pl.lit(symbol).alias("symbol"))

# Final selection + date helper
out = (
    df.with_columns(pl.col("ts").dt_date().alias("date"))
        .select(["symbol", "ts", "date", "open", "high", "low", "close", "volume"])
)

print(out.head())
print(out.schema)

# Write the Parquet
out.write_parquet("normalized:parquet", compression="zstd")