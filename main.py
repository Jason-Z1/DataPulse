import os

from parser import parse

import pyarrow as pa
import pyarrow.csv as csv 

# Finds the file directory
# Is subjected to change in form if we decide to use a different structure layout for data
def find_file(symbol: str, increm: str) -> str:
   alpha = str[0]
   path = "../" + increm + "/FirstData/Downloaded_March15/Historical_stock_full_" + alpha + "_" + increm + "_adj_splitdiv.zip/"
   path += symbol + "_full_" + increm + "_adjsplitdiv.txt/"
   return path


def parse(filePath: str, startTime: str, endTime: str) -> str:
   # Should use PyArrow to parse the file given and returns the chunk
   # of data that is requested.

   chunk = ""

   return chunk

if __name__ == "__main__":
   # path = find_file("AAPL", "1hour")

   path = "../FirstData/AA_full_5min_adjsplitdiv.txt"

   table = csv.read_csv(path)
   print(f"Loaded {path} with {len(table)} rows")

   table = parse(path, "2019-06-01 00:00:00", "2023-12-29 15:50:00")

   print(f"Loaded {len(table)} rows in the filtered version")





