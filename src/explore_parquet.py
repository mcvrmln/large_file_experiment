""" Python script for exploring Parquet files"""

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import os
import glob

files = glob.glob("./data/*.parquet")

for file in files:
    df = pd.read_parquet(file, engine="pyarrow")
    print(f"Filename is: {file}")
    print(df.head(10))
    print(df.info())

print(files)

pq_array = pa.parquet.read_table(files[0], memory_map=True)

print(pq_array)

# table2 = pq.read_table("./data/20240220T21201708460426_file.parquet")

# print(table2.to_pandas().head(15))
# print(table2.to_pandas().info())
# print(table2.to_pandas().count())

# print(table2)
