""" Python script for exploring Parquet files"""

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

pq_array = pa.parquet.read_table(
    "./data/20240220T21201708460426_file.parquet", memory_map=True
)

# print(pq_array)

table2 = pq.read_table("./data/20240220T21201708460426_file.parquet")

print(table2.to_pandas().head(15))
print(table2.to_pandas().info())
print(table2.to_pandas().count())

print(table2)
