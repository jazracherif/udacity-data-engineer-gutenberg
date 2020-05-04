import parquet
import json
import glob
import pandas as pd


catalog = pd.read_parquet("./out/catalog")
print(catalog)

reading_difficulty = pd.read_parquet("./out/reading_difficulty")
print(reading_difficulty)
