import pandas as pd

csv_file_path = '/Users/anikaraghavan/Downloads/text2sql-eval/data/spider/datasets_original/test_dataset.csv'

df = pd.read_csv(csv_file_path)
unique_db_ids = df['db_id'].nunique()

print(f"Total unique db_id values: {unique_db_ids}")