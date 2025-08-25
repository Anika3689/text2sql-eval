import pandas as pd
import json
import csv
from datasets import load_dataset


"""Loads necessary attributes of samples from SPIDER dataset https://huggingface.co/datasets/xlangai/spider 
into a dataset"""

load_training = True
load_test = False
load_schema = False
keep_columns = ['db_id', 'question', 'query', 'query_toks']

if __name__ == '__main__':

    if load_training:
        df = pd.read_parquet('/Users/anikaraghavan/Downloads/text2sql-eval/data/spider/datasets_original/source/train-00000-of-00001.parquet')
    elif load_test:
        df = pd.read_parquet('data/spider/validation-00000-of-00001.parquet')
    elif load_schema:
        df = pd.read_json("hf://datasets/richardr1126/spider-schema/spider_schema_rows_v2.json")
        print(df.head(1))


    
    filtered_dataset = df[keep_columns]
    print(filtered_dataset.head())
    filtered_dataset.to_csv('/Users/anikaraghavan/Downloads/text2sql-eval/data/spider/datasets_original/train_dataset.csv', index=False)
