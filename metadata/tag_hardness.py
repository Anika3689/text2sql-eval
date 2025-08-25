import pandas as pd
import json
from utils.tokenize_query import tokenize
from metadata.query_complexity import QueryComplexity
from dataclasses import asdict


if __name__ == '__main__':

    dataset_path = "/Users/anikaraghavan/Downloads/text2sql-eval/data/spider/datasets_original/train_dataset.csv"
    output_path = "/Users/anikaraghavan/Downloads/text2sql-eval/data/spider/train_with_metadata.json"

    df = pd.read_csv(dataset_path)

    with open(output_path, "a") as f:
        for idx in range(len(df)):

            row = df.iloc[idx]
            db_id = row['db_id']
            question = row['question']
            query = row['query']
            
            #Tokenize query:
            tokens = tokenize(query)

            qc = QueryComplexity({'query': query, 'query_toks' : tokens})

            hardness = qc.get_hardness_level()  

            data = {
                "db_id" : db_id,
                "question" : question,
                "query" : query,
                "tokens" : tokens,
                "hardness" : hardness,
                "sql_features" : asdict(qc.feature_set)
            }

            f.write(json.dumps(data, indent=4) + "\n")
        