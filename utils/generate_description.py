import pandas as pd
from utils.deserialize_db_model import deserialize_db_schema_model
from openai import OpenAI
import time
import os

client = OpenAI()

def generate_description(question, query, db_id, schema_text=None):
    prompt = f"""
    Given the following natural language question and its SQL query, 
    write a concise (around 1 sentence) description of what the query is doing.

    Question: {question}
    SQL: {query}
    Database: {db_id}
    Schema: {schema_text if schema_text else "[schema omitted]"}
    Description:
    """
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return response.choices[0].message.content.strip()

def get_schema_text(db_id, db_schema_mapping):
    db_schema = db_schema_mapping.get(db_id, "Schema not found for this db_id.")
    schema_ddl = db_schema.get_ddl_string()
    return schema_ddl


if __name__ == '__main__':
    dataset_type = 'train'
    dataset_path = 'data/spider/datasets_original/train_dataset.csv'
    df = pd.read_csv(dataset_path)  

    path_interim_db_schemas = 'data/spider/interim_db_schemas_object'
    db_schema_mapping = deserialize_db_schema_model(path_interim_db_schemas)

    output_file = f"{dataset_type}_with_descriptions.csv"

    if not os.path.exists(output_file):
        with open(output_file, "w", encoding="utf-8", newline="") as f:
            df.head(0).assign(questions=[], description=[]).to_csv(f, index=False)

    for i, row in df.iterrows():
        schema_text = get_schema_text(row["db_id"], db_schema_mapping)
        question = row['question']
        desc = generate_description(question, row["query"], row["db_id"], schema_text)

        row_df = pd.DataFrame([{
            "question": question,
            "description": desc
        }])

        row_df.to_csv(output_file, mode="a", header=False, index=False)

        print(f"Processed {i}")
        time.sleep(1)