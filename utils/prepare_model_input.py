import pandas as pd
import json
import sqlglot
from sqlglot.expressions import Identifier, Table
from utils.deserialize_db_model import deserialize_db_schema_model
from datasets import Dataset, DatasetDict
from data.prompt_with_json_response import prompt
import json
import csv

from canonicalized_data_format.ddl_objects import DBSchemaModel


class PrepareModelInput:

    def __init__(self, arch : str, dataSource : Dataset, normalized_db_schemas, descriptions):

        if isinstance(normalized_db_schemas, str):
            normalized_db_schemas = deserialize_db_schema_model(normalized_db_schemas)
        self.dataSource = dataSource
        #Mapping of db_id : Schema representations
        self.normalized_db_schemas = normalized_db_schemas
        self.descriptions = descriptions
        self.prepare_based_on_architecture(arch)

    def prepare_based_on_architecture(self, arch : str):
        if arch == 'decoder-only':
            self.prepared_dataset = self.prepare_input_for_decoder_only_model()
        elif arch == 'encoder-decoder':
            pass
        else:
            raise TypeError("Model architecture must be seq2seq or decoder-only")
    
    def prepare_input_for_decoder_only_model(self) -> Dataset | DatasetDict:
        if isinstance(self.dataSource, Dataset):
            return self.dataSource.map(
                lambda example : self._format_single_sample_for_decoder_only(example),
                remove_columns=self.dataSource.column_names
            )
        else:
            raise TypeError("dataSource must be a huggingFace Dataset or DatasetDict.")

    def _format_single_sample_for_decoder_only(self, example):

        db_id = example['db_id']
        current_db_schema = self.normalized_db_schemas.get(db_id, "Schema not found for this db_id.")
        schema_ddl = current_db_schema.get_ddl_string()
        identifiers = extract_identifiers_from_schema(current_db_schema)
        postgres_sql_gold = convert_to_postgresql(example['query'])
        postgres_sql_gold = quote_postgres_identifiers(postgres_sql_gold, identifiers)
        gold_response = get_expected_response(postgres_sql_gold, self.descriptions.get(example['question'], ''), '')

        formatted_sample = {
            "text": (
                "<|begin_of_text|>"
                "<|start_header_id|>system<|end_header_id|>\n\n"
                f"{prompt}"
                "<|eot_id|>"
                "<|start_header_id|>user<|end_header_id|>\n\n"
                f"### Database Schema\n{schema_ddl}\n\n"
                f"### Input\n{example['question']}"
                "<|eot_id|>"
                "<|start_header_id|>assistant<|end_header_id|>\n\n"
                f"{gold_response}"
                "<|eot_id|>"
            )
        }
        return formatted_sample

def get_expected_response(gold_sql, description, suggestions, follow_up_questions=None):
    if follow_up_questions is None:
        follow_up_questions = []

    response = {
        "response_code": "IWX-AI-SUCCESS-001",
        "sql_query": gold_sql,
        "description": description,
        "follow_up_questions": follow_up_questions
    }

    if suggestions.strip():
        response["suggestions"] = suggestions

    return json.dumps(response, ensure_ascii=False)


def extract_identifiers_from_schema(schema_obj : DBSchemaModel) -> set[str]:
    quoted_identifiers = []
    column_names = []
    for table in schema_obj.tables:
        quoted_identifiers.append(table.name.lower())
        new_cols = [name.lower() for name in table.attributes.keys()]
        column_names += new_cols

    quoted_identifiers += column_names
    return quoted_identifiers

def convert_to_postgresql(query: str) -> str:
    return sqlglot.transpile(query, read='sqlite', write='postgres')[0]

def quote_postgres_identifiers(sql: str, schema_identifiers: set[str]) -> str:
    """
    Converts SQL to PostgreSQL style with double-quoted identifiers.
    
    Args:
        sql: Original SQL query (e.g., from Spider dataset).
        schema_identifiers: Set of all table and column names (case-sensitive).
        
    Returns:
        PostgreSQL-style SQL with all identifiers quoted.
    """
    ast = sqlglot.parse_one(sql, read='postgres')

    for node in ast.walk():
        if isinstance(node, Identifier) and (node.name).lower() in schema_identifiers:
            node.set("quoted", True)
    return ast.sql(dialect='postgres')

def load_qna_dict(csv_file, question_col="question", description_col="description"):
    qna_dict = {}
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            print(row[description_col])
            qna_dict[row[question_col]] = row[description_col]
    return qna_dict

if __name__ == '__main__':
    path_interim_db_schemas = '/Users/anikaraghavan/Downloads/text2sql-eval/data/spider/interim_db_schemas_object'
    df = pd.read_csv('data/spider/datasets_original/train_dataset.csv')
    dataSource = Dataset.from_pandas(df)

    qna = load_qna_dict("data/train_with_descriptions.csv")

    dataprocessor = PrepareModelInput('decoder-only', dataSource, path_interim_db_schemas, qna)
    formatted_dataset = dataprocessor.prepared_dataset
    with open('data/spider/ps_train_formatted_descriptions.jsonl', 'wb') as output_file:
        formatted_dataset.to_json(output_file, orient='records')
