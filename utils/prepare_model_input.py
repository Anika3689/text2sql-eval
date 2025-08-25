import pandas as pd
import json
import sqlglot
from sqlglot.expressions import Identifier, Table
from utils.deserialize_db_model import deserialize_db_schema_model
from datasets import Dataset, DatasetDict

from canonicalized_data_format.ddl_objects import DBSchemaModel


class PrepareModelInput:

    def __init__(self, arch : str, dataSource : Dataset, normalized_db_schemas):

        if isinstance(normalized_db_schemas, str):
            normalized_db_schemas = deserialize_db_schema_model(normalized_db_schemas)
        self.dataSource = dataSource
        self.normalized_db_schemas = normalized_db_schemas
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

        formatted_sample = {
            "text": (
                "<|begin_of_text|>"
                "<|start_header_id|>system<|end_header_id|>\n\n"
                "You are an advanced SQL Assistant created by Infoworks, specializing in generating precise PostgreSQL queries from user inputs and provided database schema. "
                "Your design eliminates the need for prior database schema knowledge, offering a seamless experience in SQL query creation. You have 2 core functions:\n"
                "### Core Functions:\n"
                "- **Query Generation:** Transform user inputs into syntactically correct SQL queries. Ensure each query adheres to strict syntax and logic validation against the provided schema and also strictly adheres to performance guidelines given below.\n"
                "  - **Golden SQL:** If golden sql is part of the user provided context evaluate if the same can be used to answer the question if yes respond with the golden sql.\n"
                "  - **Focus:** Your primary role is SQL query generation. Avoid engaging in unrelated tasks such as web searches, math, joke, code writing, poetry, or general knowledge queries.\n"
                "  - **Schema Dependence:** Rely solely on user-provided schemas for query generation. Assume necessary details about formulas, column values, and join criteria as needed.\n"
                "  - **Professionalism:** Assist users in improving SQL query efficiency and accuracy while maintaining a professional demeanor.\n"
                "  - **Precision in Query Generation:** Validate each query to ensure alignment with user inputs and specified schema requirements. And make sure you quote all entities like tables and columns in the generated SQL using double quotes \"\" as per PostgreSQL conventions.\n"
                "  - **Optimization:** Prioritize returning the most informative data. Avoid querying all columns, query only the necessary ones, minimize the use of subqueries, use CTEs for readability, leverage window functions for advanced data analysis over self-joins, and consider the cost of data movement across warehouses. Use clustering keys to improve query performance by optimizing how data is stored and accessed in PostgreSQL, reduce the volume of data being processed by filtering early, reduce the number of query operations, use sorts only when necessary, avoid joins with an OR condition.\n"
                "  - **Attention to Detail:** Use the `current_date` function for current date queries. Double-check the PostgreSQL query for common errors, ensuring data type correctness, proper quoting of identifiers, correct function arguments usage, and PostgreSQL features such as using `quote_ident()` for dynamic referencing and employing `TRY_CAST()` for safe type conversion.\n"
                "<|eot_id|>"
                "<|start_header_id|>user<|end_header_id|>\n\n"
                f"### Database Schema\n{schema_ddl}\n\n"
                f"### Input\n{example['question']}"
                "<|eot_id|>"
                "<|start_header_id|>assistant<|end_header_id|>\n\n"
                f"{postgres_sql_gold}"
                "<|eot_id|>"
            )
        }


        return formatted_sample

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



if __name__ == '__main__':
    testing = True
    path_interim_db_schemas = '/Users/anikaraghavan/Downloads/text2sql-eval/data/spider/interim_db_schemas_object'
    #if not testing:
    df = pd.read_csv('data/spider/datasets_original/train_dataset.csv')
    dataSource = Dataset.from_pandas(df)

    dataprocessor = PrepareModelInput('decoder-only', dataSource, path_interim_db_schemas)
    formatted_dataset = dataprocessor.prepared_dataset
    with open('data/spider/ps_train_formatted_chat_template.jsonl', 'wb') as output_file:
        formatted_dataset.to_json(output_file, orient='records')
