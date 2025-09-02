
entrypoint.py:
This script runs one of two types of evaluation pipelines on a dataset of gold and predicted SQL queries:

Execution-based accuracy (arg: exec)
Compares the actual query results against the given database to compute execution accuracy and performs a stratified evaluation based on schema and gold query features. 

Component-based accuracy (arg: component)
Evaluates the predicted queries by comparing SQL components (SELECT, WHERE, JOIN, etc.) against the gold queries.

*Note: The input dataset must contain the fields named as : (question) for the NL-question, (query) for the gold query, and (pred query) for the model's query prediction.

You can specify a directory (output_dir) to store all contents created by running the script.

Usage:
python run_evaluation.py \
    --eval_type <exec|component> \
    --input_dataset <path_to_dataset.csv> \
    --output_dir <output_directory> \
    --db_dir <database_directory_or_postgres_credentials> \
    [--engine <sqlite|postgres>] \
    [--log_resultsets]  # optional, only for exec

Arguments
--eval_type: Type of evaluation (exec or component)

--input_dataset: CSV file containing gold and predicted queries

--output_dir: Directory to save evaluation results, plots, and metadata

--db_dir: Directory containing SQLite databases or PostgreSQL credentials

--engine: (optional) Choose 'sqlite' or postgres (needed if not SQLite)

--log_resultsets: (optional) Optional flag to log query result sets (only for execution-based evaluation)

The script will generate CSV results, metadata, schema statistics, and visualizations in the output directory. The last two arguments are only needed for exec-based evaluation.
