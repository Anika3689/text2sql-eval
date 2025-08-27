from evaluation.execution_evaluate import execute_query, categorize_error
import csv

def csv_to_dict_list(csv_path):
    """Reads CSV and returns a list of dicts."""
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return [row for row in reader]

def log_unexecutable_queries(samples, db_dir, log_file_path):
    """
    Filters unexecutable gold queries and logs them.
    """
    incorrect_count = 0
    with open(log_file_path, "w", encoding='utf-8') as log_file:
        log_file.write("question,db_id,gold,error\n")
        
        for s in samples:
            db_path = f"{db_dir}/{s['db_id']}/{s['db_id']}.sqlite"
            gold_query = s['query']
            _, gold_err = execute_query(db_path, gold_query)
            error = categorize_error(gold_err)
            if error is not None:
                log_file.write(f"{s['question'].replace(',', ';')},{s['db_id']},{gold_query.replace(',', ';')},{error}\n")
                incorrect_count += 1

    return incorrect_count

if __name__ == "__main__":
    csv_path = "/Users/anikaraghavan/Downloads/COPY-text2sql-eval/data/spider/datasets_original/test_dataset.csv"
    db_dir = "/Users/anikaraghavan/Downloads/COPY-text2sql-eval/data/spider/database_files"
    log_file_path = "/Users/anikaraghavan/Downloads/COPY-text2sql-eval/data/unexecutable_queries_log.csv"

    # Load dataset
    samples = csv_to_dict_list(csv_path)
    print(f"Loaded {len(samples)} samples from CSV.")

    # Log unexecutable gold queries
    count = log_unexecutable_queries(samples, db_dir, log_file_path)
    print(f"Found {count} unexecutable gold queries. Logged to {log_file_path}.")