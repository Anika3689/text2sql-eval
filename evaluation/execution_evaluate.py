import sqlite3
import pandas as pd
import numpy as np

def execute_query(db_path, query):
    """Execute SQL query and return results (or error)."""
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df, None
    except Exception as e:
        return None, str(e)

def categorize_error(error_msg):
    """Map raw SQLite error messages into helpful categories."""
    if error_msg is None:
        return None
    msg = error_msg.lower()
    if "syntax error" in msg:
        return "Syntax Error"
    if "no such table" in msg:
        return "Missing Table"
    if "no such column" in msg:
        return "Missing Column"
    if "ambiguous column" in msg:
        return "Ambiguous Column"
    if "datatype mismatch" in msg:
        return "Datatype Mismatch"
    return "Other Error"

def match_result_sets(gold_df, pred_df, order_sensitive=False):
    """Compare two DataFrames ignoring column order, enforcing row order only if needed."""
    if gold_df.shape != pred_df.shape:
        return False

    gold_cols = list(gold_df.columns)
    pred_cols = set(pred_df.columns)  # just to track size

    used_pred = set()
    for gcol in gold_cols:
        gseries = gold_df[gcol]
        matched = False
        for pcol in pred_df.columns:
            if pcol in used_pred:
                continue
            pseries = pred_df[pcol]

            if order_sensitive:
                # row-by-row match
                if gseries.equals(pseries):
                    used_pred.add(pcol)
                    matched = True
                    break
            else:
                # compare as multisets (ignore row order)
                if set(gseries) == set(pseries) and gseries.value_counts().equals(pseries.value_counts()):
                    used_pred.add(pcol)
                    matched = True
                    break
        if not matched:
            return False

    return True

def evaluate_execution(samples, db_dir):
    """
    samples: list of dicts like
        {"db_id": "database_name", "gold": "SELECT ...", "pred": "SELECT ..."}
    """
    results = []
    correct_count = 0

    for s in samples:
        db_path = f"{db_dir}/{s['db_id']}/{s['db_id']}.sqlite"

        gold_query, pred_query = s["gold"], s["pred"]
        gold_df, gold_err = execute_query(db_path, gold_query)
        pred_df, pred_err = execute_query(db_path, pred_query)

        gold_cat = categorize_error(gold_err)
        pred_cat = categorize_error(pred_err)

        correct = False
        order_sensitive = "order by" in gold_query
        if gold_err is None and pred_err is None:
            correct = match_result_sets(gold_df, pred_df, order_sensitive)

        results.append({
            "db_id": s["db_id"],
            "correct": correct,
            "gold_error": gold_cat,
            "pred_error": pred_cat
        })

        if correct:
            correct_count += 1

    accuracy = correct_count / len(samples)
    return accuracy, results


if __name__ == '__main__':

    samples = [
        {
            "db_id": "college_1",
            "gold": "SELECT STU_LNAME AS l_name, STU_FNAME FROM student WHERE PROF_NUM > 300 ORDER BY STU_LNAME DESC",
            "pred": "SELECT STU_FNAME, STU_LNAME FROM student WHERE PROF_NUM > 300 ORDER BY STU_LNAME DESC"
        },
    ]

    accuracy, results = evaluate_execution(samples, '/Users/anikaraghavan/Downloads/text2sql-eval/data/spider/database_files')
    print(accuracy)
    print(results)