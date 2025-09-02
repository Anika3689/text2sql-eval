from openai import OpenAI
import pandas as pd
import time

client = OpenAI()

EVAL_PROMPT = """You are given a SQL query and its parsed representation.
Your task is to verify if the parsed representation correctly reflects the structure of the SQL query.

Rules:
1. Only check the **structure**, operators, clauses, joins, aggregates, etc.
2. **Ignore case differences in string literals** (e.g., 'France' vs 'france' should be considered equivalent).
3. Only reply "Yes" if the parsed representation is correct, otherwise reply "No" with a brief explanation.

sql: Represents a complete SQL query or a subquery. This structure is recursive.
  Type: dict
  Format:
  {
    'select': (isDistinct: bool, select_expressions: list)
      - isDistinct: True if SELECT DISTINCT, False otherwise.
      - select_expressions: List of (val_unit: val_unit) for selected columns/expressions.
    'from': {'table_units': list, 'conds': condition}
      - table_units: List of table_unit tuples.
      - conds: A 'condition' list representing join conditions (if any explicit ON clauses, else []).
    'where': condition | None
      - condition: A 'condition' list for the WHERE clause. None if no WHERE clause.
    'group_by': list | None
      - List of col_unit for GROUP BY columns. None if no GROUP BY clause.
    'order_by': list | None
      - List of (val_unit: val_unit, direction: str) tuples.
      - direction: 'asc' or 'desc'.
      - None if no ORDER BY clause.
    'having': condition | None
      - A 'condition' list for the HAVING clause. None if no HAVING clause.
    'limit': int | None
      - The LIMIT value. None if no LIMIT clause.
    'intersect': True | False
      - True if the current (top-level) query is an intersect.
    'except': True | False
      - True if the current (top-level) query is an except.
    'union': True | False
      - True if the current (top-level) query is an union.
  }
  *The sql dict will contain keys 'left' and 'right' if the top-level query is an intersect/except/union. These keys represent the queries being IEU'd on, respectively.
"""

def evaluate_sql(sql: str, parsed_repr: str, model="gpt-4o") -> str:
    """Ask the model if parsed_repr correctly matches sql. Returns 'Yes' or 'No'."""
    prompt = EVAL_PROMPT + f" SQL Query: {sql} Parsed Representation: {parsed_repr} "
    
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=2000,
        temperature=0
    )
    time.sleep(1)
    return resp.choices[0].message.content.strip()


if __name__ == "__main__":
    
    parsed_reps_df = pd.read_csv("parsed_reps.csv")
    responses = []
    for i in range(len(parsed_reps_df)):
        sample = parsed_reps_df.iloc[i]
        sql = sample['queries']
        parsed = sample['parsed_reps']
        result = evaluate_sql(sql, parsed)
        print("Evaluation result:", result)
        responses.append(result)
    
    parsed_reps_df['response'] = responses
    parsed_reps_df.to_csv('parsed_reps.csv')