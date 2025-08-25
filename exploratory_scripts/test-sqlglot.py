from sqlglot import parse_one, exp

def get_join_on_conditions(sql_query):
    """
    Extracts all join ON conditions from an SQL query using sqlglot.

    Args:
        sql_query (str): The SQL query string.

    Returns:
        list: A list of strings, where each string represents a join ON condition.
    """
    expressions = parse_one(sql_query)
    on_conditions = []

    for join_exp in expressions.find_all(exp.Join):
        if join_exp.on:
            print(join_exp)
    return on_conditions



expr = parse_one("SELECT * FROM table_1 AS t1 JOIN table_2 AS t2 ON t1.id = t2.id OR t1.id < t2.id JOIN table_3 AS t3 ON t2.id = t3.ref")
print(expr.args)
base_table = expr.args['from'].this
joins = expr.args.get('joins', [])

join_info = []

for join in joins:
    right_table_expr = join.this
    on_condition = join.args.get('on')
    join_info.append({
        'table': right_table_expr.name if isinstance(right_table_expr, exp.Table) else right_table_expr.sql(),
        'condition': on_condition.sql() if on_condition else None,
        'join_type': join.args.get('kind', 'INNER')
    })

print("Base table:", base_table.name)
print("Joins:", join_info)