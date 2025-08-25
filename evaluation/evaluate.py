from evaluation.canonical_query_representation import *
from collections import Counter
import pprint

# **Remember to add checks for clauses that don't appear in the query (before calculating those components scores)

def calculate_f1(precision, recall):
    if precision + recall == 0:
        f1 = 0
    else:
        f1 = 2 * precision * recall / (precision + recall)
    return f1

def get_limit_score(gold_limit, pred_limit):
    if gold_limit is None and pred_limit is None:
        return None
    elif gold_limit is None or pred_limit is None:
        return 0
    return gold_limit == pred_limit

    
def calc_select_score(gold_select_vals : list[ValUnit], pred_select_vals: list[ValUnit]):
    # Full column matches (col. id, agg function, distinct status)
    full_matches, gold_len, pred_len = unordered_structural_match(
        gold_select_vals, pred_select_vals, equal_val_units
    )
    # --- Extract col_units ---
    gold_col_units = [u for unit in gold_select_vals for u in extract_all_col_units(unit)]
    pred_col_units = [u for unit in pred_select_vals for u in extract_all_col_units(unit)]

    # Column-only matches (ignore agg_id)
    gold_cols_noagg = Counter((col_unit.col_id, col_unit.isDistinct) for col_unit in gold_col_units)
    pred_cols_noagg = Counter((col_unit.col_id, col_unit.isDistinct) for col_unit in pred_col_units)
    matches_without_agg = sum([min(gold_cols_noagg[item], pred_cols_noagg[item]) for item in gold_cols_noagg])

    # Column-only ignoring DISTINCT too
    gold_cols_nodist = Counter(col_unit.col_id for col_unit in gold_col_units)
    pred_cols_nodist = Counter(col_unit.col_id for col_unit in pred_col_units)
    matches_nodist = sum([min(gold_cols_nodist[col_id], pred_cols_nodist[col_id]) for col_id in gold_cols_nodist])

    return {
        "select_f1": calculate_f1(full_matches/pred_len if pred_len else 0, 
                                  full_matches/gold_len if gold_len else 0),
        "col_no_agg_f1": calculate_f1(
            matches_without_agg / len(pred_col_units) if pred_col_units else 0,
            matches_without_agg / len(gold_col_units) if gold_col_units else 0
        ),
        "col_no_distinct_f1": calculate_f1(
            matches_nodist / len(pred_col_units) if pred_col_units else 0,
            matches_nodist / len(gold_col_units) if gold_col_units else 0
        )
    }

def calc_from_score(gold_tables : list[TableUnit], pred_tables : list[TableUnit]):
    # Full match (table + join type)
    full_matches, gold_len, pred_len = unordered_structural_match(
        gold_tables, pred_tables, equal_table_units
    )
    # Table-only match (ignoring join type)
    table_only_matches, _, _ = unordered_structural_match(
        [TableUnit(t.table_type, t.table) for t in gold_tables],
        [TableUnit(t.table_type, t.table) for t in pred_tables],
        lambda g,p : equal_table_units(g, p, False)
    )

    precision = full_matches / pred_len if pred_len > 0 else 0
    recall = full_matches / gold_len if gold_len > 0 else 0

    table_precision = table_only_matches / pred_len if pred_len > 0 else 0
    table_recall = table_only_matches / gold_len if gold_len > 0 else 0
    return {
        "from_f1": calculate_f1(precision, recall),
        "table_match_f1": calculate_f1(table_precision, table_recall),
    }


def calc_group_by_score(gold_col_units: list[ColUnit], pred_col_units: list[CondUnit]):
    if not gold_col_units and not pred_col_units:
        return None
    elif not gold_col_units or not pred_col_units:
        return 0
    
    matching_column_units = set(gold_col_units) & set(pred_col_units)
    return calculate_f1(len(matching_column_units) / len(pred_col_units) if pred_col_units else 0, 
                        len(matching_column_units) / len(gold_col_units) if gold_col_units else 0)


def calc_order_by_score(gold_list: list[tuple[ValUnit, str]], pred_list: list[tuple[ValUnit, str]]):
    if not gold_list and not pred_list:
        return None
    elif not gold_list or not pred_list:
        return 0
    full_matches, num_gold_orders, num_pred_orders = ordered_structural_match(gold_list, pred_list, equal_val_units)
    undirected_matches, _ , _ = unordered_structural_match([val_unit for val_unit, _ in gold_list], 
                                                           [val_unit for val_unit, _ in pred_list], equal_val_units)
    return {
        'order_by_f1': calculate_f1(full_matches / num_pred_orders if num_pred_orders > 0 else 0,
                                    full_matches / num_gold_orders if num_gold_orders > 0 else 0),
        'expressions_no_direction_f1': calculate_f1(undirected_matches / num_pred_orders if num_pred_orders > 0 else 0,
                                                undirected_matches / num_gold_orders if num_gold_orders > 0 else 0)
    }


def calc_condition_score(gold_condition: list[CondUnit], pred_condition : list[CondUnit]): 
    if not gold_condition and not pred_condition:
        return None
    elif not gold_condition or not pred_condition:
        return 0
    
    # Extract just the atomic conditions
    gold_conditions = [cond for cond in gold_condition if type(cond) != str]
    pred_conditions = [cond for cond in pred_condition if type(cond) != str]

    cond_matches, num_gold_conds, num_pred_conds = unordered_structural_match(
        gold_conditions, pred_conditions, equal_atomic_conditions
    )
    gold_cols = [col_unit.col_id for cond_unit in gold_conditions for u in [cond_unit.operand, cond_unit.val1] for col_unit in extract_all_col_units(u)]
    pred_cols = [col_unit.col_id for cond_unit in pred_conditions for u in [cond_unit.operand, cond_unit.val1] for col_unit in extract_all_col_units(u)]

    gold_col_freqs = Counter(gold_cols)
    pred_col_freqs = Counter(pred_cols)
    col_matches = sum(min(gold_col_freqs[col], pred_col_freqs[col]) for col in gold_col_freqs)

    return {
        "conditions_f1" : calculate_f1(cond_matches / num_pred_conds if pred_conditions else 0, 
                                       cond_matches / num_gold_conds if gold_conditions else 0),
        "col_only_f1" : calculate_f1(col_matches / len(pred_cols) if pred_cols else 0,
                                    col_matches / len(gold_cols) if gold_cols else 0), 
    }

def extract_all_col_units(unit):
    if type(unit) == ColUnit:
        return [unit]
    elif type(unit) == ValUnit:
        return extract_all_col_units(unit.operand1) + extract_all_col_units(unit.operand2)
    return []

"""Used to replicate set matching between unhashable elements"""
def unordered_structural_match(gold_list, pred_list, equal_fn):
    matches = 0
    used_pred = set()

    for gold in gold_list:
        for i, pred in enumerate(pred_list):
            if i not in used_pred and equal_fn(gold, pred):
                matches += 1
                used_pred.add(i)
                break
    return matches, len(gold_list), len(pred_list)

"""Compares elements in the same position"""
def ordered_structural_match(gold_list, pred_list, equal_fn):
    matches = 0
    for gold, pred in zip(gold_list, pred_list):
        gold_unit, gold_dir = gold
        pred_unit, pred_dir = pred
        if gold_dir == pred_dir and equal_fn(gold_unit, pred_unit):
            matches += 1
    return matches, len(gold_list), len(pred_list)

def equal_table_units(gold: TableUnit, pred: TableUnit, is_join_included=True):
    if gold.table_type != pred.table_type or (is_join_included and gold.join_type != pred.join_type):
        return False
    return operand_equal(gold.table, pred.table)

def equal_commutative_conditions(gold_cond: CondUnit, pred_cond: CondUnit):
    # Order of operands doesn’t matter
    return (
        operand_equal(gold_cond.operand, pred_cond.operand)
        and operand_equal(gold_cond.val1, pred_cond.val1)
    ) or (
        operand_equal(gold_cond.operand, pred_cond.val1)
        and operand_equal(gold_cond.val1, pred_cond.operand)
    )

def equal_in_conditions(gold_cond: CondUnit, pred_cond: CondUnit) -> bool:
    gold_val, pred_val = gold_cond.val1, pred_cond.val1
    if type(gold_val) == type(pred_val) == list:
        gold_val = set(gold_val)
        pred_val = set(pred_val)
    return (
        operand_equal(gold_cond.operand, pred_cond.operand)
        and operand_equal(gold_val, pred_val)
    )

def equal_atomic_conditions(gold_cond: CondUnit, pred_cond: CondUnit) -> bool:
    if gold_cond.not_op != pred_cond.not_op:
        return False
    
    gold_op, pred_op = gold_cond.op_id, pred_cond.op_id
    if gold_op == pred_op:
        if gold_op in COMMUTATIVE_OPS:
            return equal_commutative_conditions(gold_cond, pred_cond)
        elif gold_op == 'in':
            return equal_in_conditions(gold_cond, pred_cond)
        else:
            return (
                operand_equal(gold_cond.operand, pred_cond.operand)
                and operand_equal(gold_cond.val1, pred_cond.val1)
                and operand_equal(gold_cond.val2, pred_cond.val2)
            )

    if opposing_comparison_signs((gold_op, pred_op)):
        return (
            operand_equal(gold_cond.operand, pred_cond.val1)
            and operand_equal(gold_cond.val1, pred_cond.operand)
        )
    return False


def opposing_comparison_signs(signs: tuple[str]):
    """Return True if the two operators are logical opposites."""
    gold_op, pred_op = signs
    return OPPOSING_SIGNS.get(gold_op) == pred_op


def equal_val_units(gold: ValUnit, pred: ValUnit):
    g_unit_op, g_operand1, g_operand2 = gold
    p_unit_op, p_operand1, p_operand2 = pred
    
    if g_unit_op != p_unit_op:
        return False
    
    if g_unit_op.lower() in ('sub', 'div'):
        if type(g_operand1) != type(p_operand1) or type(g_operand2) != type(p_operand2):
            return False
        return operand_equal(g_operand1, p_operand1) and operand_equal(g_operand2, p_operand2)
    elif g_unit_op.lower() in ('add', 'mul'):
        return (operand_equal(g_operand1, p_operand1) or operand_equal(g_operand1, p_operand2)) and \
                (operand_equal(g_operand2, p_operand1) or operand_equal(g_operand2, p_operand2))
    else:
        #Operator is None (the val_unit holds a single col_unit as operand_1)
        return operand_equal(g_operand1, p_operand1) 


def operand_equal(op1, op2):
    def isColUnitWrapper(val_unit):
        """Returns if a ValUnit(expression) is just a wrapper around a single Column Unit"""
        return type(val_unit.operand1) == ColUnit and val_unit.operand2 is None
    if op1 is None and op2 is None:
        return True
    elif op1 is None or op2 is None:
        return False
    
    if type(op1) == ValUnit and type(op2) == ValUnit:
        return equal_val_units(op1, op2)
    elif type(op1) == ValUnit and isColUnitWrapper(op1) and type(op2) == ColUnit:
        return op1.operand1 == op2
    elif type(op2) == ValUnit and isColUnitWrapper(op2) and type(op1) == ColUnit:
        return op2.operand1 == op1
    elif type(op1) == type(op2) == dict:  
        return equal_sql_dict(op1, op2) 
    elif type(op1) == type(op2):
        return op1 == op2
    
    return False


def equal_sql_dict(gold_sql: dict, pred_sql: dict):
    def all_ones(d):
        return all(
            all_ones(v) if isinstance(v, dict) else v is None or v == 1 
            for v in d.values()
        )
    scores = compare_sql_components(gold_sql, pred_sql)
    return all_ones(scores)

def compare_sql_components(gold_sql, pred_sql):
    scores = {}
    # --- INTERSECTION/EXCEPTION/UNION ---
    for iue_part in SQL_OPS:
        equal_score = 0
        if gold_sql[iue_part] and pred_sql[iue_part]:
            equal_score = equal_sql_dict(gold_sql['right_query'], pred_sql['right_query'])
            scores[iue_part] = equal_score
            gold_sql = gold_sql['left_query']
            pred_sql = pred_sql['left_query']

    # --- SELECT ---
    gold_distinct, gold_select = gold_sql['select']
    pred_distinct, pred_select = pred_sql['select']
    distinct_score = 1 if gold_distinct == pred_distinct else 0
    select_f1 = calc_select_score(gold_select, pred_select) 
    select_f1['distinct'] = distinct_score
    scores['select'] = select_f1
    # --- FROM ---
    scores['from'] = calc_from_score(gold_sql['from']['table_units'], 
                                     pred_sql['from']['table_units'])
    
    scores['explicit_join_conds'] = calc_condition_score(gold_sql['from']['conds'], 
                                                         pred_sql['from']['conds'])
    # -- WHERE ---
    scores['where'] = calc_condition_score(gold_sql['where'], 
                                           pred_sql['where'])
    # -- GROUP BY / HAVING ---
    group_by_score = calc_group_by_score(gold_sql['group_by'], 
                                         pred_sql['group_by'])
    having_score = calc_condition_score(gold_sql['having'], 
                                        pred_sql['having'])
    scores['group'] = group_by_score
    scores['group_by_having'] = (group_by_score + having_score) / 2 if group_by_score and having_score else None
    # -- ORDER BY ---
    scores['order'] = calc_order_by_score(gold_sql['order_by'], 
                                          pred_sql['order_by'])
    # -- LIMIT --
    scores['limit'] = get_limit_score(gold_sql['limit'], pred_sql['limit'])
    return scores


gold_sql = {
    "intersect": False,
    "union": False,
    "except": False,
    "from": {
        "table_units": [
            TableUnit(table_type="table_unit", table=0, join_type=None),
            TableUnit(table_type="table_unit", table=2, join_type="INNER"),
        ],
        "conds": [
            CondUnit(
                not_op=False,
                op_id="eq",
                operand=ValUnit(
                    unit_op="none",
                    operand1=ColUnit(agg_id=None, col_id=0, isDistinct=False),
                    operand2=None,
                ),
                val1=ColUnit(agg_id=None, col_id=2, isDistinct=False),
                val2=None,
            )
        ],
    },
    "select": (
        False,
        [
            ValUnit(
                unit_op="none",
                operand1=ColUnit(agg_id=None, col_id=3, isDistinct=False),
                operand2=None,
            )
        ],
    ),
    "where": [
        CondUnit(
            not_op=False,
            op_id="in",
            operand=ValUnit(
                unit_op="none",
                operand1=ColUnit(agg_id=None, col_id=1, isDistinct=False),
                operand2=None,
            ),
            val1={
                "intersect": False,
                "union": False,
                "except": False,
                "from": {
                    "table_units": [
                        TableUnit(table_type="table_unit", table=0, join_type=None),
                        TableUnit(table_type="table_unit", table=1, join_type="INNER"),
                    ],
                    "conds": [
                        CondUnit(
                            not_op=False,
                            op_id="eq",
                            operand=ValUnit(
                                unit_op="none",
                                operand1=ColUnit(
                                    agg_id=None, col_id=0, isDistinct=False
                                ),
                                operand2=None,
                            ),
                            val1=ColUnit(agg_id=None, col_id=2, isDistinct=False),
                            val2=None,
                        )
                    ],
                },
                "select": (
                    False,
                    [
                        ValUnit(
                            unit_op="none",
                            operand1=ColUnit(agg_id=None, col_id=1, isDistinct=False),
                            operand2=None,
                        )
                    ],
                ),
                "where": [
                    CondUnit(
                        not_op=False,
                        op_id="gt",
                        operand=ValUnit(
                            unit_op="none",
                            operand1=ColUnit(agg_id=None, col_id=3, isDistinct=False),
                            operand2=None,
                        ),
                        val1={
                            "intersect": False,
                            "union": False,
                            "except": False,
                            "from": {
                                "table_units": [
                                    TableUnit(
                                        table_type="table_unit", table=1, join_type=None
                                    )
                                ],
                                "conds": [],
                            },
                            "select": (
                                False,
                                [
                                    ValUnit(
                                        unit_op="none",
                                        operand1=ColUnit(
                                            agg_id="avg", col_id=3, isDistinct=False
                                        ),
                                        operand2=None,
                                    )
                                ],
                            ),
                            "where": None,
                            "having": None,
                            "group_by": None,
                            "order_by": None,
                            "limit": None,
                        },
                        val2=None,
                    )
                ],
                "having": None,
                "group_by": None,
                "order_by": None,
                "limit": None,
            },
            val2=None,
        )
    ],
    "having": None,
    "group_by": None,
    "order_by": None,
    "limit": None,
}

pred_sql = gold_sql = {
    "intersect": False,
    "union": False,
    "except": False,
    "from": {
        "table_units": [
            TableUnit(table_type="table_unit", table=0, join_type=None),
            TableUnit(table_type="table_unit", table=2, join_type="INNER"),
        ],
        "conds": [
            CondUnit(
                not_op=False,
                op_id="eq",
                operand=ValUnit(
                    unit_op="none",
                    operand1=ColUnit(agg_id=None, col_id=0, isDistinct=False),
                    operand2=None,
                ),
                val1=ColUnit(agg_id=None, col_id=2, isDistinct=False),
                val2=None,
            )
        ],
    },
    "select": (
        False,
        [
            ValUnit(
                unit_op="none",
                operand1=ColUnit(agg_id=None, col_id=3, isDistinct=False),
                operand2=None,
            )
        ],
    ),
    "where": [
        CondUnit(
            not_op=False,
            op_id="in",
            operand=ValUnit(
                unit_op="none",
                operand1=ColUnit(agg_id=None, col_id=1, isDistinct=False),
                operand2=None,
            ),
            val1={
                "intersect": False,
                "union": False,
                "except": False,
                "from": {
                    "table_units": [
                        TableUnit(table_type="table_unit", table=0, join_type=None),
                        TableUnit(table_type="table_unit", table=1, join_type="INNER"),
                    ],
                    "conds": [
                        CondUnit(
                            not_op=False,
                            op_id="eq",
                            operand=ValUnit(
                                unit_op="none",
                                operand1=ColUnit(
                                    agg_id=None, col_id=0, isDistinct=False
                                ),
                                operand2=None,
                            ),
                            val1=ColUnit(agg_id=None, col_id=2, isDistinct=False),
                            val2=None,
                        )
                    ],
                },
                "select": (
                    False,
                    [
                        ValUnit(
                            unit_op="none",
                            operand1=ColUnit(agg_id=None, col_id=1, isDistinct=False),
                            operand2=None,
                        )
                    ],
                ),
                "where": [
                    CondUnit(
                        not_op=False,
                        op_id="gt",
                        operand=ValUnit(
                            unit_op="none",
                            operand1=ColUnit(agg_id=None, col_id=3, isDistinct=False),
                            operand2=None,
                        ),
                        val1={
                            "intersect": False,
                            "union": False,
                            "except": False,
                            "from": {
                                "table_units": [
                                    TableUnit(
                                        table_type="table_unit", table=1, join_type=None
                                    )
                                ],
                                "conds": [],
                            },
                            "select": (
                                False,
                                [
                                    ValUnit(
                                        unit_op="none",
                                        operand1=ColUnit(
                                            agg_id="avg", col_id=3, isDistinct=False
                                        ),
                                        operand2=None,
                                    )
                                ],
                            ),
                            "where": None,
                            "having": None,
                            "group_by": None,
                            "order_by": None,
                            "limit": None,
                        },
                        val2=None,
                    )
                ],
                "having": None,
                "group_by": None,
                "order_by": None,
                "limit": None,
            },
            val2=None,
        )
    ],
    "having": None,
    "group_by": None,
    "order_by": None,
    "limit": None,
}



scores = compare_sql_components(gold_sql, pred_sql)
pprint.pprint(scores, indent=4, width=80)
# print(scores['select'])
# print(scores['from'])
# print(scores['where'])
# print(scores['explicit_join_conds'])
# print(scores['group'])
# print(scores['order'])