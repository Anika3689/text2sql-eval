import unittest
from metadata.query_complexity import QueryComplexity

class TestQueryComplexity(unittest.TestCase):

    def test_get_query_length(self):
        query = "SELECT creation, name , budget_in_billions FROM department"
        tokens = ["SELECT", "creation", ",", "name", ",", "budget_in_billions", "FROM", "department"]
        sample = {'query': query, 'query_toks': tokens}
        qc = QueryComplexity(sample)
        self.assertEqual(qc.get_query_length(), len(tokens))

    def test_get_involved_tables(self):
        query = "SELECT * FROM Customers AS C JOIN Orders AS O ON C.id = O.customer_id"
        tokens = query.split()
        qc = QueryComplexity({'query': query, 'query_toks': tokens})
        self.assertEqual(qc.get_involved_tables(), 2)

        aliases_query = "SELECT * FROM Customers AS C1 JOIN Customers as C2 ON C.id = C.id"
        tokens = aliases_query.split()
        qc = QueryComplexity({'query': aliases_query, 'query_toks': tokens})
        self.assertEqual(qc.get_involved_tables(), 2)

    def test_get_num_joins(self):
        query = "SELECT * FROM A JOIN B ON A.id = B.id JOIN C ON B.id = C.id"
        tokens = query.split()
        qc = QueryComplexity({'query': query, 'query_toks': tokens})
        self.assertEqual(qc.get_num_joins(), 2)

    def test_count_aggregates(self):
        tokens = [
            "SELECT",
            "max",
            "(",
            "budget_in_billions",
            ")",
            ",",
            "min",
            "(",
            "budget_in_billions",
            ")",
            "FROM",
            "department"
            ]
        query = "SELECT max(budget_in_billions) , min(budget_in_billions) FROM department"
        qc = QueryComplexity({'query': query, 'query_toks': tokens})
        self.assertEqual(qc.count_aggregates(), 2)

    def test_count_select_columns(self):
        query = "SELECT name, age as A FROM Users"
        tokens = query.split()
        qc = QueryComplexity({'query': query, 'query_toks': tokens})
        self.assertEqual(qc.count_select_columns(), 2)
        #self.assertEqual(qc.get_hardness_level(), "medium")

    def test_count_where_conditions(self):
        query = "SELECT * FROM Customers WHERE age > 30 AND country = 'USA'"
        tokens = query.split()
        qc = QueryComplexity({'query': query, 'query_toks': tokens})
        self.assertEqual(qc.count_where_conditions(), 2)
        #self.assertEqual(qc.get_hardness_level(), "medium")

    def test_count_group_by(self):
        query = "SELECT department, COUNT(*) FROM Employees GROUP BY department, role"
        tokens = query.split()
        qc = QueryComplexity({'query': query, 'query_toks': tokens})
        self.assertEqual(qc.count_group_by(), 2)


class TestQueryHardness(unittest.TestCase):

    def test_easy_query(self):
        query = "SELECT name FROM users"
        tokens = query.split()
        qc = QueryComplexity({'query': query, 'query_toks': tokens})
        self.assertEqual(qc.get_hardness_level(), "easy")

    def test_medium_query_with_agg(self):
        query = "SELECT MAX(salary) FROM employees"
        tokens = query.split()
        qc = QueryComplexity({'query': query, 'query_toks': tokens})
        self.assertEqual(qc.get_hardness_level(), "easy")

    def test_medium_query_with_select_cols(self):
        query = "SELECT name, age FROM users"
        tokens = query.split()
        qc = QueryComplexity({'query': query, 'query_toks': tokens})
        self.assertEqual(qc.get_hardness_level(), "medium")

    def test_hard_query_with_multiple_group_by(self):
        query = "SELECT COUNT(*), department FROM employees GROUP BY department, region"
        tokens = query.split()
        qc = QueryComplexity({'query': query, 'query_toks': tokens})
        self.assertEqual(qc.get_hardness_level(), "medium")

    def test_extra_query_with_many_components(self):
        query = """
        SELECT MAX(score), name FROM students
        WHERE grade > 90 AND passed = 1
        GROUP BY class, teacher
        INTERSECT
        SELECT MAX(score), name FROM alumni
        WHERE year > 2015
        GROUP BY department
        """
        tokens = [tok for tok in query.split() if tok.strip()]
        qc = QueryComplexity({'query': query, 'query_toks': tokens})
        self.assertEqual(qc.get_hardness_level(), "extra")


if __name__ == '__main__':
    unittest.main()
