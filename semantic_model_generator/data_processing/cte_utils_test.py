import re

import pytest

from semantic_model_generator.data_processing.cte_utils import remove_ltable_cte


class TestRemoveLogicalTableCTE:
    def test_removes_logical_table_cte(self) -> None:
        """
        Testing that we remove logical table CTEs corresponding to existing table names.
        """
        query = "WITH __logical_table AS (SELECT * FROM table1) SELECT * FROM __logical_table"
        table_names = ["LOGICAL_TABLE"]
        expected_query = "SELECT * FROM __logical_table"

        actual_output = remove_ltable_cte(query, table_names=table_names)
        actual_output = re.sub(r"\s+", " ", actual_output)

        assert actual_output == expected_query

    def test_does_not_remove_non_logical_cte(self) -> None:
        """
        Testing that CTEs not mapping to existing table names are not removed.
        """
        query = (
            "WITH __other_table AS (SELECT * FROM table1) SELECT * FROM __other_table"
        )
        table_names = ["LOGICAL_TABLE"]
        expected_query = (
            "WITH __other_table AS ( SELECT * FROM table1 ) SELECT * FROM __other_table"
        )

        actual_output = remove_ltable_cte(query, table_names=table_names)
        actual_output = re.sub(r"\s+", " ", actual_output)

        assert actual_output == expected_query

    def test_mixed_ctes(self) -> None:
        """
        Given a query containing a mixture of CTEs, only the logical table CTEs should be removed.
        """
        query = "WITH __logical_table AS (SELECT * FROM table1), __other_table AS (SELECT * FROM table2), __custom_table AS (SELECT * FROM table3) SELECT * FROM __logical_table"
        table_names = ["LOGICAL_TABLE"]
        expected_query = "WITH __other_table AS ( SELECT * FROM table2 ), __custom_table AS ( SELECT * FROM table3 ) SELECT * FROM __logical_table"

        actual_output = remove_ltable_cte(query, table_names=table_names)
        actual_output = re.sub(r"\s+", " ", actual_output)

        assert actual_output == expected_query

    def test_throws_value_error_without_cte(self) -> None:
        """
        Testing that an error is thrown if there is no CTE in the query.
        """
        query = "SELECT * FROM table1"
        table_names = ["LOGICAL_TABLE"]

        with pytest.raises(ValueError):
            remove_ltable_cte(query, table_names=table_names)

    def test_throws_value_error_if_first_cte_not_logical_table(self) -> None:
        """
        Testing that an error is thrown if the first CTE is not a logical table.
        """
        query = "WITH random_alias AS (SELECT * FROM table1), __logical_table AS (SELECT * FROM table2) SELECT * FROM __logical_table"
        table_names = ["LOGICAL_TABLE"]

        with pytest.raises(ValueError):
            remove_ltable_cte(query, table_names=table_names)
