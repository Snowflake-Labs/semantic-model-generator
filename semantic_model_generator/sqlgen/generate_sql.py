from typing import Optional, Union

import sqlglot
from sqlglot.dialects.snowflake import Snowflake
from semantic_model_generator.validate.fields import validate_contains_datatype_for_each_col
from semantic_model_generator.protos.semantic_model_pb2 import (
    Dimension,
    FullyQualifiedTable,
    Measure,
    Table,
    TimeDimension,
)
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    OBJECT_DATATYPES,
)

_AGGREGATION_FUNCTIONS = [
    "sum",
    "avg",
    "min",
    "max",
    "count",
    "stdev",
    "var",
    "percentile_cont",
]


def _fully_qualified_table_name(table: FullyQualifiedTable) -> str:
    """Returns fully qualified table name such as my_db.my_schema.my_table"""
    fqn = table.table
    if len(table.schema) > 0:
        fqn = f"{table.schema}.{fqn}"
    if len(table.database) > 0:
        fqn = f"{table.database}.{fqn}"
    return fqn


def _convert_to_snowflake_sql(sql: str) -> str:
    """
    Converts a given SQL statement to Snowflake SQL syntax using SQLGlot.

    Args:
    sql (str): The SQL statement to convert.

    Returns:
    str: The SQL statement in Snowflake syntax.
    """
    try:
        expression = sqlglot.parse_one(sql, dialect=Snowflake)
    except Exception as e:
        raise ValueError(
            f"Unable to parse sql statement.\n Provided sql: {sql}\n. Error: {e}"
        )

    return expression.sql()


def _create_select_statement(table: Table, limit: int) -> str:
    def _return_col_or_expr(
        col: Union[TimeDimension, Dimension, Measure]
    ) -> Optional[str]:
        # TODO(jhilgart): Handle quoted names properly.
        if " " in col.name:
            raise ValueError(
                f"Column names should not have spaces in them. Passed = {col.name}"
            )
        if col.name.count('"') % 2 != 0:  # Odd number of quotes indicates an issue
            raise ValueError(
                f"Invalid column name '{col.name}'. Mismatched quotes detected."
            )
        expr = (
            f"{col.expr} as {col.name}"
            if col.expr.lower() != col.name.lower()
            else f"{col.expr}"
        )
        if expr == "":
            return None
        # Validate no aggregations in cols.
        for agg in _AGGREGATION_FUNCTIONS:
            if agg.lower() in expr.lower():
                raise ValueError(
                    f"Aggregations aren't allowed in columns yet. Please remove from {expr}."
                )
        if col.data_type.upper() in OBJECT_DATATYPES:
            raise ValueError(
                f"We do not support object datatypes in the semantic model. Col {col.name} has data type {col.data_type}. Please remove this column from your semantic model."
            )

        return expr

    columns = []
    for dim_col in table.dimensions:
        columns.append(_return_col_or_expr(dim_col))
    for measure_col in table.measures:
        columns.append(_return_col_or_expr(measure_col))
    for time_dim_col in table.time_dimensions:
        columns.append(_return_col_or_expr(time_dim_col))

    filtered_columns = [item for item in columns if item is not None]
    if len(filtered_columns) == 0:
        raise ValueError(f"No columns found for table {table.name}. Please remove this")

    select = ""
    select += "SELECT \n"
    select += ",\n".join(filtered_columns) + "\n"
    select += f"FROM {_fully_qualified_table_name(table.base_table)}" + "\n"
    select += f"LIMIT {limit}"
    return select


def generate_select_with_all_cols(table: Table, limit: int) -> str:
    """
    Generates a SQL SELECT statement for a specified semantic model table and row limit.

    Args:
        table (Table): The table metadata from which to extract column names.
        limit (int): Max number of rows the query should return.

    Returns:
        str: A SQL statement formatted for Snowflake.
    """
    

    select = _create_select_statement(table, limit)
    
    validate_contains_datatype_for_each_col(table)

    return _convert_to_snowflake_sql(select)
