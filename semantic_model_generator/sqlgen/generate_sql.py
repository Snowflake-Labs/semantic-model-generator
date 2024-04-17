from typing import Optional, Set, Union

import sqlglot
from sqlglot.dialects.snowflake import Snowflake

from semantic_model_generator.protos.semantic_model_pb2 import (
    Dimension,
    FullyQualifiedTable,
    Measure,
    Table,
    TimeDimension,
)


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


def _create_select_statement(table: Table, cols: Set[str], limit: int) -> str:
    def _return_col_or_expr(
        col: Union[TimeDimension, Dimension, Measure], cols: Set[str]
    ) -> Optional[str]:
        # TODO(jhilgart): Handle quoted names properly.
        if col.name.lower() not in cols:
            return None
        expr = (
            f'{col.expr} as "{col.name}"'
            if col.expr.lower() != col.name.lower()
            else f"{col.expr}"
        )
        if expr == "":
            return None
        return expr

    columns = []
    for dim_col in table.dimensions:
        columns.append(_return_col_or_expr(dim_col, cols))
    for time_col in table.measures:
        columns.append(_return_col_or_expr(time_col, cols))
    for time_dim_col in table.time_dimensions:
        columns.append(_return_col_or_expr(time_dim_col, cols))

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
    cols = []
    for time_dim_col in table.time_dimensions:
        cols.append(time_dim_col.name.lower())
    for dim_col in table.dimensions:
        cols.append(dim_col.name.lower())
    for meausre_col in table.measures:
        cols.append(meausre_col.name.lower())

    col_set = set(cols)
    select = _create_select_statement(table, col_set, limit)

    return _convert_to_snowflake_sql(select)
