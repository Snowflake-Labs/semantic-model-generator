# TODO: support in quoted columns are not well tested. Need to add more tests.

import copy
from typing import Dict, List, Optional

import sqlglot
import sqlglot.expressions
from loguru import logger
from sqlglot.dialects.snowflake import Snowflake

from semantic_model_generator.protos import semantic_model_pb2
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    OBJECT_DATATYPES,
)

_LOGICAL_TABLE_PREFIX = "__"


def is_logical_table(table_name: str) -> bool:
    """Returns true if 'table_name' is a logical table name."""
    return table_name.startswith(_LOGICAL_TABLE_PREFIX) and len(table_name) > len(
        _LOGICAL_TABLE_PREFIX
    )


def logical_table_name(table: semantic_model_pb2.Table) -> str:
    """Returns the name of logical table for a given table.  E.g. __fact"""
    return _LOGICAL_TABLE_PREFIX + table.name  # type: ignore[no-any-return]


def fully_qualified_table_name(table: semantic_model_pb2.FullyQualifiedTable) -> str:
    """Returns fully qualified table name such as my_db.my_schema.my_table"""
    fqn = table.table
    if len(table.schema) > 0:
        fqn = f"{table.schema}.{fqn}"
    if len(table.database) > 0:
        fqn = f"{table.database}.{fqn}"
    return fqn  # type: ignore[no-any-return]


def is_aggregation_expr(col: semantic_model_pb2.Column) -> bool:
    """Check if an expr contains aggregation function.
    Note: only flag True for aggregations that would changes number of rows of data.
    For window function, given the operation will produce value per row, mark as False here.

    Raises:
        ValueError: if expr is not parsable, or if aggregation expressions in non-measure columns.
    """
    parsed = sqlglot.parse_one(col.expr, dialect="snowflake")
    agg_func = list(parsed.find_all(sqlglot.expressions.AggFunc))
    window = list(parsed.find_all(sqlglot.expressions.Window))
    # We've confirmed window functions cannot appear inside aggregate functions
    # (gets execution error msg: Window function [SUM(...) OVER (PARTITION BY ...)] may not appear inside an aggregate function).
    # So if there's a window function present there can't also be an aggregate function applied to the window function.
    if len(agg_func) > 0 and len(window) == 0:
        if col.kind != 2:
            raise ValueError("Only allow aggregation expressions for measures.")
        return True
    return False


def _is_physical_table_column(col: semantic_model_pb2.Column) -> bool:
    """Returns whether the column refers to a single raw table column."""
    try:
        parsed = sqlglot.parse_one(col.expr, dialect="snowflake")
        return isinstance(parsed, sqlglot.expressions.Column)
    except Exception as ex:
        logger.warning(
            f"Failed to parse sql expression: {col.expr}. Error: {ex}. {col}"
        )
        return False


def _is_identifier_quoted(col_name: str) -> bool:
    return '"' in col_name


def _standardize_sf_identifier(col_name: str) -> str:
    # If the name is quoted, remove quotes if all cap letter and no spaces within quotes; else return the origin.
    if _is_identifier_quoted(col_name):
        col_name_stripped = col_name.strip('"')
        if col_name_stripped.isupper() and " " not in col_name:
            # Return the non-quoted and lower case version.
            return col_name_stripped.lower()
        else:
            # Return the original quoted version.
            return col_name

    else:
        # For non-quoted columns, return the lower case version.
        return col_name.lower()


def remove_ltable_cte(sql_w_ltable_cte: str) -> str:
    """Given a sql with prefix'd logical table conversion CTE,
    return:
        sql_without_logical_cte: the sql without the logical table conversion CTE.

    Raises:
        ValueError: If didn't find any CTE or parsed first CTE is not logical table CTE.
    """
    ast = sqlglot.parse_one(sql_w_ltable_cte, read=Snowflake)
    with_ = ast.args.get("with")
    if with_ is None:
        raise ValueError("Must contain the logical CTE.")
    if not is_logical_table(with_.expressions[0].alias):
        raise ValueError("Must contain the logical CTE.")

    if len(with_.expressions) == 1:
        # If only one cte, remove full with clause
        with_.pop()
    else:
        # Otherwise simply remove the first cte.
        with_.expressions[0].pop()
    sql_without_logical_cte = ast.sql(dialect=Snowflake, pretty=True)
    return sql_without_logical_cte  # type: ignore [no-any-return]


def _validate_col(column: semantic_model_pb2.Column) -> None:
    if " " in column.name.strip():
        raise ValueError(
            f"Please do not include spaces in your column name: {column.name}"
        )
    if column.data_type.upper() in OBJECT_DATATYPES:
        raise ValueError(
            f"We do not support object datatypes in the semantic model. Col {column.name} has data type {column.data_type}. Please remove this column from your semantic model or flatten it to non-object type."
        )


def validate_all_cols(table: semantic_model_pb2.Table) -> None:
    for column in table.columns:
        _validate_col(column)


def _get_col_expr(column: semantic_model_pb2.Column) -> str:
    """Return column expr in SQL format.
    Raise errors if columns is of OBJECT_DATATYPES, which we do not support today."""
    return (
        f"{column.expr.strip()} as {column.name.strip()}"
        if column.expr.strip().lower() != column.name.strip().lower()
        else f"{column.expr.strip()}"
    )


def _generate_cte_for(
    table: semantic_model_pb2.Table, columns: List[semantic_model_pb2.Column]
) -> str:
    """
    Returns a CTE representing a logical table that selects 'col' columns from 'table'.
    """

    if len(columns) == 0:
        raise ValueError("Please include at least one column to generate CTE on.")
    else:
        expr_columns = [_get_col_expr(col) for col in columns]
        cte = f"WITH {logical_table_name(table)} AS (\n"
        cte += "SELECT \n"
        cte += ",\n".join(expr_columns) + "\n"
        cte += f"FROM {fully_qualified_table_name(table.base_table)}"
        cte += ")"
        return cte


def get_all_physical_column_references(
    column: semantic_model_pb2.Column,
) -> List[str]:
    """Returns a set of column names referenced in the column expression.

    For example, the following column expressions yield the following return values:
    foo -> [foo]
    foo+bar -> [foo, bar]
    sum(foo) -> [foo]
    """
    try:
        parsed = sqlglot.parse_one(column.expr, dialect="snowflake")
        col_names = set()
        for col in parsed.find_all(sqlglot.expressions.Column):
            # TODO(renee): update to use _standardize_sf_identifier to handle quoted columns.
            col_name = col.name.lower()
            if col.this.quoted:
                col_name = col.name
            col_names.add(col_name)
        return sorted(list(col_names))
    except Exception as ex:
        raise ValueError(f"Failed to parse sql expression: {column.expr}. Error: {ex}")


def get_all_column_references_in_table(
    table: semantic_model_pb2.Table,
) -> Dict[str, Optional[semantic_model_pb2.Column]]:
    """Returns all physical table columns referenced in the table.

    Maps the raw table column name to the semantic context Column that directly references it.
    If the raw table column name has no exact semantic context Column, it'll be mapped to None.
    """
    all_col_references: Dict[str, Optional[semantic_model_pb2.Column]] = {}
    for col in table.columns:
        col_references = get_all_physical_column_references(column=col)
        if _is_physical_table_column(col):
            column = col
        else:
            column = None
        for reference in col_references:
            if reference in all_col_references:
                all_col_references[reference] = all_col_references[reference] or column
            else:
                all_col_references[reference] = column
    return all_col_references


def _enrich_column_in_expr_with_aggregation(
    table: semantic_model_pb2.Table,
) -> semantic_model_pb2.Table:
    """
    Append column mentioned in expr with aggregation, if not listed explicitly in table.columns.
    """
    tbl = copy.deepcopy(table)
    col_name_to_obj = get_all_column_references_in_table(tbl)
    cols_to_append = set()
    for col in tbl.columns:
        if not is_aggregation_expr(col):
            continue
        for col_name in get_all_physical_column_references(col):
            if col_name_to_obj[col_name] is None:
                cols_to_append.add(col_name)

    for col_name_to_append in cols_to_append:
        dest_col = semantic_model_pb2.Column(
            name=col_name_to_append, expr=col_name_to_append
        )
        # Only append when the name is not used; otherwise will cause compilation issue.
        if _standardize_sf_identifier(dest_col.name) not in [
            _standardize_sf_identifier(col.name) for col in tbl.columns
        ]:
            tbl.columns.append(dest_col)
    return tbl


def _generate_non_agg_cte(table: semantic_model_pb2.Table) -> Optional[str]:
    """
    Returns a CTE representing a logical table that selects 'col' columns from 'table' except for aggregation columns.
    """
    filtered_cols = [col for col in table.columns if not is_aggregation_expr(col)]
    if len(filtered_cols) > 0:
        return _generate_cte_for(table, filtered_cols)
    else:
        return None


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


def generate_select(
    table_in_column_format: semantic_model_pb2.Table, limit: int
) -> List[str]:
    """Generate select query for all columns for validation purpose."""
    sqls_to_return: List[str] = []
    # Generate select query for columns without aggregation exprs.
    non_agg_cte = _generate_non_agg_cte(table_in_column_format)
    if non_agg_cte is not None:
        non_agg_sql = (
            non_agg_cte
            + f"SELECT * FROM {logical_table_name(table_in_column_format)} LIMIT {limit}"
        )
        sqls_to_return.append(_convert_to_snowflake_sql(non_agg_sql))

    # Generate select query for columns with aggregation exprs.
    agg_cols = [
        col for col in table_in_column_format.columns if is_aggregation_expr(col)
    ]
    if len(agg_cols) == 0:
        return sqls_to_return
    else:
        agg_cte = _generate_cte_for(table_in_column_format, agg_cols)
        agg_sql = (
            agg_cte
            + f"SELECT * FROM {logical_table_name(table_in_column_format)} LIMIT {limit}"
        )
        sqls_to_return.append(_convert_to_snowflake_sql(agg_sql))
    return sqls_to_return


def expand_all_logical_tables_as_ctes(
    sql_query: str, model_in_column_format: semantic_model_pb2.SemanticModel
) -> str:
    """
    Returns a SQL query that expands all logical tables contained in ctx as ctes.
    """

    def generate_full_logical_table_ctes(
        ctx: semantic_model_pb2.SemanticModel,
    ) -> List[str]:
        """
        Given an arbitrary SQL, returns a list of CTEs representing all the logical tables
        referenced in it.
        """
        ctes: List[str] = []
        for table in ctx.tables:
            # Append all columns and expressions for the logical table.
            # If table contains expr with aggregations, enrich its referred columns into the table.
            table_ = _enrich_column_in_expr_with_aggregation(table)
            cte = _generate_non_agg_cte(table_)
            if cte is not None:
                ctes.append(cte)
        return ctes

    # Step 1: Generate a CTE for each logical table referenced in the query.
    ctes = generate_full_logical_table_ctes(model_in_column_format)

    # Step 2: Parse each generated CTE as a 'WITH' clause.
    new_withs = []
    for cte in ctes:
        new_withs.append(
            sqlglot.parse_one(cte, read=Snowflake, into=sqlglot.expressions.With)
        )

    # Step 3: Prefix the CTEs to the original query.
    ast = sqlglot.parse_one(sql_query, read=Snowflake)
    with_ = ast.args.get("with")
    # If the query doesn't have a WITH clause, then generate one.
    if with_ is None:
        merged_with = new_withs[0]
        remaining_ctes = [w.expressions[0] for w in new_withs[1:]]
        merged_with.set("expressions", merged_with.expressions + remaining_ctes)
        ast.set("with", merged_with)
    # If the query already has a WITH clause, prefix the CTEs to it.
    else:
        new_ctes = [w.expressions[0] for w in new_withs]
        with_.set("expressions", new_ctes + with_.expressions)
    return ast.sql(dialect=Snowflake, pretty=True)  # type: ignore [no-any-return]


def context_to_column_format(
    ctx: semantic_model_pb2.SemanticModel,
) -> semantic_model_pb2.SemanticModel:
    """
    Converts semantic_model_pb2.SemanticModel from a dimension/measure format to a column format.
    Returns a new semantic_model_pb2.SemanticModel object that's in column format.
    """
    ret = semantic_model_pb2.SemanticModel()
    ret.CopyFrom(ctx)
    for table in ret.tables:
        column_format = len(table.columns) > 0
        dimension_measure_format = (
            len(table.dimensions) > 0
            or len(table.time_dimensions) > 0
            or len(table.measures) > 0
        )
        if column_format and dimension_measure_format:
            raise ValueError(
                "table {table.name} defines both columns and dimensions/time_dimensions/measures."
            )
        if column_format:
            continue
        for d in table.dimensions:
            col = semantic_model_pb2.Column()
            col.kind = semantic_model_pb2.ColumnKind.dimension
            col.name = d.name
            col.synonyms.extend(d.synonyms)
            col.description = d.description
            col.expr = d.expr
            col.data_type = d.data_type
            col.unique = d.unique
            col.sample_values.extend(d.sample_values)
            table.columns.append(col)
        del table.dimensions[:]

        for td in table.time_dimensions:
            col = semantic_model_pb2.Column()
            col.kind = semantic_model_pb2.ColumnKind.time_dimension
            col.name = td.name
            col.synonyms.extend(td.synonyms)
            col.description = td.description
            col.expr = td.expr
            col.data_type = td.data_type
            col.unique = td.unique
            col.sample_values.extend(td.sample_values)
            table.columns.append(col)
        del table.time_dimensions[:]

        for m in table.measures:
            col = semantic_model_pb2.Column()
            col.kind = semantic_model_pb2.ColumnKind.measure
            col.name = m.name
            col.synonyms.extend(m.synonyms)
            col.description = m.description
            col.expr = m.expr
            col.data_type = m.data_type
            col.default_aggregation = m.default_aggregation
            col.sample_values.extend(m.sample_values)
            table.columns.append(col)
        del table.measures[:]
    return ret
