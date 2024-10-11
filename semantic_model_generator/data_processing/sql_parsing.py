import re
from typing import Dict, List, Optional, Set, Union

import sqlglot
from sqlglot.dialects.dialect import NormalizationStrategy
from sqlglot.dialects.snowflake import Snowflake
from sqlglot.errors import OptimizeError, ParseError, TokenError
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import Resolver, _qualify_columns
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.schema import Schema, ensure_schema

from semantic_model_generator.validate.keywords import SF_RESERVED_WORDS

DOUBLE_QUOTE = '"'
_SF_UNQUOTED_CASE_INSENSITIVE_IDENTIFIER = r"[A-Za-z_][A-Za-z0-9_]*(?:\$[A-Za-z0-9_]*)?"
_SF_UNQUOTED_CASE_SENSITIVE_IDENTIFIER = r"[A-Z_][A-Z0-9_]*(?:\$[A-Z0-9_]*)?"
UNQUOTED_CASE_INSENSITIVE_RE = re.compile(
    f"^({_SF_UNQUOTED_CASE_INSENSITIVE_IDENTIFIER})$"
)
UNQUOTED_CASE_SENSITIVE_RE = re.compile(f"^({_SF_UNQUOTED_CASE_SENSITIVE_IDENTIFIER})$")


def _get_escaped_name(id: str) -> str:
    """Add double quotes to escape quotes.
    Replace double quotes with double double quotes if there is existing double
    quotes.

    NOTE: See note in :meth:`_is_quoted`.

    Args:
        id: The string to be checked & treated.

    Returns:
        String with quotes would doubled; original string would add double quotes.
    """
    escape_quotes = id.replace(DOUBLE_QUOTE, DOUBLE_QUOTE + DOUBLE_QUOTE)
    return DOUBLE_QUOTE + escape_quotes + DOUBLE_QUOTE


def get_escaped_names(
    ids: Optional[Union[str, List[str]]]
) -> Optional[Union[str, List[str]]]:
    """Given a user provided identifier(s), this method will compute the equivalent
    column name identifier(s) in case of column name contains special characters, and
    maintains case-sensitivity
    https://docs.snowflake.com/en/sql-reference/identifiers-syntax.

    Args:
        ids: User provided column name identifier(s).

    Returns:
        Double-quoted Identifiers for column names, to make sure that column names are
        case sensitive.

    Raises:
        ValueError: if input types is unsupported or column name identifiers are
            invalid.
    """

    if ids is None:
        return None
    elif type(ids) is list:
        return [_get_escaped_name(id) for id in ids]
    elif type(ids) is str:
        return _get_escaped_name(ids)
    else:
        raise ValueError(
            "Unsupported type. Only string or list of string are supported for "
            "selecting columns."
        )


def should_be_quoted(identifier: str) -> bool:
    """Checks whether a given identifier should be quoted.

    NOTE: Assumes the identifier is given as it is stored in DB metadata
    and as shown in INFORMATION_SCHEMA or the output a DESCRIBE command.
    (The upper case for unquoted identifiers.)

    Args:
        identifier: The identifier to be checked

    Returns:
        Whether should be quoted.
    """
    if UNQUOTED_CASE_SENSITIVE_RE.match(identifier):
        if identifier in SF_RESERVED_WORDS:
            return True
        return False

    return True


def get_llm_friendly_name(identifier: str) -> str:
    """Return the form simplest for an LLM (lower case preferred)
    of an identifier. Put the identifier in double quotes if needed.

    NOTE: Assumes the identifier is given as it is stored in DB metadata
    and as shown in INFORMATION_SCHEMA or the output a DESCRIBE command.
    (The upper case for unquoted identifiers.)

    Args:
        identifier: The identifier to be checked

    Returns:
        Transformed identifier.
    """

    if should_be_quoted(identifier):
        return get_escaped_names(identifier)  # type: ignore

    return identifier.lower()


def get_all_table_names(sql_str: str) -> List[str]:
    """
    Given a string of SQL, returns all the tables present in the query
    """
    return [
        table.name
        for table in sqlglot.parse_one(sql_str).find_all(sqlglot.exp.Table)
        if table and table.name
    ]


def get_table_names_excluding_subqueries(sql_str: str) -> List[str]:
    """
    Given a string of SQL, returns all the tables present but not present in subqueries.
    For example, in the following query, this method excludes sub_table.
        SELECT primary_table.id, (SELECT MAX(value) FROM sub_table
        WHERE sub_table.primary_id = primary_table.id) AS max_value FROM primary_table

    Does not differentiate between a FROM in a WITH clauses and vanilla FROM.
    Does not consider table functions as table names.

    Note that queries with semi-structured access like f.value:ProductID::INTEGER
    are not supported in the current prod version of SQLGLOT==16.7.3, though locally
    the version 18.16.1 should enable this type of parsing.

    This can be useful in two ways: if one wants to swap columns between tables
    in sub-queries with the main queries to create an error, and also if one simply
    wants the tables used exclusively in the main FROM and JOIN clauses.
    """
    parsed = sqlglot.parse_one(sql_str)
    main_query_tables = []

    def _is_in_main_query(expr: sqlglot.Expression) -> bool:
        """Checks if the expression is present in a subquery by ascending the parents"""
        while expr.parent:
            if isinstance(expr.parent, sqlglot.expressions.Subquery) or isinstance(
                expr.parent, sqlglot.expressions.Where
            ):
                return False
            expr = expr.parent
        return True

    def _find_tables(expression: sqlglot.Expression) -> None:
        """Recurses through the parsed tree to find tables not in subqueries"""
        if (
            isinstance(expression, sqlglot.expressions.Table)
            and _is_in_main_query(expression)
            and expression.name
        ):
            main_query_tables.append(expression.name)
        for arg in expression.args.values():
            if isinstance(arg, sqlglot.expressions.Expression):
                _find_tables(arg)
            elif isinstance(arg, list):
                for sub in arg:
                    _find_tables(sub)

    # Start with the FROM clause
    from_clause = parsed.args.get("from")
    if from_clause:
        for from_expr in from_clause.find_all(sqlglot.expressions.Table):
            _find_tables(from_expr)

    # Look at the With clauses as well
    for with_clause in parsed.find_all(sqlglot.expressions.With):
        _find_tables(with_clause)

    # Then look at the JOIN clauses
    for join in parsed.find_all(sqlglot.expressions.Join):
        _find_tables(join)

    return list(dict.fromkeys(main_query_tables))  # Remove duplicates, preserves order


def get_all_column_names(sql_str: str) -> List[str]:
    """
    Given a string of SQL, returns all the columns selected from in the query
    """
    return [
        column.name
        for column in sqlglot.parse_one(sql_str).find_all(sqlglot.exp.Column)
    ]


def get_all_column_names_from_select(sql_str: str) -> List[str]:
    """
    Gets all the columns present specifically in the select clause
    Ignores columns used in a subquery or in a where clause
    """
    parsed = sqlglot.parse_one(sql_str)
    select_columns = []

    def _is_in_main_query(expr: sqlglot.Expression) -> bool:
        """
        Checks if the expression is present in a subquery or
        where clause by ascending the parents
        """
        while expr.parent:
            if isinstance(expr.parent, sqlglot.expressions.Subquery) or isinstance(
                expr.parent, sqlglot.expressions.Where
            ):
                return False
            expr = expr.parent
        return True

    # Traverse SELECT expressions
    for select in parsed.find_all(sqlglot.expressions.Select):
        for expression in select.args.get("expressions", []):
            # don't look in where clauses
            if not _is_in_main_query(expression):
                continue
            # If it's a column or an alias containing a column, add it to the list
            if isinstance(expression, sqlglot.expressions.Column):
                select_columns.append(expression.name)
            elif hasattr(expression, "this") and isinstance(
                expression.this, sqlglot.expressions.Column
            ):
                select_columns.append(expression.this.sql())

    return select_columns


def get_tables_with_distinct_columns_from_used_in_query(
    sql_str: str,
    all_table_names_in_schemas: List[str],
    columns_per_table: Dict[str, List[str]],
) -> List[str]:
    """
    Finds unused tables in the schema that do not contain at least one of the
    currently used columns from the currently in-use table names.

    Note that to simplify the implementation, a subquery is not considered in-use.

    If there are no columns in use, then return an empty list by default
    """
    tables_without_present_columns = []
    columns_used_in_query = get_all_column_names(sql_str)
    table_names_in_use = get_table_names_excluding_subqueries(sql_str)

    if len(columns_used_in_query) == 0:
        return []

    for table_name in all_table_names_in_schemas:
        if table_name in table_names_in_use:
            continue
        current_column_is_not_present_in_table = False
        for used_column in columns_used_in_query:
            if used_column not in columns_per_table[table_name]:
                current_column_is_not_present_in_table = True
        if current_column_is_not_present_in_table:
            tables_without_present_columns.append(table_name)
    return tables_without_present_columns


def get_columns_not_present_in_any_in_use_tables(
    table_names_in_use: List[str],
    all_table_names_in_schemas: List[str],
    columns_per_table: Dict[str, List[str]],
) -> List[str]:
    """
    Gets all the columns of other tables that are not present in currently
    used tables.

    Assumes that the tables will have column information in the dictionary.
    """
    all_columns_in_schema = [
        column
        for table in all_table_names_in_schemas
        for column in columns_per_table.get(table, [])
    ]
    all_columns_in_current_tables = [
        column
        for table in table_names_in_use
        for column in columns_per_table.get(table, [])
    ]
    return [
        column
        for column in all_columns_in_schema
        if column not in all_columns_in_current_tables
    ]


class FQNNormalizationError(Exception):
    pass


class FQNNormalizationQualifyColumnError(FQNNormalizationError):
    pass


class FQNNormalizationParsingError(FQNNormalizationError):
    pass


class LowercaseSnowflake(Snowflake):  # type: ignore
    """'snowflake' dialect enforces uppercase identifiers normalization by default
    thus here we introduce custom dialect to apply lowercase instead"""

    NORMALIZATION_STRATEGY = NormalizationStrategy.LOWERCASE


def transform_sql_to_fqn_form(
    sql: str, schema_simple: dict[str, dict[str, str]], pretty_output: bool = False
) -> str:
    """Transform query into "fully qualified names" form, where all column identifiers
    are expanded to table_name.column_name form. Additionally all non-mixed-case identifiers
    are lowercaseed and the whole query is formatted with sqlglot.

    Args:
        sql (str): input SQL query text
        schema_simple (dict[str, dict[str, str]]): schema dict, the same as in
            SQLGlot's optimize method: https://github.com/tobymao/sqlglot?tab=readme-ov-file#sql-optimizer
        pretty_output (bool, optional): apply tabs and newlines in final formatting. Defaults to False.

    Raises:
        FQNNormalizationParsingError: in case of unparsable query
        FQNNormalizationQualifyColumnError: in case of unidentifiable columns which are present in a query
        but are missing from provided schema

    Returns:
        str: normalized query
    """
    # We have to normalize schema here in order to handle mixed-case columns
    normalized_schema = {
        get_llm_friendly_name(table_name): {
            get_llm_friendly_name(c_name): c_type for c_name, c_type in columns.items()
        }
        for table_name, columns in schema_simple.items()
    }
    schema: Schema = ensure_schema(normalized_schema, dialect=LowercaseSnowflake)

    try:
        parsed = sqlglot.parse_one(sql, dialect=LowercaseSnowflake)
    except (ParseError, TokenError) as e:
        raise FQNNormalizationParsingError(str(e))

    # normalize all unquoted identifiers to lowercase
    parsed = normalize_identifiers(parsed, LowercaseSnowflake)
    # this handles normalization of some mixed-case table names
    parsed = qualify_tables(parsed, schema=schema, dialect=LowercaseSnowflake)

    # traverse every possible scope (one scope coresponds to one perticular select expression in a query)
    for scope in traverse_scope(parsed):
        # Resolver object helps with identifying column's parent table in a given scope
        resolver = Resolver(scope, schema, infer_schema=schema.empty)
        try:
            # sqlglot's optimization, which should qualify all columns (but in some cases it won't)
            _qualify_columns(scope, resolver)
        except OptimizeError as e:
            raise FQNNormalizationQualifyColumnError(str(e))

        # gather alias to table mapping for all tables in a current scope
        aliases = {
            table.alias: table.this
            for table in scope.expression.find_all(sqlglot.exp.Table)
            if table.alias
        }
        aliases.update(
            {
                a.name: a.parent.this
                for a in scope.expression.find_all(sqlglot.exp.TableAlias)
                if isinstance(a.parent, sqlglot.exp.Table)
            }
        )

        # remove all table aliases
        for table in scope.tables:
            if table.alias:
                table.set("alias", None)

        # fix all cases unhandled by _qualify_columns(...) optimization (GROUP BY and QUALIFY OVER)
        # and expand all remaining table aliases to table identifiers
        for column in scope._raw_columns:  # type: ignore
            if column.table == "":
                if (table_name_or_alias := resolver.get_table(column.name)) is not None:
                    table_name = aliases.get(
                        table_name_or_alias.name, table_name_or_alias.name
                    )
                    column.set("table", table_name)
            elif column.table in aliases:
                column.set("table", aliases[column.table])

        # fix case when we star-expand one particular table via alias:
        # SELECT f.* FROM foo as f --> SELECT foo.* FROM foo
        for star in scope.find_all(sqlglot.exp.Star):  # type: ignore
            if (
                isinstance(star.parent, sqlglot.exp.Column)
                or isinstance(star.parent, sqlglot.exp.Table)
            ) and star.parent.table in aliases:  # type: ignore
                star.parent.set("table", aliases[star.parent.table])  # type: ignore

    return str(parsed.sql(LowercaseSnowflake, pretty=pretty_output))


def extract_table_columns(sql: str) -> Dict[str, Set[str]]:
    """
    Given an arbitrary SQL, returns a map from referenced tables to their referenced columns.
    """
    # First, qualify all columns names with their table names.
    qualified_sql = transform_sql_to_fqn_form(sql, {}, pretty_output=True)
    table_columns: Dict[str, Set[str]] = {}
    parse = sqlglot.parse_one(qualified_sql, read=Snowflake)

    # Find all tables that are referenced.
    # This covers the case where a column is never specifically referenced in the
    # table, such as `select count(*) from table`.
    for t in parse.find_all(sqlglot.expressions.Table):
        if t.name not in table_columns:
            table_columns[t.name] = set()

    # Now map all the referenced columns to their tables.
    for e in parse.find_all(sqlglot.expressions.Column):
        table_columns.setdefault(e.table, set()).add(e.name)

    # Finally, find any tables that we `select * from` and add * to the columns.
    # Note that this only finds `*` when it is not qualified by a table name.
    # If it is qualified by by a table name, it will be parsed as column and
    # handled above.
    for ss in parse.find_all(sqlglot.expressions.Select):
        if sqlglot.expressions.Star() not in ss.expressions:
            continue
        from_ = ss.args.get("from", None)
        joins = ss.args.get("joins", [])
        if from_ is None:
            print(f"No from clause found in `select *` statement in query {sql}")
            continue

        for table in [from_] + joins:
            table_columns.setdefault(table.this.name, set()).add("*")

    return table_columns
