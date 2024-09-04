import concurrent.futures
from collections import defaultdict
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional, TypeVar

import pandas as pd
from loguru import logger
from snowflake.connector import DictCursor
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError

from semantic_model_generator.data_processing.data_types import Column, Table
from semantic_model_generator.snowflake_utils import env_vars
from semantic_model_generator.snowflake_utils.utils import snowflake_connection

ConnectionType = TypeVar("ConnectionType")
# Append this to the end of the auto-generated comments to indicate that the comment was auto-generated.
AUTOGEN_TOKEN = "__"
_autogen_model = "llama3-8b"

# This is the raw column name from snowflake information schema or desc table
_COMMENT_COL = "COMMENT"
_COLUMN_NAME_COL = "COLUMN_NAME"
_DATATYPE_COL = "DATA_TYPE"
_TABLE_SCHEMA_COL = "TABLE_SCHEMA"
_TABLE_NAME_COL = "TABLE_NAME"
# Below are the renamed column names when we fetch into dataframe, to differentiate between table/column comments
_COLUMN_COMMENT_ALIAS = "COLUMN_COMMENT"
_TABLE_COMMENT_COL = "TABLE_COMMENT"

# https://docs.snowflake.com/en/sql-reference/data-types-datetime
TIME_MEASURE_DATATYPES = [
    "DATE",
    "DATETIME",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_TZ",
    "TIMESTAMP",
    "TIME",
]
# https://docs.snowflake.com/en/sql-reference/data-types-text
DIMENSION_DATATYPES = [
    "VARCHAR",
    "CHAR",
    "CHARACTER",
    "NCHAR",
    "STRING",
    "TEXT",
    "NVARCHAR",
    "NVARCHAR2",
    "CHAR VARYING",
    "NCHAR VARYING",
    "BINARY",
    "VARBINARY",
]
# https://docs.snowflake.com/en/sql-reference/data-types-numeric
MEASURE_DATATYPES = [
    "NUMBER",
    "DECIMAL",
    "DEC",
    "NUMERIC",
    "INT",
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "BYTEINT",
    "FLOAT",
    "FLOAT4",
    "FLOAT8",
    "DOUBLE",
    "DOUBLE PRECISION",
    "REAL",
]
OBJECT_DATATYPES = ["VARIANT", "ARRAY", "OBJECT", "GEOGRAPHY"]


_QUERY_TAG = "SEMANTIC_MODEL_GENERATOR"


def _get_table_comment(
    conn: SnowflakeConnection, table_name: str, columns_df: pd.DataFrame
) -> str:
    if columns_df[_TABLE_COMMENT_COL].iloc[0]:
        return columns_df[_TABLE_COMMENT_COL].iloc[0]  # type: ignore[no-any-return]
    else:
        # auto-generate table comment if it is not provided.
        try:
            tbl_ddl = (
                conn.cursor()  # type: ignore[union-attr]
                .execute(f"select get_ddl('table', '{table_name}');")
                .fetchall()[0][0]
                .replace("'", "\\'")
            )
            comment_prompt = f"Here is a table with below DDL: {tbl_ddl} \nPlease provide a business description for the table. Only return the description without any other text."
            complete_sql = f"select SNOWFLAKE.CORTEX.COMPLETE('{_autogen_model}', '{comment_prompt}')"
            cmt = conn.cursor().execute(complete_sql).fetchall()[0][0]  # type: ignore[union-attr]
            return str(cmt + AUTOGEN_TOKEN)
        except Exception as e:
            logger.warning(f"Unable to auto generate table comment: {e}")
            return ""


def _get_column_comment(
    conn: SnowflakeConnection, column_row: pd.Series, column_values: Optional[List[str]]
) -> str:
    if column_row[_COLUMN_COMMENT_ALIAS]:
        return column_row[_COLUMN_COMMENT_ALIAS]  # type: ignore[no-any-return]
    else:
        # auto-generate column comment if it is not provided.
        try:
            comment_prompt = f"""Here is column from table {column_row['TABLE_NAME']}:
name: {column_row['COLUMN_NAME']};
type: {column_row['DATA_TYPE']};
values: {';'.join(column_values) if column_values else ""};
Please provide a business description for the column. Only return the description without any other text."""
            complete_sql = f"select SNOWFLAKE.CORTEX.COMPLETE('{_autogen_model}', '{comment_prompt}')"
            cmt = conn.cursor().execute(complete_sql).fetchall()[0][0]  # type: ignore[union-attr]
            return str(cmt + AUTOGEN_TOKEN)
        except Exception as e:
            logger.warning(f"Unable to auto generate column comment: {e}")
            return ""


def get_table_representation(
    conn: SnowflakeConnection,
    schema_name: str,
    table_name: str,
    table_index: int,
    ndv_per_column: int,
    columns_df: pd.DataFrame,
    max_workers: int,
) -> Table:
    table_comment = _get_table_comment(conn, table_name, columns_df)

    def _get_ndv_per_column(column_row: pd.Series, ndv_per_column: int) -> int:
        data_type = column_row[_DATATYPE_COL]
        data_type = data_type.split("(")[0].strip().upper()
        if data_type in DIMENSION_DATATYPES:
            # For dimension columns, we will by default fetch at least 25 distinct values
            # As we index all dimensional column sample values by default.
            return max(25, ndv_per_column)
        if data_type in TIME_MEASURE_DATATYPES:
            return max(3, ndv_per_column)
        if data_type in MEASURE_DATATYPES:
            return max(3, ndv_per_column)
        return ndv_per_column

    def _get_col(col_index: int, column_row: pd.Series) -> Column:
        return _get_column_representation(
            conn=conn,
            schema_name=schema_name,
            table_name=table_name,
            column_row=column_row,
            column_index=col_index,
            ndv=_get_ndv_per_column(column_row, ndv_per_column),
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_col_index = {
            executor.submit(_get_col, col_index, column_row): col_index
            for col_index, (_, column_row) in enumerate(columns_df.iterrows())
        }
        index_and_column = []
        for future in concurrent.futures.as_completed(future_to_col_index):
            col_index = future_to_col_index[future]
            column = future.result()
            index_and_column.append((col_index, column))
        columns = [c for _, c in sorted(index_and_column, key=lambda x: x[0])]

    return Table(
        id_=table_index,
        name=table_name,
        comment=table_comment,
        columns=columns,
    )


def _get_column_representation(
    conn: SnowflakeConnection,
    schema_name: str,
    table_name: str,
    column_row: pd.Series,
    column_index: int,
    ndv: int,
) -> Column:
    column_name = column_row[_COLUMN_NAME_COL]
    column_datatype = column_row[_DATATYPE_COL]
    column_values = None
    if ndv > 0:
        # Pull sample values.
        try:
            cursor = conn.cursor(DictCursor)
            assert cursor is not None, "Cursor is unexpectedly None"
            cursor_execute = cursor.execute(
                f'select distinct "{column_name}" from "{schema_name}"."{table_name}" limit {ndv}'
            )
            assert cursor_execute is not None, "cursor_execute should not be none "
            res = cursor_execute.fetchall()
            # Cast all values to string to ensure the list is json serializable.
            # A better solution would be to identify the possible types that are not
            # json serializable (e.g. datetime objects) and apply the appropriate casting
            # in just those cases.
            if len(res) > 0:
                if isinstance(res[0], dict):
                    col_key = [k for k in res[0].keys()][0]
                    column_values = [str(r[col_key]) for r in res]
                else:
                    raise ValueError(
                        f"Expected the first item of res to be a dict. Instead passed {res}"
                    )
        except Exception as e:
            logger.error(f"unable to get values: {e}")

    column_comment = _get_column_comment(conn, column_row, column_values)

    column = Column(
        id_=column_index,
        column_name=column_name,
        comment=column_comment,
        column_type=column_datatype,
        values=column_values,
    )
    return column


def _fetch_valid_tables_and_views(conn: SnowflakeConnection) -> pd.DataFrame:
    def _get_df(query: str) -> pd.DataFrame:
        cursor = conn.cursor().execute(query)
        assert cursor is not None, "cursor should not be none here."

        df = pd.DataFrame(
            cursor.fetchall(), columns=[c.name for c in cursor.description]
        )
        return df[["name", "schema_name", "comment"]].rename(
            columns=dict(
                name=_TABLE_NAME_COL,
                schema_name=_TABLE_SCHEMA_COL,
                comment=_TABLE_COMMENT_COL,
            )
        )

    tables = _get_df("show tables in database")
    views = _get_df("show views in database")
    return pd.concat([tables, views], axis=0)


def fetch_databases(conn: SnowflakeConnection) -> List[str]:
    """
    Fetches all databases that the current user has access to
    Args:
        conn: SnowflakeConnection to run the query

    Returns: a list of database names

    """
    query = "show databases;"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    return [result[1] for result in results]


def fetch_warehouses(conn: SnowflakeConnection) -> List[str]:
    """
    Fetches all warehouses that the current user has access to
    Args:
        conn: SnowflakeConnection to run the query

    Returns: a list of warehouses names

    """
    query = "show warehouses;"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    return [result[0] for result in results]


def fetch_schemas_in_database(conn: SnowflakeConnection, db_name: str) -> List[str]:
    """
    Fetches all schemas that the current user has access to in the current database
    Args:
        conn: SnowflakeConnection to run the query
        db_name: The name of the database to connect to.

    Returns: a list of qualified schema names (db.schema)

    """
    query = f"show schemas in database {db_name};"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    return [f"{result[4]}.{result[1]}" for result in results]


def fetch_tables_views_in_schema(
    conn: SnowflakeConnection, schema_name: str
) -> list[str]:
    """
    Fetches all tables and views that the current user has access to in the current schema
    Args:
        conn: SnowflakeConnection to run the query
        schema_name: The name of the schema to connect to.

    Returns: a list of fully qualified table names.
    """
    query = f"show tables in schema {schema_name};"
    cursor = conn.cursor()
    cursor.execute(query)
    tables = cursor.fetchall()
    # Each row in the result has columns (created_on, table_name, database_name, schema_name, ...)
    results = [f"{result[2]}.{result[3]}.{result[1]}" for result in tables]

    query = f"show views in schema {schema_name};"
    cursor = conn.cursor()
    cursor.execute(query)
    views = cursor.fetchall()
    # Each row in the result has columns (created_on, view_name, reserved, database_name, schema_name, ...)
    results += [f"{result[3]}.{result[4]}.{result[1]}" for result in views]

    return results


def fetch_stages_in_schema(conn: SnowflakeConnection, schema_name: str) -> list[str]:
    """
    Fetches all stages that the current user has access to in the current schema
    Args:
        conn: SnowflakeConnection to run the query
        schema_name: The name of the schema to connect to.

    Returns: a list of fully qualified stage names
    """

    query = f"show stages in schema {schema_name};"
    cursor = conn.cursor()
    cursor.execute(query)
    stages = cursor.fetchall()

    return [f"{result[2]}.{result[3]}.{result[1]}" for result in stages]


def fetch_yaml_names_in_stage(conn: SnowflakeConnection, stage_name: str) -> list[str]:
    """
    Fetches all yaml files that the current user has access to in the current stage
    Args:
        conn: SnowflakeConnection to run the query
        stage_name: The fully qualified name of the stage to connect to.

    Returns: a list of yaml file names
    """

    query = f"list @{stage_name} pattern='.*\\.yaml';"
    cursor = conn.cursor()
    cursor.execute(query)
    yaml_files = cursor.fetchall()

    # The file name is prefixed with "@{stage_name}/", so we need to remove that prefix.
    return [result[0].split("/")[-1] for result in yaml_files]


def get_valid_schemas_tables_columns_df(
    conn: SnowflakeConnection,
    table_schema: Optional[str] = None,
    table_names: Optional[List[str]] = None,
) -> pd.DataFrame:
    if table_names and not table_schema:
        logger.warning(
            "Provided table_name without table_schema, cannot filter to fetch the specific table"
        )

    where_clause = ""
    if table_schema:
        where_clause += f" where t.table_schema ilike '{table_schema}' "
        if table_names:
            table_names_str = ", ".join([f"'{t.lower()}'" for t in table_names])
            where_clause += f"AND LOWER(t.table_name) in ({table_names_str}) "
    query = f"""select t.{_TABLE_SCHEMA_COL}, t.{_TABLE_NAME_COL}, c.{_COLUMN_NAME_COL}, c.{_DATATYPE_COL}, c.{_COMMENT_COL} as {_COLUMN_COMMENT_ALIAS}
from information_schema.tables as t
join information_schema.columns as c on t.table_schema = c.table_schema and t.table_name = c.table_name{where_clause}
order by 1, 2, c.ordinal_position"""
    cursor_execute = conn.cursor().execute(query)
    assert cursor_execute, "cursor_execute should not be None here"
    schemas_tables_columns_df = cursor_execute.fetch_pandas_all()

    valid_tables_and_views_df = _fetch_valid_tables_and_views(conn=conn)

    valid_schemas_tables_columns_df = valid_tables_and_views_df.merge(
        schemas_tables_columns_df, how="inner", on=(_TABLE_SCHEMA_COL, _TABLE_NAME_COL)
    )
    return valid_schemas_tables_columns_df


class SnowflakeConnector:
    def __init__(
        self,
        account_name: str,
        max_workers: int = 1,
    ):
        self.account_name: str = account_name
        self._max_workers = max_workers

    # Required env vars below
    def _get_role(self) -> str:
        role = env_vars.SNOWFLAKE_ROLE
        if not role:
            raise ValueError(
                "You need to set an env var for the snowflake role. export SNOWFLAKE_ROLE=<your-snowflake-role>"
            )
        return role

    def _get_user(self) -> str:
        user = env_vars.SNOWFLAKE_USER
        if not user:
            raise ValueError(
                "You need to set an env var for the snowflake user. export SNOWFLAKE_USER=<your-snowflake-user>"
            )
        return user

    def _get_password(self) -> Optional[str]:
        password = env_vars.SNOWFLAKE_PASSWORD
        if not password and self._get_authenticator().lower() != "externalbrowser":  # type: ignore[union-attr]
            raise ValueError(
                "You need to set an env var for the snowflake password. export SNOWFLAKE_PASSWORD=<your-snowflake-password>"
            )
        return password

    def _get_warehouse(self) -> str:
        warehouse = env_vars.SNOWFLAKE_WAREHOUSE
        if not warehouse:
            raise ValueError(
                "You need to set an env var for the snowflake warehouse. export SNOWFLAKE_WAREHOUSE=<your-snowflake-warehouse-name>"
            )
        return warehouse

    def _get_host(self) -> Optional[str]:
        host = env_vars.SNOWFLAKE_HOST
        if not host:
            logger.info(
                "No host set. Attempting to connect without. To set export SNOWFLAKE_HOST=<snowflake-host-name>"
            )
        return host

    def _get_authenticator(self) -> Optional[str]:
        auth = env_vars.SNOWFLAKE_AUTHENTICATOR
        return auth

    def _get_mfa_passcode(self) -> Optional[str]:
        passcode = env_vars.SNOWFLAKE_MFA_PASSCODE
        return passcode

    def _is_mfa_passcode_in_password(self) -> bool:
        passcode_in_password = env_vars.SNOWFLAKE_MFA_PASSCODE_IN_PASSWORD
        if not passcode_in_password:
            return False
        return passcode_in_password.lower() == "true"

    @contextmanager
    def connect(
        self, db_name: str, schema_name: Optional[str] = None
    ) -> Generator[SnowflakeConnection, None, None]:
        """Opens a connection to the database and optional schema.

        This function is a context manager for a connection that can be used to execute queries.
        Example usage:

        with connector.connect(db_name="my_db", schema_name="my_schema") as conn:
            connector.execute(conn=conn, query="select * from table")

        Args:
            db_name: The name of the database to connect to.
            schema_name: The name of the schema to connect to. Primarily needed for Snowflake databases.
        """
        conn = None
        try:
            conn = self.open_connection(db_name, schema_name=schema_name)
            yield conn
        finally:
            if conn is not None:
                self._close_connection(conn)

    def open_connection(
        self, db_name: str, schema_name: Optional[str] = None
    ) -> SnowflakeConnection:
        connection = snowflake_connection(
            user=self._get_user(),
            password=self._get_password(),
            account=str(self.account_name),
            role=self._get_role(),
            warehouse=self._get_warehouse(),
            host=self._get_host(),
            authenticator=self._get_authenticator(),
            passcode=self._get_mfa_passcode(),
            passcode_in_password=self._is_mfa_passcode_in_password(),
        )
        if db_name:
            set_database(connection, db_name=db_name)
        if schema_name:
            set_schema(connection, schema_name=schema_name)

        if _QUERY_TAG:
            connection.cursor().execute(f"ALTER SESSION SET QUERY_TAG = '{_QUERY_TAG}'")
        connection.cursor().execute(
            f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {env_vars.DEFAULT_SESSION_TIMEOUT_SEC}"
        )
        return connection

    def _close_connection(self, connection: SnowflakeConnection) -> None:
        connection.close()

    def execute(
        self,
        connection: SnowflakeConnection,
        query: str,
    ) -> Dict[str, List[Any]]:
        try:
            if connection.warehouse is None:
                warehouse = self._get_warehouse()
                logger.debug(
                    f"There is no Warehouse assigned to Connection, setting it to config default ({warehouse})"
                )
                # TODO(jhilgart): Do we need to replace - with _?
                # Snowflake docs suggest we need identifiers with _, https://docs.snowflake.com/en/sql-reference/identifiers-syntax,
                # but unclear if we need this here.
                connection.cursor().execute(
                    f'use warehouse {warehouse.replace("-", "_")}'
                )
            cursor = connection.cursor(DictCursor)
            logger.info(f"Executing query = {query}")
            cursor_execute = cursor.execute(query)
            # assert below for MyPy. Should always be true.
            assert cursor_execute, "cursor_execute should not be None here"
            result = cursor_execute.fetchall()
        except ProgrammingError as e:
            raise ValueError(f"Query Error: {e}")

        out_dict = defaultdict(list)
        for row in result:
            if isinstance(row, dict):
                for k, v in row.items():
                    out_dict[k].append(v)
            else:
                raise ValueError(
                    f"Expected a dict for row object. Instead passed {row}"
                )
        return out_dict


def set_database(conn: SnowflakeConnection, db_name: str) -> None:
    try:
        conn.cursor().execute(f"USE DATABASE {db_name}")
    except Exception as e:
        raise ValueError(
            f"Could not connect to database {db_name}. Does the database exist in the account?"
        ) from e


def set_schema(conn: SnowflakeConnection, schema_name: str) -> None:
    try:
        conn.cursor().execute(f"USE SCHEMA {schema_name}")
    except Exception as e:
        raise ValueError(
            f"Could not connect to schema {schema_name}. Does the schema exist in the selected database?"
        ) from e
