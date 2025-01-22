import concurrent.futures
import json
from collections import defaultdict
from contextlib import contextmanager
from textwrap import dedent
from typing import Any, Dict, Generator, List, Optional, Sequence, TypeVar, Union

import pandas as pd
from loguru import logger
from snowflake.connector import DictCursor
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError
from snowflake.connector.pandas_tools import write_pandas

from semantic_model_generator.data_processing.data_types import Column, Table
from semantic_model_generator.snowflake_utils import env_vars
from semantic_model_generator.snowflake_utils.utils import snowflake_connection

ConnectionType = TypeVar("ConnectionType")
# Append this to the end of the auto-generated comments to indicate that the comment was auto-generated.
AUTOGEN_TOKEN = "__"
_AUTOGEN_MODEL = "llama3-8b"

# This is the raw column name from snowflake information schema or desc table
_COMMENT_COL = "COMMENT"
_COLUMN_NAME_COL = "COLUMN_NAME"
_DATATYPE_COL = "DATA_TYPE"
# Below are the renamed column names when we fetch into dataframe, to differentiate between table/column comments
_COLUMN_COMMENT_ALIAS = "COLUMN_COMMENT"
_TABLE_COMMENT_COL = "TABLE_COMMENT"

# https://docs.snowflake.com/en/sql-reference/data-types-datetime
TIME_MEASURE_DATATYPES = [
    "DATE",
    "DATETIME",
    "TIME",
    "TIMESTAMP",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_TZ",
]
# https://docs.snowflake.com/en/sql-reference/data-types-text
DIMENSION_DATATYPES = [
    "BOOLEAN",
    "BINARY",
    "CHAR VARYING",
    "CHAR",
    "CHARACTER",
    "NCHAR VARYING",
    "NCHAR",
    "NVARCHAR",
    "NVARCHAR2",
    "STRING",
    "TEXT",
    "VARBINARY",
    "VARCHAR",
]
# https://docs.snowflake.com/en/sql-reference/data-types-numeric
MEASURE_DATATYPES = [
    "BIGINT",
    "BYTEINT",
    "DEC",
    "DECIMAL",
    "DOUBLE PRECISION",
    "DOUBLE",
    "FLOAT",
    "FLOAT4",
    "FLOAT8",
    "INT",
    "INTEGER",
    "NUMBER",
    "NUMERIC",
    "REAL",
    "SMALLINT",
    "TINYINT",
]
OBJECT_DATATYPES = ["ARRAY", "GEOGRAPHY", "OBJECT", "VARIANT"]


_QUERY_TAG = "SEMANTIC_MODEL_GENERATOR"


def batch_cortex_complete(
    conn: SnowflakeConnection, queries: Sequence[str], model: str
) -> List[str]:
    import snowflake.snowpark._internal.utils as snowpark_utils

    query_frame = pd.DataFrame(dict(QUERY=queries))
    table_name = snowpark_utils.random_name_for_temp_object(
        snowpark_utils.TempObjectType.TABLE
    )
    write_pandas(
        conn=conn,
        df=query_frame,
        table_name=table_name,
        overwrite=True,
        table_type="temporary",
    )

    query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', QUERY) AS RESULT
    FROM {conn.database}.{conn.schema}.{table_name}
    """
    cursor = conn.cursor()
    cursor.execute(query)
    result_frame = cursor.fetch_pandas_all()
    result: List[str] = result_frame["RESULT"].to_list()

    return result


def _get_table_comment(
    conn: SnowflakeConnection,
    table_fqn: str,
    columns_df: pd.DataFrame,
    columns: Sequence[Column],
) -> str:
    if columns_df[_TABLE_COMMENT_COL].iloc[0]:
        return columns_df[_TABLE_COMMENT_COL].iloc[0]  # type: ignore[no-any-return]
    else:
        # auto-generate table comment if it is not provided.
        try:
            tbl_ddl = (
                conn.cursor()  # type: ignore[union-attr]
                .execute(f"select get_ddl('table', '{table_fqn}');")
                .fetchall()[0][0]
                .replace("'", "\\'")
            )
            column_comments = json.dumps(
                obj={col.column_name: col.comment for col in columns}, indent=2
            )
            comment_prompt_template = dedent(
                """
                Here is a table with below DDL:
                ```sql
                {tbl_ddl}
                ```

                Here are the comments associated with each column of the table:
                ```json
                {column_comments}
                ```

                Please provide a business description for the table. Only return the description without any other text.
                """
            ).strip()
            comment_prompt = comment_prompt_template.format(
                tbl_ddl=tbl_ddl, column_comments=column_comments
            ).replace("'", "\\'")
            complete_sql = (
                f"select SNOWFLAKE.CORTEX.COMPLETE('{_AUTOGEN_MODEL}', "
                f"'{comment_prompt}')"
            )
            cmt = conn.cursor().execute(complete_sql).fetchall()[0][0]  # type: ignore[union-attr]
            return str(cmt + AUTOGEN_TOKEN)
        except Exception as e:
            logger.warning(f"Unable to auto generate table comment: {e}")
            return ""


def get_table_primary_keys(
    conn: SnowflakeConnection, table_fqn: str
) -> Optional[list[str]]:
    query = f"show primary keys in table {table_fqn};"
    cursor = conn.cursor()
    cursor.execute(query)
    primary_keys = cursor.fetchall()
    if primary_keys:
        return [pk[3] for pk in primary_keys]
    return None


def get_table_representation(
    conn: SnowflakeConnection,
    table_fqn: str,
    max_string_sample_values: int,
    columns_df: pd.DataFrame,
    max_workers: int,
) -> Table:

    def _get_col(col_index: int, column_row: pd.Series) -> Column:
        return _get_column_representation(
            conn=conn,
            table_fqn=table_fqn,
            column_row=column_row,
            column_index=col_index,
            max_string_sample_values=max_string_sample_values,
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

    _add_column_comments(
        conn=conn,
        table_fqn=table_fqn,
        columns=columns,
    )

    table_comment = _get_table_comment(
        conn=conn,
        table_fqn=table_fqn,
        columns_df=columns_df,
        columns=columns,
    )

    return Table(name=table_fqn, comment=table_comment, columns=columns)


def _add_column_comments(
    conn: SnowflakeConnection, table_fqn: str, columns: List[Column]
) -> None:
    prompts = []
    for column in columns:
        if not column.comment:
            values_insert = (
                f"values: {json.dumps(column.values)};" if column.values else ""
            )

            comment_prompt = f"""\
            Here is a column from the table named {table_fqn}:
            name: {column.column_name};
            type: {column.column_type};
            {values_insert}
            Please provide a business description for the column. \
            Only return the description without any other text.
            """
            comment_prompt = dedent(comment_prompt.strip())
            comment_prompt = comment_prompt.replace("'", "\\'")
            prompts.append(comment_prompt)

    comments = batch_cortex_complete(
        conn=conn,
        queries=prompts,
        model="mistral-large2",
    )
    # Add updated comments.
    i = 0
    for column in columns:
        if not column.comment:
            column.comment = comments[i].strip()
            i += 1


def _get_column_representation(
    conn: SnowflakeConnection,
    table_fqn: str,
    column_row: pd.Series,
    column_index: int,
    max_string_sample_values: int,
) -> Column:
    # TODO(kschmaus): we could look at MANY sample values for the description.
    column_name = column_row[_COLUMN_NAME_COL]
    column_datatype = column_row[_DATATYPE_COL]

    column_values = None
    if column_datatype in DIMENSION_DATATYPES:
        cursor = conn.cursor(DictCursor)
        assert cursor is not None, "Cursor is unexpectedly None"
        cursor_execute = cursor.execute(
            f"""
            select distinct "{column_name}" from {table_fqn}
            where "{column_name}" is not null
            limit {max_string_sample_values + 1}
            """
        )
        assert cursor_execute is not None, "cursor_execute should not be none "
        res = cursor_execute.fetchall()
        if len(res) <= max_string_sample_values:
            column_values = [str(x[column_name]) for x in res]

    column = Column(
        id_=column_index,
        column_name=column_name,
        comment=column_row[_COLUMN_COMMENT_ALIAS],
        column_type=column_datatype,
        values=column_values,
    )
    return column


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


def fetch_table_schema(conn: SnowflakeConnection, table_fqn: str) -> dict[str, str]:
    """
    Fetches the table schema the current user has access
    Args:
        conn: SnowflakeConnection to run the query
        table_fqn: The fully qualified name of the table to connect to.

    Returns: a list of column names
    """
    query = f"DESCRIBE TABLE {table_fqn};"
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return dict([x[:2] for x in result])


def fetch_yaml_names_in_stage(
    conn: SnowflakeConnection, stage_name: str, include_yml: bool = False
) -> list[str]:
    """
    Fetches all yaml files that the current user has access to in the current stage
    Args:
        conn: SnowflakeConnection to run the query
        stage_name: The fully qualified name of the stage to connect to.
        include_yml: If True, will look for .yaml and .yml. If False, just .yaml. Defaults to False.

    Returns: a list of yaml file names
    """
    if include_yml:
        query = f"list @{stage_name} pattern='.*\\.yaml|.*\\.yml';"
    else:
        query = f"list @{stage_name} pattern='.*\\.yaml';"
    cursor = conn.cursor()
    cursor.execute(query)
    yaml_files = cursor.fetchall()

    # The file name is prefixed with "@{stage_name}/", so we need to remove that prefix.
    return [result[0].split("/")[-1] for result in yaml_files]


def fetch_table(conn: SnowflakeConnection, table_fqn: str) -> pd.DataFrame:
    query = f"SELECT * FROM {table_fqn};"
    cursor = conn.cursor()
    cursor.execute(query)
    query_result = cursor.fetch_pandas_all()
    return query_result


def create_table_in_schema(
    conn: SnowflakeConnection,
    table_fqn: str,
    columns_schema: Dict[str, str],
) -> bool:
    """
    Creates a table in the specified schema with the specified columns
    Args:
        conn: SnowflakeConnection to run the query
        table_fqn: The fully qualified name of the table to create
        columns_schema: A list of Column objects representing the columns of the table

    Returns: True if the table was created successfully, False otherwise
    """
    field_type_list = [f"{k} {v}" for k, v in columns_schema.items()]
    # Construct the create table query
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_fqn} (
        {', '.join(field_type_list)}
    )
    """

    # Execute the query
    cursor = conn.cursor()
    try:
        cursor.execute(create_table_query)
        return True
    except ProgrammingError as e:
        logger.error(f"Error creating table: {e}")
        return False


def get_valid_schemas_tables_columns_df(
    conn: SnowflakeConnection, table_fqn: str
) -> pd.DataFrame:
    database_name, schema_name, table_name = table_fqn.split(".")

    query = f"""
        select
            c.{_COLUMN_NAME_COL},
            c.{_DATATYPE_COL},
            c.{_COMMENT_COL} as {_COLUMN_COMMENT_ALIAS},
            t.{_COMMENT_COL} as {_TABLE_COMMENT_COL}
        from {database_name}.information_schema.tables as t
        join {database_name}.information_schema.columns as c
        on true
        and t.table_schema = c.table_schema
        and t.table_name = c.table_name
        and t.table_name ilike '{table_name}'
        where t.table_schema ilike '{schema_name}'
        order by c.ordinal_position
    """
    cursor_execute = conn.cursor().execute(query)
    columns_df = cursor_execute.fetch_pandas_all()  # type: ignore[union-attr]
    return columns_df


def get_table_hash(conn: SnowflakeConnection, table_fqn: str) -> str:
    query = f"SELECT HASH_AGG(*)::VARCHAR AS TABLE_HASH FROM {table_fqn};"
    cursor = conn.cursor()
    cursor.execute(query)
    query_result = cursor.fetch_pandas_all()
    return query_result["TABLE_HASH"].item()  # type: ignore[no-any-return]


def execute_query(conn: SnowflakeConnection, query: str) -> Union[pd.DataFrame, str]:
    try:
        if query == "":
            raise ValueError("Query string is empty")
        cursor = conn.cursor()
        cursor.execute(query)
        query_result = cursor.fetch_pandas_all()
        return query_result
    except Exception as e:
        logger.info(f"Query execution failed: {e}")
        return str(e)


class SnowflakeConnector:
    def __init__(self, account_name: str, max_workers: int = 1):
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

        if _QUERY_TAG:
            connection.cursor().execute(f"ALTER SESSION SET QUERY_TAG = '{_QUERY_TAG}'")
        connection.cursor().execute(
            f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {env_vars.DEFAULT_SESSION_TIMEOUT_SEC}"
        )
        return connection

    def _close_connection(self, connection: SnowflakeConnection) -> None:
        connection.close()

    def execute(
        self, connection: SnowflakeConnection, query: str
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
