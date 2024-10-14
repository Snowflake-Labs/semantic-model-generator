from unittest import mock
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from semantic_model_generator.data_processing.data_types import Column, Table
from semantic_model_generator.snowflake_utils import snowflake_connector


@pytest.fixture
def mock_snowflake_connection_env(monkeypatch):
    # Mock environment variable
    monkeypatch.setenv("SNOWFLAKE_HOST", "test_host")

    # Use this fixture to also patch instance methods if needed
    with patch.object(
        snowflake_connector.SnowflakeConnector, "_get_user", return_value="test_user"
    ), patch.object(
        snowflake_connector.SnowflakeConnector,
        "_get_password",
        return_value="test_password",
    ), patch.object(
        snowflake_connector.SnowflakeConnector, "_get_role", return_value="test_role"
    ), patch.object(
        snowflake_connector.SnowflakeConnector,
        "_get_warehouse",
        return_value="test_warehouse",
    ), patch.object(
        snowflake_connector.SnowflakeConnector, "_get_host", return_value="test_host"
    ):
        yield


@pytest.fixture
def schemas_tables_columns() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "DATA_TYPE",
            "COLUMN_COMMENT",
        ],
        data=[
            ["TEST_SCHEMA_1", "table_1", "col_1", "VARCHAR", None],
            ["TEST_SCHEMA_1", "table_1", "col_2", "NUMBER", None],
            ["TEST_SCHEMA_1", "table_2", "col_1", "NUMBER", "table_2_col_1_comment"],
            [
                "TEST_SCHEMA_1",
                "table_2",
                "col_2",
                "TIMESTAMP_NTZ",
                "table_2_col_2_comment",
            ],
            ["TEST_SCHEMA_2", "table_3", "col_1", "VARIANT", None],
            [
                "TEST_SCHEMA_2",
                "invalid_table",
                "col_1",
                "VARIANT",
                "invalid_table_col_1_comment",
            ],
        ],
    )


@pytest.fixture
def valid_tables() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["TABLE_SCHEMA", "TABLE_NAME", "TABLE_COMMENT"],
        data=[
            ["TEST_SCHEMA_1", "table_1", None],
            ["TEST_SCHEMA_1", "table_2", "table_2_comment"],
            ["TEST_SCHEMA_2", "table_3", "table_3_comment"],
        ],
    )


_TEST_TABLE_ONE = Table(
    id_=0,
    name="table_1",
    columns=[
        Column(
            id_=0,
            column_name="col_1",
            column_type="text",
            is_primary_key=True,
            is_foreign_key=False,
        ),
        Column(
            id_=1,
            column_name="col_2",
            column_type="number",
            is_primary_key=False,
            is_foreign_key=False,
        ),
    ],
)


@mock.patch(
    "semantic_model_generator.snowflake_utils.snowflake_connector.snowflake_connection"
)
def test_connect(
    mock_snowflake_connection: mock.MagicMock, mock_snowflake_connection_env
):
    mock_snowflake_connection.return_value = mock.MagicMock()

    connector = snowflake_connector.SnowflakeConnector(account_name="test_account")
    with connector.connect(db_name="test") as conn:
        pass

    conn.cursor().execute.assert_has_calls(
        [
            call("ALTER SESSION SET QUERY_TAG = 'SEMANTIC_MODEL_GENERATOR'"),
            call("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 120"),
        ]
    )
    conn.close.assert_called_with()


@mock.patch(
    "semantic_model_generator.snowflake_utils.snowflake_connector.snowflake_connection"
)
def test_connect_with_schema(
    mock_snowflake_connection: mock.MagicMock, mock_snowflake_connection_env
):
    mock_snowflake_connection.return_value = mock.MagicMock()

    connector = snowflake_connector.SnowflakeConnector(
        account_name="test_account",
    )
    with connector.connect(db_name="test_db", schema_name="test_schema") as conn:
        pass

    conn.cursor().execute.assert_has_calls(
        [
            call("ALTER SESSION SET QUERY_TAG = 'SEMANTIC_MODEL_GENERATOR'"),
            call("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 120"),
        ]
    )
    conn.close.assert_called_with()


@mock.patch(
    "semantic_model_generator.snowflake_utils.snowflake_connector._fetch_valid_tables_and_views"
)
@mock.patch(
    "semantic_model_generator.snowflake_utils.snowflake_connector.snowflake_connection"
)
def test_get_valid_schema_table_columns_df(
    mock_snowflake_connection: mock.MagicMock,
    mock_valid_tables: mock.MagicMock,
    valid_tables: pd.DataFrame,
    schemas_tables_columns: pd.DataFrame,
):
    mock_conn = mock.MagicMock()
    # We expect get_database_representation() to execute queries in this order:
    # - select from information_schema.tables
    # - select from information_schema.columns for each table.
    mock_conn.cursor().execute().fetch_pandas_all.side_effect = [
        schemas_tables_columns[schemas_tables_columns["TABLE_NAME"] == "table_1"]
    ]
    mock_snowflake_connection.return_value = mock_conn
    mock_valid_tables.return_value = valid_tables

    got = snowflake_connector.get_valid_schemas_tables_columns_df(
        mock_conn, "TEST_DB", "TEST_SCHEMA_1", ["table_1"]
    )

    want_data = {
        "TABLE_SCHEMA": ["TEST_SCHEMA_1", "TEST_SCHEMA_1"],
        "TABLE_NAME": ["table_1", "table_1"],
        "TABLE_COMMENT": [None, None],
        "COLUMN_NAME": ["col_1", "col_2"],
        "DATA_TYPE": ["VARCHAR", "NUMBER"],
        "COLUMN_COMMENT": [None, None],
    }

    # Create a DataFrame
    want = pd.DataFrame(want_data)

    assert_frame_equal(want, got)

    # Assert that the connection executed the expected queries.
    query = "select t.TABLE_SCHEMA, t.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.COMMENT as COLUMN_COMMENT\nfrom TEST_DB.information_schema.tables as t\njoin TEST_DB.information_schema.columns as c on t.table_schema = c.table_schema and t.table_name = c.table_name where t.table_schema ilike 'TEST_SCHEMA_1' AND LOWER(t.table_name) in ('table_1') \norder by 1, 2, c.ordinal_position"
    mock_conn.cursor().execute.assert_any_call(query)


@pytest.fixture
def snowflake_data():
    return [
        # This mimics the return value of cursor.fetchall() for tables and views
        ([("table1", "schema1", "A table comment")], [("column1", "dtype")]),
        ([("view1", "schema1", "A view comment")], [("column1", "dtype")]),
    ]


@pytest.fixture
def expected_df():
    # Expected DataFrame structure based on mocked fetchall data
    return pd.DataFrame(
        {
            snowflake_connector._TABLE_NAME_COL: ["table1", "view1"],
            snowflake_connector._TABLE_SCHEMA_COL: ["schema1", "schema1"],
            snowflake_connector._TABLE_COMMENT_COL: [
                "A table comment",
                "A view comment",
            ],
        }
    )


def test_fetch_valid_tables_and_views(snowflake_data, expected_df):
    # Mock SnowflakeConnection and cursor
    mock_conn = mock.MagicMock()
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.execute.return_value = mock_cursor
    # Set side effects for fetchall and description based on snowflake_data fixture
    mock_cursor.fetchall.side_effect = [snowflake_data[0][0], snowflake_data[1][0]]

    mock_name_one = MagicMock()
    mock_name_one.name = "name"
    mock_name_two = MagicMock()
    mock_name_two.name = "schema_name"
    mock_name_three = MagicMock()
    mock_name_three.name = "comment"

    mocked_descriptions = [mock_name_one, mock_name_two, mock_name_three]
    mock_cursor.description = mocked_descriptions

    # Call the function to test
    result_df = snowflake_connector._fetch_valid_tables_and_views(mock_conn, "mock_db")

    # Assert the result is as expected
    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )

    # Verify execute was called with correct queries
    mock_cursor.execute.assert_any_call("show tables in database mock_db")
    mock_cursor.execute.assert_any_call("show views in database mock_db")
