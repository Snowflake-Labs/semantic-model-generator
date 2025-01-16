from unittest import mock
from unittest.mock import call, patch

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
