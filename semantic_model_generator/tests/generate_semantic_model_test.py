from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pytest
import yaml

from semantic_model_generator.data_processing import proto_utils
from semantic_model_generator.data_processing.data_types import Column, Table
from semantic_model_generator.main import (
    generate_base_semantic_context_from_snowflake,
    raw_schema_to_semantic_context,
)
from semantic_model_generator.protos import semantic_model_pb2
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    SnowflakeConnector,
)


@pytest.fixture
def mock_snowflake_connection():
    """Fixture to mock the snowflake_connection function."""
    with patch(
        "semantic_model_generator.snowflake_utils.snowflake_connector.snowflake_connection"
    ) as mock:
        mock.return_value = MagicMock()
        yield mock


_CONVERTED_TABLE_ALIAS = Table(
    id_=0,
    name="ALIAS",
    columns=[
        Column(
            id_=0,
            column_name="ZIP_CODE",
            column_type="TEXT",
            values=None,
            comment=None,
        ),
        Column(
            id_=1,
            column_name="AREA_CODE",
            column_type="NUMBER",
            values=None,
            comment=None,
        ),
        Column(
            id_=2,
            column_name="BAD_ALIAS",
            column_type="TIMESTAMP",
            values=None,
            comment=None,
        ),
        Column(
            id_=3,
            column_name="CBSA",
            column_type="NUMBER",
            values=None,
            comment=None,
        ),
    ],
    comment=None,
)

_CONVERTED_TABLE_ZIP_CODE = Table(
    id_=0,
    name="PRODUCTS",
    columns=[
        Column(
            id_=0,
            column_name="SKU",
            column_type="NUMBER",
            values=["1", "2", "3"],
            comment=None,
        ),
    ],
    comment=None,
)


@pytest.fixture
def mock_snowflake_connection_env(monkeypatch):
    # Mock environment variable
    monkeypatch.setenv("SNOWFLAKE_HOST", "test_host")

    # Use this fixture to also patch instance methods if needed
    with patch.object(
        SnowflakeConnector, "_get_user", return_value="test_user"
    ), patch.object(
        SnowflakeConnector, "_get_password", return_value="test_password"
    ), patch.object(
        SnowflakeConnector, "_get_role", return_value="test_role"
    ), patch.object(
        SnowflakeConnector, "_get_warehouse", return_value="test_warehouse"
    ), patch.object(
        SnowflakeConnector, "_get_host", return_value="test_host"
    ):
        yield


@pytest.fixture
def mock_dependencies(mock_snowflake_connection):
    valid_schemas_tables_columns_df_alias = pd.DataFrame(
        {
            "TABLE_NAME": ["ALIAS"] * 4,
            "COLUMN_NAME": ["ZIP_CODE", "AREA_CODE", "BAD_ALIAS", "CBSA"],
            "DATA_TYPE": ["VARCHAR", "INTEGER", "DATETIME", "DECIMAL"],
        }
    )
    valid_schemas_tables_columns_df_zip_code = pd.DataFrame(
        {
            "TABLE_NAME": ["PRODUCTS"],
            "COLUMN_NAME": ["SKU"],
            "DATA_TYPE": ["NUMBER"],
        }
    )
    valid_schemas_tables_representations = [
        valid_schemas_tables_columns_df_alias,
        valid_schemas_tables_columns_df_zip_code,
    ]
    table_representations = [
        _CONVERTED_TABLE_ALIAS,  # Value returned on the first call.
        _CONVERTED_TABLE_ZIP_CODE,  # Value returned on the second call.
    ]

    with patch(
        "semantic_model_generator.main.get_valid_schemas_tables_columns_df",
        side_effect=valid_schemas_tables_representations,
    ), patch(
        "semantic_model_generator.main.get_table_representation",
        side_effect=table_representations,
    ):
        yield


def test_raw_schema_to_semantic_context(
    mock_dependencies, mock_snowflake_connection, mock_snowflake_connection_env
):
    want_yaml = "name: Test Db Schema Test\ntables:\n  - name: Alias\n    description: '  '\n    base_table:\n      database: test_db\n      schema: schema_test\n      table: ALIAS\n    filters:\n      - name: '  '\n        synonyms:\n          - '  '\n        description: '  '\n        expr: '  '\n    dimensions:\n      - name: Zip Code\n        synonyms:\n          - '  '\n        description: '  '\n        expr: ZIP_CODE\n        data_type: TEXT\n    time_dimensions:\n      - name: Bad Alias\n        synonyms:\n          - '  '\n        description: '  '\n        expr: BAD_ALIAS\n        data_type: TIMESTAMP\n    measures:\n      - name: Area Code\n        synonyms:\n          - '  '\n        description: '  '\n        expr: AREA_CODE\n        data_type: NUMBER\n      - name: Cbsa\n        synonyms:\n          - '  '\n        description: '  '\n        expr: CBSA\n        data_type: NUMBER\n"

    snowflake_account = "test_account"
    fqn_tables = ["test_db.schema_test.ALIAS"]

    semantic_model, unique_database_schemas = raw_schema_to_semantic_context(
        fqn_tables=fqn_tables, snowflake_account=snowflake_account
    )

    # Assert the result as expected
    assert isinstance(semantic_model, semantic_model_pb2.SemanticModel)
    assert isinstance(unique_database_schemas, str)
    assert len(semantic_model.tables) > 0
    assert unique_database_schemas == "test_db_schema_test"

    result_yaml = proto_utils.proto_to_yaml(semantic_model)
    assert result_yaml == want_yaml

    mock_snowflake_connection.assert_called_once_with(
        user="test_user",
        password="test_password",
        account="test_account",
        role="test_role",
        warehouse="test_warehouse",
        host="test_host",
    )


@patch("builtins.open", new_callable=mock_open)
def test_generate_base_context_with_placeholder_comments(
    mock_file,
    mock_dependencies,
    mock_snowflake_connection,
    mock_snowflake_connection_env,
):

    fqn_tables = ["test_db.schema_test.ALIAS"]
    snowflake_account = "test_account"
    output_path = "output_model_path.yaml"

    generate_base_semantic_context_from_snowflake(
        fqn_tables=fqn_tables,
        snowflake_account=snowflake_account,
        output_yaml_path=output_path,
    )

    mock_file.assert_called_once_with(output_path, "w")
    # Assert file save called with placeholder comments added.
    mock_file().write.assert_called_once_with(
        "name: Test Db Schema Test\ntables:\n  - name: Alias\n    description: '  ' # <FILL-OUT>\n    base_table:\n      database: test_db\n      schema: schema_test\n      table: ALIAS\n    filters:\n      - name: '  ' # <FILL-OUT>\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: '  ' # <FILL-OUT>\n    dimensions:\n      - name: Zip Code\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: ZIP_CODE\n        data_type: TEXT\n    time_dimensions:\n      - name: Bad Alias\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: BAD_ALIAS\n        data_type: TIMESTAMP\n    measures:\n      - name: Area Code\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: AREA_CODE\n        data_type: NUMBER\n      - name: Cbsa\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: CBSA\n        data_type: NUMBER\n"
    )


@patch("builtins.open", new_callable=mock_open)
def test_generate_base_context_with_placeholder_comments_cross_database_cross_schema(
    mock_file,
    mock_dependencies,
    mock_snowflake_connection,
    mock_snowflake_connection_env,
):

    fqn_tables = [
        "test_db.schema_test.ALIAS",
        "a_different_database.a_different_schema.PRODUCTS",
    ]
    snowflake_account = "test_account"
    output_path = "output_model_path.yaml"

    generate_base_semantic_context_from_snowflake(
        fqn_tables=fqn_tables,
        snowflake_account=snowflake_account,
        output_yaml_path=output_path,
    )

    mock_file.assert_called_once_with(output_path, "w")
    # Assert file save called with placeholder comments added along with sample values and cross-database
    mock_file().write.assert_called_once_with(
        "name: Test Db Schema Test A Different Database A Different Schema\ntables:\n  - name: Alias\n    description: '  ' # <FILL-OUT>\n    base_table:\n      database: test_db\n      schema: schema_test\n      table: ALIAS\n    filters:\n      - name: '  ' # <FILL-OUT>\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: '  ' # <FILL-OUT>\n    dimensions:\n      - name: Zip Code\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: ZIP_CODE\n        data_type: TEXT\n    time_dimensions:\n      - name: Bad Alias\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: BAD_ALIAS\n        data_type: TIMESTAMP\n    measures:\n      - name: Area Code\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: AREA_CODE\n        data_type: NUMBER\n      - name: Cbsa\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: CBSA\n        data_type: NUMBER\n  - name: Products\n    description: '  ' # <FILL-OUT>\n    base_table:\n      database: a_different_database\n      schema: a_different_schema\n      table: PRODUCTS\n    filters:\n      - name: '  ' # <FILL-OUT>\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: '  ' # <FILL-OUT>\n    measures:\n      - name: Sku\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: SKU\n        data_type: NUMBER\n        sample_values:\n          - '1'\n          - '2'\n          - '3'\n"
    )


def test_semantic_model_to_yaml() -> None:
    want_yaml = "name: transaction_ctx\ntables:\n  - name: transactions\n    description: A table containing data about financial transactions. Each row contains\n      details of a financial transaction.\n    base_table:\n      database: my_database\n      schema: my_schema\n      table: transactions\n    columns:\n      - name: transaction_id\n        description: A unique id for this transaction.\n        expr: transaction_id\n        data_type: BIGINT\n        kind: dimension\n        unique: true\n      - name: account_id\n        description: The account id that initialized this transaction.\n        expr: account_id\n        data_type: BIGINT\n        kind: dimension\n      - name: initiation_date\n        description: Timestamp when the transaction was initiated. In UTC.\n        expr: initiation_date\n        data_type: DATETIME\n        kind: time_dimension\n      - name: amount\n        description: The amount of this transaction.\n        expr: amount\n        data_type: DECIMAL\n        kind: measure\n        default_aggregation: sum\n"
    got = semantic_model_pb2.SemanticModel(
        name="transaction_ctx",
        tables=[
            semantic_model_pb2.Table(
                name="transactions",
                description="A table containing data about financial transactions. Each row contains details of a financial transaction.",
                base_table=semantic_model_pb2.FullyQualifiedTable(
                    database="my_database",
                    schema="my_schema",
                    table="transactions",
                ),
                columns=[
                    semantic_model_pb2.Column(
                        name="transaction_id",
                        kind=semantic_model_pb2.ColumnKind.dimension,
                        description="A unique id for this transaction.",
                        expr="transaction_id",
                        data_type="BIGINT",
                        unique=True,
                    ),
                    semantic_model_pb2.Column(
                        name="account_id",
                        kind=semantic_model_pb2.ColumnKind.dimension,
                        description="The account id that initialized this transaction.",
                        expr="account_id",
                        data_type="BIGINT",
                        unique=False,
                    ),
                    semantic_model_pb2.Column(
                        name="initiation_date",
                        kind=semantic_model_pb2.ColumnKind.time_dimension,
                        description="Timestamp when the transaction was initiated. In UTC.",
                        expr="initiation_date",
                        data_type="DATETIME",
                        unique=False,
                    ),
                    semantic_model_pb2.Column(
                        name="amount",
                        kind=semantic_model_pb2.ColumnKind.measure,
                        description="The amount of this transaction.",
                        expr="amount",
                        data_type="DECIMAL",
                        default_aggregation=semantic_model_pb2.AggregationType.sum,
                    ),
                ],
            )
        ],
    )
    got_yaml = proto_utils.proto_to_yaml(got)
    assert got_yaml == want_yaml

    # Parse the YAML strings into Python data structures
    want_data = yaml.safe_load(want_yaml)
    got_data = yaml.safe_load(got_yaml)

    # Now compare the data structures
    assert (
        want_data == got_data
    ), "The generated YAML does not match the expected structure."
