from unittest.mock import MagicMock, call, mock_open, patch

import pandas as pd
import pytest
import yaml

from semantic_model_generator.data_processing import proto_utils
from semantic_model_generator.data_processing.data_types import Column, Table
from semantic_model_generator.generate_model import (
    _AUTOGEN_COMMENT_WARNING,
    _to_snake_case,
    generate_base_semantic_model_from_snowflake,
    raw_schema_to_semantic_context,
)
from semantic_model_generator.protos import semantic_model_pb2
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    OBJECT_DATATYPES,
    SnowflakeConnector,
)


def test_to_snake_case():
    text = "Hello World-How are_you"

    assert "hello_world_how_are_you" == _to_snake_case(text)


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
            comment="some column comment",
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
    comment="some table comment",
)


_CONVERTED_TABLE_ALIAS_NEW_DTYPE = Table(
    id_=0,
    name="ALIAS",
    columns=[
        Column(
            id_=0,
            column_name="ZIP_CODE",
            column_type="OBJECT",
            values=None,
            comment=None,
        ),
        Column(
            id_=1,
            column_name="AREA_CODE",
            column_type="ANOTHER_DATATYPE",
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


_TABLE_WITH_OBJECT_COL = Table(
    id_=0,
    name="PRODUCTS",
    columns=[
        Column(
            id_=0,
            column_name="SKU",
            column_type="OBJECT",
            values=["{1:2}", "{2:3}", "{3:4}"],
            comment=None,
        ),
    ],
    comment=None,
)

_TABLE_WITH_MANY_SAMPLE_VALUES = Table(
    id_=0,
    name="PRODUCTS",
    columns=[
        Column(
            id_=0,
            column_name="SKU",
            column_type="NUMBER",
            values=["1", "2", "3"] * 550,
            comment=None,
        ),
    ],
    comment=None,
)

_TABLE_THAT_EXCEEDS_CONTEXT = Table(
    id_=0,
    name="PRODUCTS",
    columns=[
        Column(
            id_=i,
            column_name=f"column_{i}",
            column_type="NUMBER",
            values=["1", "2", "3"],
            comment=None,
        )
        for i in range(800)
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
    ), patch.object(
        SnowflakeConnector, "_get_authenticator", return_value="test_authenticator"
    ), patch.object(
        SnowflakeConnector, "_get_mfa_passcode", return_value="123456"
    ), patch.object(
        SnowflakeConnector, "_is_mfa_passcode_in_password", return_value=False
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
        "semantic_model_generator.generate_model.get_valid_schemas_tables_columns_df",
        side_effect=valid_schemas_tables_representations,
    ), patch(
        "semantic_model_generator.generate_model.get_table_representation",
        side_effect=table_representations,
    ):
        yield


@pytest.fixture
def mock_dependencies_new_dtype(mock_snowflake_connection):
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
        _CONVERTED_TABLE_ALIAS_NEW_DTYPE,  # Value returned on the first call.
    ]

    with patch(
        "semantic_model_generator.generate_model.get_valid_schemas_tables_columns_df",
        side_effect=valid_schemas_tables_representations,
    ), patch(
        "semantic_model_generator.generate_model.get_table_representation",
        side_effect=table_representations,
    ):
        yield


@pytest.fixture
def mock_dependencies_object_dtype(mock_snowflake_connection):
    valid_schemas_tables_columns_df_alias = pd.DataFrame(
        {
            "TABLE_NAME": ["PRODUCTS"],
            "COLUMN_NAME": ["SKU"],
            "DATA_TYPE": ["OBJECT"],
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
        _TABLE_WITH_OBJECT_COL,  # Value returned on the first call.
    ]

    with patch(
        "semantic_model_generator.generate_model.get_valid_schemas_tables_columns_df",
        side_effect=valid_schemas_tables_representations,
    ), patch(
        "semantic_model_generator.generate_model.get_table_representation",
        side_effect=table_representations,
    ):
        yield


@pytest.fixture
def mock_dependencies_exceed_context(mock_snowflake_connection):
    valid_schemas_tables_columns_df_alias = pd.DataFrame(
        {
            "TABLE_NAME": ["PRODUCTS"],
            "COLUMN_NAME": ["SKU"],
            "DATA_TYPE": ["OBJECT"],
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
        _TABLE_THAT_EXCEEDS_CONTEXT,  # Value returned on the first call.
    ]

    with patch(
        "semantic_model_generator.generate_model.get_valid_schemas_tables_columns_df",
        side_effect=valid_schemas_tables_representations,
    ), patch(
        "semantic_model_generator.generate_model.get_table_representation",
        side_effect=table_representations,
    ):
        yield


def test_raw_schema_to_semantic_context(
    mock_dependencies, mock_snowflake_connection, mock_snowflake_connection_env
):
    want_yaml = "name: this is the best semantic model ever\ntables:\n  - name: ALIAS\n    description: some table comment\n    base_table:\n      database: TEST_DB\n      schema: SCHEMA_TEST\n      table: ALIAS\n    filters:\n      - name: '  '\n        synonyms:\n          - '  '\n        description: '  '\n        expr: '  '\n    dimensions:\n      - name: ZIP_CODE\n        synonyms:\n          - '  '\n        description: some column comment\n        expr: ZIP_CODE\n        data_type: TEXT\n    time_dimensions:\n      - name: BAD_ALIAS\n        synonyms:\n          - '  '\n        description: '  '\n        expr: BAD_ALIAS\n        data_type: TIMESTAMP\n    measures:\n      - name: AREA_CODE\n        synonyms:\n          - '  '\n        description: '  '\n        expr: AREA_CODE\n        data_type: NUMBER\n      - name: CBSA\n        synonyms:\n          - '  '\n        description: '  '\n        expr: CBSA\n        data_type: NUMBER\n"

    base_tables = ["test_db.schema_test.ALIAS"]
    semantic_model_name = "this is the best semantic model ever"

    semantic_model = raw_schema_to_semantic_context(
        base_tables=base_tables,
        conn=mock_snowflake_connection,
        semantic_model_name=semantic_model_name,
    )

    # Assert the result as expected
    assert isinstance(semantic_model, semantic_model_pb2.SemanticModel)
    assert len(semantic_model.tables) > 0

    result_yaml = proto_utils.proto_to_yaml(semantic_model)
    assert result_yaml == want_yaml


@patch("builtins.open", new_callable=mock_open)
def test_generate_base_context_with_placeholder_comments(
    mock_file,
    mock_dependencies,
    mock_snowflake_connection,
    mock_snowflake_connection_env,
):
    base_tables = ["test_db.schema_test.ALIAS"]
    output_path = "output_model_path.yaml"
    semantic_model_name = "my awesome semantic model"

    generate_base_semantic_model_from_snowflake(
        base_tables=base_tables,
        conn=mock_snowflake_connection,
        output_yaml_path=output_path,
        semantic_model_name=semantic_model_name,
    )

    mock_file.assert_called_once_with(output_path, "w")
    # Assert file save called with placeholder comments added.
    expected_calls = [
        call(_AUTOGEN_COMMENT_WARNING),
        call(
            "name: my awesome semantic model\ntables:\n  - name: ALIAS\n    description: some table comment\n    base_table:\n      database: TEST_DB\n      schema: SCHEMA_TEST\n      table: ALIAS\n    # filters:\n      # - name: '  ' # <FILL-OUT>\n        # synonyms:\n          # - '  ' # <FILL-OUT>\n        # description: '  ' # <FILL-OUT>\n        # expr: '  ' # <FILL-OUT>\n    dimensions:\n      - name: ZIP_CODE\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: some column comment\n        expr: ZIP_CODE\n        data_type: TEXT\n    time_dimensions:\n      - name: BAD_ALIAS\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: BAD_ALIAS\n        data_type: TIMESTAMP\n    measures:\n      - name: AREA_CODE\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: AREA_CODE\n        data_type: NUMBER\n      - name: CBSA\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: CBSA\n        data_type: NUMBER\n"
        ),
    ]
    mock_file().write.assert_has_calls(expected_calls)
    assert mock_file().write.call_count == 2


@patch("builtins.open", new_callable=mock_open)
def test_generate_base_context_with_placeholder_comments_cross_database_cross_schema(
    mock_file,
    mock_dependencies,
    mock_snowflake_connection,
    mock_snowflake_connection_env,
):
    base_tables = [
        "test_db.schema_test.ALIAS",
        "a_different_database.a_different_schema.PRODUCTS",
    ]
    output_path = "output_model_path.yaml"
    semantic_model_name = "Another Incredible Semantic Model"

    generate_base_semantic_model_from_snowflake(
        base_tables=base_tables,
        conn=mock_snowflake_connection,
        output_yaml_path=output_path,
        semantic_model_name=semantic_model_name,
    )

    mock_file.assert_called_once_with(output_path, "w")
    expected_calls = [
        call(_AUTOGEN_COMMENT_WARNING),
        call(
            "name: Another Incredible Semantic Model\ntables:\n  - name: ALIAS\n    description: some table comment\n    base_table:\n      database: TEST_DB\n      schema: SCHEMA_TEST\n      table: ALIAS\n    # filters:\n      # - name: '  ' # <FILL-OUT>\n        # synonyms:\n          # - '  ' # <FILL-OUT>\n        # description: '  ' # <FILL-OUT>\n        # expr: '  ' # <FILL-OUT>\n    dimensions:\n      - name: ZIP_CODE\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: some column comment\n        expr: ZIP_CODE\n        data_type: TEXT\n    time_dimensions:\n      - name: BAD_ALIAS\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: BAD_ALIAS\n        data_type: TIMESTAMP\n    measures:\n      - name: AREA_CODE\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: AREA_CODE\n        data_type: NUMBER\n      - name: CBSA\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: CBSA\n        data_type: NUMBER\n  - name: PRODUCTS\n    description: '  ' # <FILL-OUT>\n    base_table:\n      database: A_DIFFERENT_DATABASE\n      schema: A_DIFFERENT_SCHEMA\n      table: PRODUCTS\n    # filters:\n      # - name: '  ' # <FILL-OUT>\n        # synonyms:\n          # - '  ' # <FILL-OUT>\n        # description: '  ' # <FILL-OUT>\n        # expr: '  ' # <FILL-OUT>\n    measures:\n      - name: SKU\n        synonyms:\n          - '  ' # <FILL-OUT>\n        description: '  ' # <FILL-OUT>\n        expr: SKU\n        data_type: NUMBER\n        sample_values:\n          - '1'\n          - '2'\n          - '3'\n"
        ),
    ]

    # Assert file save called with placeholder comments added along with sample values and cross-database
    mock_file().write.assert_has_calls(expected_calls)
    assert mock_file().write.call_count == 2


@patch("semantic_model_generator.generate_model.logger")
@patch("builtins.open", new_callable=mock_open)
def test_generate_base_context_with_placeholder_comments_missing_datatype(
    mock_file,
    mock_logger,
    mock_dependencies_new_dtype,
    mock_snowflake_connection,
    mock_snowflake_connection_env,
):
    base_tables = ["test_db.schema_test.ALIAS"]
    output_path = "output_model_path.yaml"
    semantic_model_name = "Another Incredible Semantic Model with new dtypes"

    generate_base_semantic_model_from_snowflake(
        base_tables=base_tables,
        conn=mock_snowflake_connection,
        output_yaml_path=output_path,
        semantic_model_name=semantic_model_name,
    )

    expected_calls = [
        call(
            "We don't currently support OBJECT as an input column datatype to the Semantic Model. We are skipping column ZIP_CODE for now."
        ),
        call(
            "Column datatype does not map to a known datatype. Input was = ANOTHER_DATATYPE. We are going to place as a Dimension for now."
        ),
    ]

    # Assert that all expected calls were made in the exact order
    mock_logger.warning.assert_has_calls(expected_calls, any_order=False)


@patch("semantic_model_generator.generate_model.logger")
@patch("builtins.open", new_callable=mock_open)
def test_generate_base_context_from_table_that_has_not_supported_dtype(
    mock_file,
    mock_logger,
    mock_dependencies_object_dtype,
    mock_snowflake_connection,
    mock_snowflake_connection_env,
):
    base_tables = ["test_db.schema_test.ALIAS"]
    output_path = "output_model_path.yaml"
    semantic_model_name = "Another Incredible Semantic Model with unsupported dtypes"

    with pytest.raises(ValueError) as excinfo:
        generate_base_semantic_model_from_snowflake(
            base_tables=base_tables,
            conn=mock_snowflake_connection,
            output_yaml_path=output_path,
            semantic_model_name=semantic_model_name,
        )
    assert (
        str(excinfo.value)
        == f"No valid columns found for table PRODUCTS. Please verify that this table contains column's datatypes not in {OBJECT_DATATYPES}."
    )

    expected_calls = [
        call(
            "We don't currently support OBJECT as an input column datatype to the Semantic Model. We are skipping column SKU for now."
        ),
    ]

    # Assert that all expected calls were made in the exact order
    mock_logger.warning.assert_has_calls(expected_calls, any_order=False)

    mock_file().write.assert_not_called()


@patch("semantic_model_generator.validate.context_length.logger")
@patch("builtins.open", new_callable=mock_open)
def test_generate_base_context_from_table_that_has_too_long_context(
    mock_file,
    mock_logger,
    mock_dependencies_exceed_context,
    mock_snowflake_connection,
    mock_snowflake_connection_env,
):
    base_tables = ["test_db.schema_test.ALIAS"]
    output_path = "output_model_path.yaml"
    semantic_model_name = "Another Incredible Semantic Model with long context"

    generate_base_semantic_model_from_snowflake(
        base_tables=base_tables,
        conn=mock_snowflake_connection,
        output_yaml_path=output_path,
        semantic_model_name=semantic_model_name,
    )

    mock_file.assert_called_once_with(output_path, "w")
    mock_logger.warning.assert_called_once_with(
        "WARNING 🚨: "
        "The Semantic model is too large. \n"
        "Passed size is 166501 characters. "
        "We need you to remove 42580 characters in your semantic model. "
        "Please check: \n "
        "(1) If you have long descriptions that can be truncated. \n "
        "(2) If you can remove some columns that are not used within your tables. \n "
        "(3) If you have extra tables you do not need. \n "
        "Once you've finished updating, please validate your semantic model."
    )

    mock_file.assert_called_once_with(output_path, "w")


def test_semantic_model_to_yaml() -> None:
    want_yaml = "name: transaction_ctx\ntables:\n  - name: transactions\n    description: A table containing data about financial transactions. Each row contains\n      details of a financial transaction.\n    base_table:\n      database: my_database\n      schema: my_schema\n      table: transactions\n    dimensions:\n      - name: transaction_id\n        description: A unique id for this transaction.\n        expr: transaction_id\n        data_type: BIGINT\n        unique: true\n    time_dimensions:\n      - name: initiation_date\n        description: Timestamp when the transaction was initiated. In UTC.\n        expr: initiation_date\n        data_type: DATETIME\n    measures:\n      - name: amount\n        description: The amount of this transaction.\n        expr: amount\n        data_type: DECIMAL\n        default_aggregation: sum\n"
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
                time_dimensions=[
                    semantic_model_pb2.TimeDimension(
                        name="initiation_date",
                        description="Timestamp when the transaction was initiated. In UTC.",
                        expr="initiation_date",
                        data_type="DATETIME",
                        unique=False,
                    )
                ],
                measures=[
                    semantic_model_pb2.Fact(
                        name="amount",
                        description="The amount of this transaction.",
                        expr="amount",
                        data_type="DECIMAL",
                        default_aggregation=semantic_model_pb2.AggregationType.sum,
                    ),
                ],
                dimensions=[
                    semantic_model_pb2.Dimension(
                        name="transaction_id",
                        description="A unique id for this transaction.",
                        expr="transaction_id",
                        data_type="BIGINT",
                        unique=True,
                    )
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
