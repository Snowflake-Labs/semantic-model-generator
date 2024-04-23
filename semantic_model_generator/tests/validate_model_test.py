import tempfile
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from semantic_model_generator.validate_model import validate


@pytest.fixture
def mock_snowflake_connection():
    """Fixture to mock the snowflake_connection function."""
    with patch("semantic_model_generator.validate_model.SnowflakeConnector") as mock:
        mock.return_value = MagicMock()
        yield mock


_VALID_YAML = """name: my test semantic model
tables:
  - name: ALIAS
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: ALIAS
    dimensions:
      - name: ALIAS
        synonyms:
            - 'an alias for something'
        expr: ALIAS
        data_type: TEXT
        sample_values:
          - Holtsville
          - Adjuntas
          - Boqueron
    measures:
      - name: ZIP_CODE
        synonyms:
            - 'another synonym'
        expr: ZIP_CODE
        data_type: NUMBER
        sample_values:
          - '501'
  - name: AREA_CODE
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: AREA_CODE
    measures:
      - name: ZIP_CODE
        expr: ZIP_CODE
        data_type: NUMBER
        sample_values:
          - '501'
          - '544'
      - name: AREA_CODE
        expr: AREA_CODE
        data_type: NUMBER
        sample_values:
          - '631'
"""

_INVALID_YAML_FORMATTING = """name: my test semantic model
tables:
  - name: ALIAS
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: ALIAS
    dimensions:
    - name: ALIAS
    synonyms:
        - 'an alias for something'
    expr: ALIAS
    data_type: TEXT
    sample_values:
        - Holtsville
        - Adjuntas
        - Boqueron
    measures:
    - name: ZIP_CODE
    synonyms:
        - 'another synonym'
    expr: ZIP_CODE
    data_type: NUMBER
    sample_values:
        - '501'
  - name: AREA_CODE
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: AREA_CODE
    measures:
      - name: ZIP_CODE
        expr: ZIP_CODE
        data_type: NUMBER
        sample_values:
          - '501'
          - '544'
      - name: AREA_CODE
        expr: AREA_CODE
        data_type: NUMBER
        sample_values:
          - '631'
"""


@pytest.fixture
def temp_valid_yaml_file():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(_VALID_YAML)
        tmp.flush()  # Ensure all data is written to the file
        yield tmp.name


@pytest.fixture
def temp_invalid_yaml_formatting_file():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(_INVALID_YAML_FORMATTING)
        tmp.flush()
        yield tmp.name


@mock.patch("semantic_model_generator.validate_model.logger")
def test_valid_yaml(mock_logger, temp_valid_yaml_file, mock_snowflake_connection):
    account_name = "snowflake test"

    validate(temp_valid_yaml_file, account_name)

    expected_log_call_1 = mock.call.info(
        f"Successfully validated {temp_valid_yaml_file}"
    )
    expected_log_call_2 = mock.call.info("Checking logical table: ALIAS")
    expected_log_call_3 = mock.call.info("Validated logical table: ALIAS")
    assert (
        expected_log_call_1 in mock_logger.mock_calls
    ), "Expected log message not found in logger calls"
    assert (
        expected_log_call_2 in mock_logger.mock_calls
    ), "Expected log message not found in logger calls"
    assert (
        expected_log_call_3 in mock_logger.mock_calls
    ), "Expected log message not found in logger calls"
    snowflake_query_one = (
        "SELECT ALIAS, ZIP_CODE FROM AUTOSQL_DATASET_BIRD_V2.ADDRESS.ALIAS LIMIT 100"
    )
    snowflake_query_two = (
        "SELECT ALIAS, ZIP_CODE FROM AUTOSQL_DATASET_BIRD_V2.ADDRESS.ALIAS LIMIT 100"
    )
    assert any(
        snowflake_query_one in str(call)
        for call in mock_snowflake_connection.mock_calls
    ), "Query not executed"
    assert any(
        snowflake_query_two in str(call)
        for call in mock_snowflake_connection.mock_calls
    ), "Query not executed"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_invalid_yaml_formatting(mock_logger, temp_invalid_yaml_formatting_file):
    account_name = "snowflake test"
    with pytest.raises(ValueError) as exc_info:
        validate(temp_invalid_yaml_formatting_file, account_name)

    expected_error_fragment = (
        "Failed to parse tables field: "
        'Message type "semantic_model_generator.Table" has no field named "expr" at "SemanticModel.tables[0]".'
    )
    assert expected_error_fragment in str(exc_info.value), "Unexpected error message"

    expected_log_call = mock.call.info(
        f"Successfully validated {temp_invalid_yaml_formatting_file}"
    )
    assert (
        expected_log_call not in mock_logger.mock_calls
    ), "Unexpected log message found in logger calls"
