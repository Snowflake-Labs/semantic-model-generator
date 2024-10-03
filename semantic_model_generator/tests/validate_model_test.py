import tempfile
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from strictyaml import DuplicateKeysDisallowed, YAMLValidationError

from semantic_model_generator.data_processing.proto_utils import proto_to_yaml
from semantic_model_generator.tests.samples import validate_yamls
from semantic_model_generator.validate_model import validate_from_local_path


@pytest.fixture
def mock_snowflake_connection():
    """Fixture to mock the snowflake_connection function."""
    with patch(
        "semantic_model_generator.snowflake_utils.snowflake_connector.snowflake_connection"
    ) as mock:
        mock.return_value = MagicMock()
        yield mock


@pytest.fixture
def temp_valid_yaml_file_flow_style():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(validate_yamls._VALID_YAML_FLOW_STYLE)
        tmp.flush()  # Ensure all data is written to the file
        yield tmp.name


@pytest.fixture
def temp_valid_yaml_file():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(validate_yamls._VALID_YAML)
        tmp.flush()  # Ensure all data is written to the file
        yield tmp.name


@pytest.fixture
def temp_valid_yaml_file_long_vqr_context():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(validate_yamls._VALID_YAML_LONG_VQR_CONTEXT)
        tmp.flush()  # Ensure all data is written to the file
        yield tmp.name


@pytest.fixture
def temp_invalid_yaml_formatting_file():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(validate_yamls._INVALID_YAML_FORMATTING)
        tmp.flush()
        yield tmp.name


@pytest.fixture
def temp_invalid_yaml_uppercase_file():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(validate_yamls._INVALID_YAML_UPPERCASE_DEFAULT_AGG)
        tmp.flush()
        yield tmp.name


@pytest.fixture
def temp_invalid_yaml_unmatched_quote_file():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(validate_yamls._INVALID_YAML_UNMATCHED_QUOTE)
        tmp.flush()
        yield tmp.name


@pytest.fixture
def temp_invalid_yaml_incorrect_dtype():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(validate_yamls._INVALID_YAML_INCORRECT_DATA_TYPE)
        tmp.flush()
        yield tmp.name


@pytest.fixture
def temp_invalid_yaml_too_long_context():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(validate_yamls._INVALID_YAML_TOO_LONG_CONTEXT)
        tmp.flush()
        yield tmp.name


@pytest.fixture
def temp_valid_yaml_with_verified_query():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(validate_yamls._VALID_YAML_WITH_SINGLE_VERIFIED_QUERY)
        tmp.flush()
        yield tmp.name


@pytest.fixture
def temp_invalid_yaml_duplicate_verified_queries():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(validate_yamls._INVALID_YAML_DUPLICATE_VERIFIED_QUERIES)
        tmp.flush()
        yield tmp.name


@mock.patch("semantic_model_generator.validate_model.logger")
def test_valid_yaml_flow_style(
    mock_logger, temp_valid_yaml_file_flow_style, mock_snowflake_connection
):
    validate_from_local_path(temp_valid_yaml_file_flow_style, mock_snowflake_connection)


@mock.patch("semantic_model_generator.validate_model.logger")
def test_valid_yaml(mock_logger, temp_valid_yaml_file, mock_snowflake_connection):
    validate_from_local_path(temp_valid_yaml_file, mock_snowflake_connection)

    expected_log_call_1 = mock.call.info("Successfully validated!")
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
    snowflake_query_one = "WITH __ALIAS AS (SELECT ALIAS, ZIP_CODE FROM AUTOSQL_DATASET_BIRD_V2.ADDRESS.ALIAS) SELECT * FROM __ALIAS LIMIT 1"
    snowflake_query_two = "WITH __AREA_CODE AS (SELECT ZIP_CODE, AREA_CODE FROM AUTOSQL_DATASET_BIRD_V2.ADDRESS.AREA_CODE) SELECT * FROM __AREA_CODE LIMIT 1"
    assert any(
        snowflake_query_one in str(call)
        for call in mock_snowflake_connection.mock_calls
    ), "Query not executed"
    assert any(
        snowflake_query_two in str(call)
        for call in mock_snowflake_connection.mock_calls
    ), "Query not executed"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_valid_yaml_with_long_vqr_context(
    mock_logger, temp_valid_yaml_file_long_vqr_context, mock_snowflake_connection
):
    validate_from_local_path(
        temp_valid_yaml_file_long_vqr_context, mock_snowflake_connection
    )

    expected_log_call_1 = mock.call.info("Successfully validated!")
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
    snowflake_query_one = "WITH __ALIAS AS (SELECT ALIAS, ZIP_CODE FROM AUTOSQL_DATASET_BIRD_V2.ADDRESS.ALIAS) SELECT * FROM __ALIAS LIMIT 1"
    snowflake_query_two = "WITH __AREA_CODE AS (SELECT ZIP_CODE, AREA_CODE FROM AUTOSQL_DATASET_BIRD_V2.ADDRESS.AREA_CODE) SELECT * FROM __AREA_CODE LIMIT 1"
    assert any(
        snowflake_query_one in str(call)
        for call in mock_snowflake_connection.mock_calls
    ), "Query not executed"
    assert any(
        snowflake_query_two in str(call)
        for call in mock_snowflake_connection.mock_calls
    ), "Query not executed"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_invalid_yaml_formatting(
    mock_logger, temp_invalid_yaml_formatting_file, mock_snowflake_connection
):
    with pytest.raises(DuplicateKeysDisallowed):
        validate_from_local_path(
            temp_invalid_yaml_formatting_file, mock_snowflake_connection
        )

    expected_log_call = mock.call.info("Successfully validated!")
    assert (
        expected_log_call not in mock_logger.mock_calls
    ), "Unexpected log message found in logger calls"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_invalid_yaml_uppercase(
    mock_logger, temp_invalid_yaml_uppercase_file, mock_snowflake_connection
):
    with pytest.raises(
        YAMLValidationError, match=".*when expecting one of: aggregation_type_unknown.*"
    ):
        validate_from_local_path(
            temp_invalid_yaml_uppercase_file, mock_snowflake_connection
        )

    expected_log_call = mock.call.info("Successfully validated!")
    assert (
        expected_log_call not in mock_logger.mock_calls
    ), "Unexpected log message found in logger calls"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_invalid_yaml_missing_quote(
    mock_logger, temp_invalid_yaml_unmatched_quote_file, mock_snowflake_connection
):
    with pytest.raises(YAMLValidationError) as exc_info:
        validate_from_local_path(
            temp_invalid_yaml_unmatched_quote_file, mock_snowflake_connection
        )

    expected_error_fragment = "name can only contain letters, underscores, decimal digits (0-9), and dollar signs ($)."
    assert expected_error_fragment in str(exc_info.value), "Unexpected error message"

    expected_log_call = mock.call.info("Successfully validated!")

    assert (
        expected_log_call not in mock_logger.mock_calls
    ), "Unexpected log message found in logger calls"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_invalid_yaml_incorrect_datatype(
    mock_logger, temp_invalid_yaml_incorrect_dtype, mock_snowflake_connection
):
    with pytest.raises(ValueError) as exc_info:
        validate_from_local_path(
            temp_invalid_yaml_incorrect_dtype, mock_snowflake_connection
        )

    expected_error = "Unable to validate your semantic model. Error = We do not support object datatypes in the semantic model. Col ZIP_CODE has data type OBJECT. Please remove this column from your semantic model or flatten it to non-object type."

    assert expected_error in str(exc_info.value), "Unexpected error message"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_invalid_yaml_too_long_context(
    mock_logger, temp_invalid_yaml_too_long_context, mock_snowflake_connection
):
    account_name = "snowflake test"
    with pytest.raises(ValueError) as exc_info:
        validate_from_local_path(temp_invalid_yaml_too_long_context, account_name)

    expected_error = (
        "Your semantic model is too large. "
        "Passed size is 164952 characters. "
        "We need you to remove 41032 characters in your semantic model. Please check: \n"
        " (1) If you have long descriptions that can be truncated. \n"
        " (2) If you can remove some columns that are not used within your tables. \n"
        " (3) If you have extra tables you do not need."
    )

    assert expected_error in str(exc_info.value), "Unexpected error message"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_valid_yaml_many_sample_values(mock_logger, mock_snowflake_connection):
    yaml = proto_to_yaml(validate_yamls._VALID_YAML_MANY_SAMPLE_VALUES)
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(yaml)
        tmp.flush()
        assert validate_from_local_path(tmp.name, mock_snowflake_connection) is None


@mock.patch("semantic_model_generator.validate_model.logger")
def test_invalid_yaml_duplicate_verified_queries(
    mock_logger, temp_invalid_yaml_duplicate_verified_queries, mock_snowflake_connection
):
    with pytest.raises(
        YAMLValidationError,
        match=r"Duplicate verified query found\.\n  in \"semantic model\", line \d+, column \d+:\n    verified_queries:\n    \^ \(line: \d+\)\ndaily cumulative expenses in 2023 dec\n  in \"semantic model\", line \d+, column \d+:\n      verified_by: renee\n    \^ \(line: \d+\)",
    ):
        validate_from_local_path(
            temp_invalid_yaml_duplicate_verified_queries, mock_snowflake_connection
        )
