import tempfile
from unittest import mock

import pytest

from semantic_model_generator.validate_model import validate

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
def test_valid_yaml(mock_logger, temp_valid_yaml_file):

    _ = validate(temp_valid_yaml_file)

    expected_log_call = mock.call.info(f"Successfully validated {temp_valid_yaml_file}")
    assert (
        expected_log_call in mock_logger.mock_calls
    ), "Expected log message not found in logger calls"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_invalid_yaml_formatting(mock_logger, temp_invalid_yaml_formatting_file):
    with pytest.raises(ValueError) as exc_info:
        validate(temp_invalid_yaml_formatting_file)

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
