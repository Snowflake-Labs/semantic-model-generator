import json
from unittest.mock import MagicMock, patch

from snowflake.connector import SnowflakeConnection

from semantic_model_generator.validate_model import validate


@patch("semantic_model_generator.validate_model.send_message")
def test_validate_success(mock_send_message):
    # Mock the response from send_message to simulate a successful response
    mock_send_message.return_value = {}

    # Call the validate function
    conn = MagicMock(spec=SnowflakeConnection)
    yaml_str = "valid_yaml_content"
    result = validate(yaml_str, conn)

    assert result is None


@patch("semantic_model_generator.validate_model.send_message")
def test_validate_error(mock_send_message):
    # Mock the response from send_message to simulate an error response
    mock_send_message.return_value = {
        "error": json.dumps(
            {
                "message": "This YAML is missing a name. Please use https://github.com/Snowflake-Labs/semantic-model-generator.*"
            }
        )
    }

    # Call the validate function and assert that it raises a ValueError
    conn = MagicMock(spec=SnowflakeConnection)
    yaml_str = "invalid_yaml_content"
    try:
        validate(yaml_str, conn)
    except ValueError as e:
        # Verify that the error message is as expected
        assert str(e) == "This YAML is missing a name."
