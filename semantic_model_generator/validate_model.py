from snowflake.connector import SnowflakeConnection

from app_utils.chat import send_message


def load_yaml(yaml_path: str) -> str:
    """
    Load local yaml file into str.

    yaml_path: str The absolute path to the location of your yaml file. Something like path/to/your/file.yaml.
    """
    with open(yaml_path) as f:
        yaml_str = f.read()
    return yaml_str


def validate(yaml_str: str, conn: SnowflakeConnection) -> None:
    """
    We perform pseudo-validation by issuing a request to Cortex Analyst with the YAML string as-is, and determining
    whether the request is successful. We don't currently have an explicit validation endpoint available, but validation
    is run at inference time, so this is a reasonable proxy.

    This is done in order to remove the need to sync validation logic locally between these codepaths and Analyst.

    yaml_str: yaml content in string format.
    conn: SnowflakeConnection Snowflake connection to pass in
    """

    dummy_request = [
        {"role": "user", "content": [{"type": "text", "text": "SMG app validation"}]}
    ]
    send_message(conn, yaml_str, dummy_request)


def validate_from_local_path(yaml_path: str, conn: SnowflakeConnection) -> None:
    yaml_str = load_yaml(yaml_path)
    validate(yaml_str, conn)
