from typing import Dict, Optional

from snowflake.connector import connect
from snowflake.connector.connection import SnowflakeConnection

from semantic_model_generator.data_processing.data_types import FQNParts


def create_fqn_table(fqn_str: str) -> FQNParts:
    if fqn_str.count(".") != 2:
        raise ValueError(
            "Expected to have a table fully qualified name following the {database}.{schema}.{table} format."
            + f"Instead found {fqn_str}"
        )
    database, schema, table = fqn_str.split(".")
    return FQNParts(database=database, schema_name=schema, table=table)


def create_connection_parameters(
    user: str,
    password: str,
    account: str,
    host: Optional[str] = None,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    authenticator: Optional[str] = None,
) -> Dict[str, str]:
    connection_parameters: Dict[str, str] = dict(
        user=user, password=password, account=account
    )
    if role:
        connection_parameters["role"] = role
    if warehouse:
        connection_parameters["warehouse"] = warehouse
    if database:
        connection_parameters["database"] = database
    if schema:
        connection_parameters["schema"] = schema
    if authenticator:
        connection_parameters["authenticator"] = authenticator
    if host:
        connection_parameters["host"] = host
    return connection_parameters


def _connection(connection_parameters: Dict[str, str]) -> SnowflakeConnection:
    # https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect
    return connect(**connection_parameters)


def snowflake_connection(
    user: str,
    password: str,
    account: str,
    role: str,
    warehouse: str,
    host: Optional[str] = None,
) -> SnowflakeConnection:
    """
    Returns a Snowflake Connection to the specified account.
    """
    return _connection(
        create_connection_parameters(
            user=user,
            password=password,
            host=host,
            account=account,
            role=role,
            warehouse=warehouse,
        )
    )
