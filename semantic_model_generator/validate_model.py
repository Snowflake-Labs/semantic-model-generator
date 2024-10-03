from loguru import logger
from snowflake.connector import SnowflakeConnection

from semantic_model_generator.data_processing.cte_utils import (
    context_to_column_format,
    expand_all_logical_tables_as_ctes,
    generate_select,
    validate_all_cols,
)
from semantic_model_generator.data_processing.proto_utils import yaml_to_semantic_model
from semantic_model_generator.validate.context_length import validate_context_length


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
    For now, validate just ensures that the yaml is correctly formatted and we can parse into our protos.

    yaml_str: yaml content in string format.
    conn: SnowflakeConnection Snowflake connection to pass in

    TODO: ensure that all expressions are valid by running a query containing all columns and expressions.
    """

    model = yaml_to_semantic_model(yaml_str)
    # Validate the context length doesn't exceed max we can support.
    validate_context_length(model, throw_error=True)

    model_in_column_format = context_to_column_format(model)

    for table in model_in_column_format.tables:
        logger.info(f"Checking logical table: {table.name}")
        try:
            validate_all_cols(table)
            sqls = generate_select(table, 1)
            # Run the query.
            # TODO: some expr maybe expensive if contains aggregations or window functions. Move to EXPLAIN?
            for sql in sqls:
                _ = conn.cursor().execute(sql)
        except Exception as e:
            raise ValueError(f"Unable to validate your semantic model. Error = {e}")
        logger.info(f"Validated logical table: {table.name}")

    for vq in model.verified_queries:
        logger.info(f"Checking verified queries for: {vq.question}")
        try:
            vqr_with_ctes = expand_all_logical_tables_as_ctes(
                vq.sql, model_in_column_format
            )
            # Run the query
            _ = conn.cursor().execute(vqr_with_ctes)
        except Exception as e:
            raise ValueError(f"Fail to validate your verified query. Error = {e}")
        logger.info(f"Validated verified query: {vq.question}")

    logger.info("Successfully validated!")


def validate_from_local_path(yaml_path: str, conn: SnowflakeConnection) -> None:
    yaml_str = load_yaml(yaml_path)
    validate(yaml_str, conn)
