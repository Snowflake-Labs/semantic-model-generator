import jsonargparse
from loguru import logger

from semantic_model_generator.data_processing.proto_utils import yaml_to_semantic_model
from semantic_model_generator.sqlgen.generate_sql import generate_select_with_all_cols


def validate(yaml_path: str, snowflake_account: str) -> None:
    """
    For now, validate just ensures that the yaml is correctly formatted and we can parse into our protos.

    TODO: ensure that all expressions are valid by running a query containing all columns and expressions.
    """
    with open(yaml_path) as f:
        yaml_str = f.read()
    model = yaml_to_semantic_model(yaml_str)

    for table in model.tables:
        logger.info(f"Checking logical table: {table.name}")
        try:
            _ = generate_select_with_all_cols(table, 100)
            # TODO: next, run select and validate this works
        except Exception as ex:
            logger.warning(
                f"Failed to generate a select query for logical table {table.name}.  Error: {ex}"
            )
        logger.info(f"Validated logical table: {table.name}")

    logger.info(f"Successfully validated {yaml_path}")


if __name__ == "__main__":
    jsonargparse.CLI(
        validate,
        as_positional=False,
    )
