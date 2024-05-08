import jsonargparse
from loguru import logger

from semantic_model_generator.data_processing.proto_utils import yaml_to_semantic_model
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    SnowflakeConnector,
)
from semantic_model_generator.sqlgen.generate_sql import generate_select_with_all_cols


def validate(yaml_path: str, snowflake_account: str) -> None:
    """
    For now, validate just ensures that the yaml is correctly formatted and we can parse into our protos.

    yaml_path: str The absolute path to the location of your yaml file. Something like path/to/your/file.yaml.
    snowflake_account: str The name of the snowflake account.

    TODO: ensure that all expressions are valid by running a query containing all columns and expressions.
    """
    with open(yaml_path) as f:
        yaml_str = f.read()
    model = yaml_to_semantic_model(yaml_str)

    connector = SnowflakeConnector(
        account_name=snowflake_account,
        max_workers=1,
    )

    for table in model.tables:
        logger.info(f"Checking logical table: {table.name}")
        # Each table can be a different database/schema.
        # Create new connection for each one.
        with connector.connect(
            db_name=table.base_table.database, schema_name=table.base_table.schema
        ) as conn:
            try:
                select = generate_select_with_all_cols(table, 100)
                # Run the query
                _ = conn.cursor().execute(select)
            except Exception as e:
                raise ValueError(
                    f"Unable to execute query with your base table against physical tables on Snowflake. Error = {e}"
                )
            logger.info(f"Validated logical table: {table.name}")

    logger.info(f"Successfully validated {yaml_path}")


if __name__ == "__main__":
    jsonargparse.CLI(
        validate,
        as_positional=False,
    )
