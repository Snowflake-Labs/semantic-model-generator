import string
from datetime import datetime

import jsonargparse
from loguru import logger

from semantic_model_generator.data_processing import data_types, proto_utils
from semantic_model_generator.protos import semantic_model_pb2
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    DIMENSION_DATATYPE_COMMON_NAME,
    MEASURE_DATATYPE_COMMON_NAME,
    TIME_MEASURE_DATATYPE_COMMON_NAME,
    SnowflakeConnector,
    get_table_representation,
    get_valid_schemas_tables_columns_df,
)
from semantic_model_generator.snowflake_utils.utils import create_fqn_table

_PLACEHOLDER_COMMENT = "  "
_FILL_OUT_TOKEN = " # <FILL-OUT>"


def _expr_to_name(expr: str) -> str:
    return expr.translate(
        expr.maketrans(string.punctuation, " " * len(string.punctuation))
    ).title()


def _get_placeholder_filter() -> list[semantic_model_pb2.NamedFilter]:
    return [
        semantic_model_pb2.NamedFilter(
            name=_PLACEHOLDER_COMMENT,
            synonyms=[_PLACEHOLDER_COMMENT],
            description=_PLACEHOLDER_COMMENT,
            expr=_PLACEHOLDER_COMMENT,
        )
    ]


def _raw_table_to_semantic_context_table(
    database: str, schema: str, raw_table: data_types.Table
) -> semantic_model_pb2.Table:
    """
    Converts a raw table representation to a semantic model table in protobuf format.

    Args:
        database (str): The name of the database containing the table.
        schema (str): The name of the schema containing the table.
        raw_table (data_types.Table): The raw table object to be transformed.

    Returns:
        semantic_model_pb2.Table: A protobuf representation of the semantic table.

    This function categorizes table columns into TimeDimensions, Dimensions, or Measures based on their data type,
    populates them with sample values, and sets placeholders for descriptions and filters.
    """

    # For each columns, decide if it is a TimeDimension, Measure, or Dimension column.
    # For now, we decide this based on datatype.
    # Any time datatype, is TimeDimension.
    # Any varchar/text is Dimension.
    # Any numerical column is Measure.

    time_dimensions = []
    dimensions = []
    measures = []

    for col in raw_table.columns:

        if col.column_type == TIME_MEASURE_DATATYPE_COMMON_NAME:
            time_dimensions.append(
                semantic_model_pb2.TimeDimension(
                    name=_expr_to_name(col.column_name),
                    expr=col.column_name,
                    data_type=col.column_type,
                    sample_values=col.values,
                    synonyms=[_PLACEHOLDER_COMMENT],
                    description=_PLACEHOLDER_COMMENT,
                )
            )

        elif col.column_type == DIMENSION_DATATYPE_COMMON_NAME:
            dimensions.append(
                semantic_model_pb2.Dimension(
                    name=_expr_to_name(col.column_name),
                    expr=col.column_name,
                    data_type=col.column_type,
                    sample_values=col.values,
                    synonyms=[_PLACEHOLDER_COMMENT],
                    description=_PLACEHOLDER_COMMENT,
                )
            )

        elif col.column_type == MEASURE_DATATYPE_COMMON_NAME:
            measures.append(
                semantic_model_pb2.Measure(
                    name=_expr_to_name(col.column_name),
                    expr=col.column_name,
                    data_type=col.column_type,
                    sample_values=col.values,
                    synonyms=[_PLACEHOLDER_COMMENT],
                    description=_PLACEHOLDER_COMMENT,
                )
            )

    return semantic_model_pb2.Table(
        name=_expr_to_name(raw_table.name),
        base_table=semantic_model_pb2.FullyQualifiedTable(
            database=database, schema=schema, table=raw_table.name
        ),
        # For fields we can not automatically infer, leave a comment for the user to fill out.
        description=_PLACEHOLDER_COMMENT,
        filters=_get_placeholder_filter(),
        dimensions=dimensions,
        time_dimensions=time_dimensions,
        measures=measures,
    )


def raw_schema_to_semantic_context(
    fqn_tables: list[str], snowflake_account: str
) -> tuple[semantic_model_pb2.SemanticModel, str]:
    """
    Converts a list of fully qualified Snowflake table names into a semantic model.

    Parameters:
    - fqn_tables (list[str]): Fully qualified table names to include in the semantic model.
    - snowflake_account (str): Snowflake account identifier.

    Returns:
    - tuple: A tuple containing the semantic model (semantic_model_pb2.SemanticModel) and the model name (str).

    This function fetches metadata for the specified tables, performs schema validation, extracts key information,
    enriches metadata from the Snowflake database, and constructs a semantic model in protobuf format.
    It handles different databases and schemas within the same account by creating unique Snowflake connections as needed.

    Raises:
    - AssertionError: If no valid tables are found in the specified schema.
    """
    connector = SnowflakeConnector(
        account_name=snowflake_account,
        ndv_per_column=3,  # number of sample values to pull per column.
        max_workers=1,
    )
    # For FQN tables, create a new snowflake connection per table in case the db/schema is different.
    table_objects = []
    unique_database_schema: list[str] = []
    for table in fqn_tables:
        # Verify this is a valid FQN table. For now, we check that the table follows the following format.
        # {database}.{schema}.{table}
        fqn_table = create_fqn_table(table)
        fqn_databse_schema = f"{fqn_table.database}_{fqn_table.schema}"
        if fqn_databse_schema not in unique_database_schema:
            unique_database_schema.append(fqn_databse_schema)

        with connector.connect(
            db_name=fqn_table.database, schema_name=fqn_table.schema
        ) as conn:
            logger.info(f"Pulling column information from {fqn_table}")
            valid_schemas_tables_columns_df = get_valid_schemas_tables_columns_df(
                conn=conn, table_schema=fqn_table.schema, table_names=[fqn_table.table]
            )
            assert not valid_schemas_tables_columns_df.empty

            # get the valid columns for this table.
            valid_columns_df_this_table = valid_schemas_tables_columns_df[
                valid_schemas_tables_columns_df["TABLE_NAME"] == fqn_table.table
            ]

            raw_table = get_table_representation(
                conn=conn,
                schema_name=fqn_table.schema,
                table_name=fqn_table.table,
                table_index=0,
                ndv_per_column=3,
                columns_df=valid_columns_df_this_table,
                max_workers=1,
            )

            table_object = _raw_table_to_semantic_context_table(
                database=fqn_table.database,
                schema=fqn_table.schema,
                raw_table=raw_table,
            )
            table_objects.append(table_object)
    semantic_model_name = "_".join(unique_database_schema)
    context = semantic_model_pb2.SemanticModel(
        name=_expr_to_name(semantic_model_name), tables=table_objects
    )
    return context, semantic_model_name


def append_comment_to_placeholders(yaml_str: str) -> str:
    """
    Finds all instances of a specified placeholder in a YAML string and appends a given text to these placeholders.
    This is the homework to fill out after your yaml is generated.

    Args:
    - yaml_str (str): The YAML string to process.

    Returns:
    - str: The modified YAML string with appended text to placeholders.
    """
    updated_yaml = []
    # Split the string into lines to process each line individually
    lines = yaml_str.split("\n")

    for line in lines:
        # Check if the placeholder is in the current line.
        # Strip the last quote to match.
        if line.rstrip("'").endswith(_PLACEHOLDER_COMMENT):
            # Replace the _PLACEHOLDER_COMMENT with itself plus the append_text
            updated_line = line + _FILL_OUT_TOKEN
            updated_yaml.append(updated_line)
        else:
            updated_yaml.append(line)

    # Join the lines back together into a single string
    return "\n".join(updated_yaml)


def generate_base_semantic_context_from_snowflake(
    fqn_tables: list[str],
    snowflake_account: str,
    output_yaml_path: str | None = None,
) -> None:
    """
    Generates a base semantic context from specified Snowflake tables and exports it to a YAML file.

    Args:
        fqn_tables: Fully qualified names of Snowflake tables to include in the semantic context.
        snowflake_account: Identifier of the Snowflake account.
        output_yaml_path: Path for the output YAML file. If None, defaults to 'semantic_model_generator/output_models/YYYYMMDDHHMMSS_<semantic_model_name>.yaml'.

    Returns:
        None. Writes the semantic context to a YAML file.
    """
    context, semantic_model_name = raw_schema_to_semantic_context(
        fqn_tables=fqn_tables,
        snowflake_account=snowflake_account,
    )
    yaml_str = proto_utils.proto_to_yaml(context)
    # Once we have the yaml, update to include to # <FILL-OUT> tokens.
    yaml_str = append_comment_to_placeholders(yaml_str)
    if output_yaml_path:
        write_path = output_yaml_path
    else:
        current_datetime = datetime.now()

        # Format the current date and time as "YYYY-MM-DD"
        formatted_datetime = current_datetime.strftime("%Y%m%d%H%M%S")
        write_path = f"semantic_model_generator/output_models/{formatted_datetime}_{semantic_model_name}.yaml"
    with open(write_path, "w") as f:
        f.write(yaml_str)
    return None


if __name__ == "__main__":
    parser = jsonargparse.ArgumentParser(
        description="CLI tool to generate semantic context models from Snowflake schemas."
    )

    parser.add_argument(
        "--fqn_tables",
        type=list,
        required=True,
        help="The list of fully qualified table names all following the format {database_name}.{schema_name}{table_name}",
    )
    parser.add_argument(
        "--snowflake_account",
        type=str,
        required=True,
        help="Your Snowflake account ID.",
    )
    parser.add_argument(
        "--output_yaml_path",
        type=str,
        required=False,
        help="Custom path to save the YAML. Optional.",
    )

    args = parser.parse_args()

    generate_base_semantic_context_from_snowflake(
        fqn_tables=args.fqn_tables,
        snowflake_account=args.snowflake_account,
        output_yaml_path=args.output_yaml_path,
    )
