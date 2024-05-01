from datetime import datetime
from typing import List, Optional

import jsonargparse
from loguru import logger

from semantic_model_generator.data_processing import data_types, proto_utils
from semantic_model_generator.protos import semantic_model_pb2
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    DIMENSION_DATATYPES,
    MEASURE_DATATYPES,
    TIME_MEASURE_DATATYPES,
    SnowflakeConnector,
    get_table_representation,
    get_valid_schemas_tables_columns_df,
)
from semantic_model_generator.snowflake_utils.utils import create_fqn_table

_PLACEHOLDER_COMMENT = "  "
_FILL_OUT_TOKEN = " # <FILL-OUT>"


def _get_placeholder_filter() -> List[semantic_model_pb2.NamedFilter]:
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

    # For each column, decide if it is a TimeDimension, Measure, or Dimension column.
    # For now, we decide this based on datatype.
    # Any time datatype, is TimeDimension.
    # Any varchar/text is Dimension.
    # Any numerical column is Measure.

    time_dimensions = []
    dimensions = []
    measures = []

    for col in raw_table.columns:

        if col.column_type in TIME_MEASURE_DATATYPES:
            time_dimensions.append(
                semantic_model_pb2.TimeDimension(
                    name=col.column_name,
                    expr=col.column_name,
                    data_type=col.column_type,
                    sample_values=col.values,
                    synonyms=[_PLACEHOLDER_COMMENT],
                    description=_PLACEHOLDER_COMMENT,
                )
            )

        elif col.column_type in DIMENSION_DATATYPES:
            dimensions.append(
                semantic_model_pb2.Dimension(
                    name=col.column_name,
                    expr=col.column_name,
                    data_type=col.column_type,
                    sample_values=col.values,
                    synonyms=[_PLACEHOLDER_COMMENT],
                    description=_PLACEHOLDER_COMMENT,
                )
            )

        elif col.column_type in MEASURE_DATATYPES:
            measures.append(
                semantic_model_pb2.Measure(
                    name=col.column_name,
                    expr=col.column_name,
                    data_type=col.column_type,
                    sample_values=col.values,
                    synonyms=[_PLACEHOLDER_COMMENT],
                    description=_PLACEHOLDER_COMMENT,
                )
            )
        else:
            logger.warning(
                f"Column datatype does not map to a known datatype. Input was = {col.column_type}. We are going to place as a Dimension for now."
            )
            dimensions.append(
                semantic_model_pb2.Dimension(
                    name=col.column_name,
                    expr=col.column_name,
                    data_type=col.column_type,
                    sample_values=col.values,
                    synonyms=[_PLACEHOLDER_COMMENT],
                    description=_PLACEHOLDER_COMMENT,
                )
            )

    return semantic_model_pb2.Table(
        name=raw_table.name,
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
    physical_tables: List[str], snowflake_account: str, semantic_model_name: str
) -> semantic_model_pb2.SemanticModel:
    """
    Converts a list of fully qualified Snowflake table names into a semantic model.

    Parameters:
        physical_tables  (list[str]): Fully qualified table names to include in the semantic model.
    - snowflake_account (str): Snowflake account identifier.
    - semantic_model_name (str): A meaningful semantic model name.

    Returns:
    - The semantic model (semantic_model_pb2.SemanticModel).

    This function fetches metadata for the specified tables, performs schema validation, extracts key information,
    enriches metadata from the Snowflake database, and constructs a semantic model in protobuf format.
    It handles different databases and schemas within the same account by creating unique Snowflake connections as needed.

    Raises:
    - AssertionError: If no valid tables are found in the specified schema.
    """
    connector = SnowflakeConnector(
        account_name=snowflake_account,
        ndv_per_column=10,  # number of sample values to pull per column.
        max_workers=1,
    )
    # For FQN tables, create a new snowflake connection per table in case the db/schema is different.
    table_objects = []
    unique_database_schema: List[str] = []
    for table in physical_tables:
        # Verify this is a valid FQN table. For now, we check that the table follows the following format.
        # {database}.{schema}.{table}
        fqn_table = create_fqn_table(table)
        fqn_databse_schema = f"{fqn_table.database}_{fqn_table.schema_name}"
        if fqn_databse_schema not in unique_database_schema:
            unique_database_schema.append(fqn_databse_schema)

        with connector.connect(
            db_name=fqn_table.database, schema_name=fqn_table.schema_name
        ) as conn:
            logger.info(f"Pulling column information from {fqn_table}")
            valid_schemas_tables_columns_df = get_valid_schemas_tables_columns_df(
                conn=conn,
                table_schema=fqn_table.schema_name,
                table_names=[fqn_table.table],
            )
            assert not valid_schemas_tables_columns_df.empty

            # get the valid columns for this table.
            valid_columns_df_this_table = valid_schemas_tables_columns_df[
                valid_schemas_tables_columns_df["TABLE_NAME"] == fqn_table.table
            ]

            raw_table = get_table_representation(
                conn=conn,
                schema_name=fqn_table.schema_name,
                table_name=fqn_table.table,
                table_index=0,
                ndv_per_column=10,  # number of sample values to pull per column.
                columns_df=valid_columns_df_this_table,
                max_workers=1,
            )

            table_object = _raw_table_to_semantic_context_table(
                database=fqn_table.database,
                schema=fqn_table.schema_name,
                raw_table=raw_table,
            )
            table_objects.append(table_object)
    # TODO(jhilgart): Call cortex model to generate a semantically friendly name here.
    context = semantic_model_pb2.SemanticModel(
        name=semantic_model_name, tables=table_objects
    )
    return context


def append_comment_to_placeholders(yaml_str: str) -> str:
    """
    Finds all instances of a specified placeholder in a YAML string and appends a given text to these placeholders.
    This is the homework to fill out after your yaml is generated.

    Parameters:
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


def _to_snake_case(s: str) -> str:
    """
    Convert a string into snake case.

    Parameters:
    s (str): The string to convert.

    Returns:
    str: The snake case version of the string.
    """
    # Replace common delimiters with spaces
    s = s.replace("-", " ").replace("_", " ")

    words = s.split(" ")

    # Convert each word to lowercase and join with underscores
    snake_case_str = "_".join([word.lower() for word in words if word]).strip()

    return snake_case_str


def generate_base_semantic_model_from_snowflake(
    physical_tables: List[str],
    snowflake_account: str,
    semantic_model_name: str,
    output_yaml_path: Optional[str] = None,
) -> None:
    """
    Generates a base semantic context from specified Snowflake tables and exports it to a YAML file.

    Parameters:
    physical_tables : Fully qualified names of Snowflake tables to include in the semantic context.
        snowflake_account: Identifier of the Snowflake account.
        semantic_model_name: The human readable model name. This should be semantically meaningful to an organization.
        output_yaml_path: Path for the output YAML file. If None, defaults to 'semantic_model_generator/output_models/YYYYMMDDHHMMSS_<semantic_model_name>.yaml'.

    Returns:
        None. Writes the semantic context to a YAML file.
    """
    formatted_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
    if not output_yaml_path:
        output_yaml_path = f"semantic_model_generator/output_models/{formatted_datetime}_{_to_snake_case(semantic_model_name)}.yaml"
    else: # Assume user gives correct path.
        write_path = output_yaml_path
    context = raw_schema_to_semantic_context(
        physical_tables,
        snowflake_account=snowflake_account,
        semantic_model_name=semantic_model_name,
    )

    yaml_str = proto_utils.proto_to_yaml(context)
    # Once we have the yaml, update to include to # <FILL-OUT> tokens.
    yaml_str = append_comment_to_placeholders(yaml_str)

    with open(write_path, "w") as f:
        f.write(yaml_str)
    logger.info(f"Semantic model saved to {write_path}")
    return None


if __name__ == "__main__":
    parser = jsonargparse.ArgumentParser(
        description="CLI tool to generate semantic context models from Snowflake schemas."
    )

    parser.add_argument(
        "--physical_tables",
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
        "--semantic_model_name",
        type=str,
        required=True,
        help="What is the name of this semantic model? Examples could be (Churn Analysis, Marketing, Sales Prospects ...etc)",
    )
    parser.add_argument(
        "--output_yaml_path",
        type=str,
        required=False,
        help="Custom path to save the YAML. Optional.",
    )

    args = parser.parse_args()

    generate_base_semantic_model_from_snowflake(
        physical_tables=args.physical_tables,
        snowflake_account=args.snowflake_account,
        semantic_model_name=args.semantic_model_name,
        output_yaml_path=args.output_yaml_path,
    )
