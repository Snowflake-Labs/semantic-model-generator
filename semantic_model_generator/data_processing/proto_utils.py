import io
import json
from typing import Any, TypeVar

import ruamel.yaml
from google.protobuf import json_format
from google.protobuf.message import Message
from strictyaml import dirty_load

from semantic_model_generator.data_processing.sql_parsing import extract_table_columns
from semantic_model_generator.protos import semantic_model_pb2
from semantic_model_generator.protos.semantic_model_pb2 import SemanticModel
from semantic_model_generator.validate.schema import SCHEMA

ProtoMsg = TypeVar("ProtoMsg", bound=Message)


def proto_to_yaml(message: ProtoMsg) -> str:
    """Serializes the input proto into a yaml message.

    Args:
        message: Protobuf message to be serialized.

    Returns:
        The serialized yaml string, or None if an error occurs.
    """
    try:
        json_data = json.loads(
            json_format.MessageToJson(message, preserving_proto_field_name=True)
        )

        # Using ruamel.yaml package to preserve message order.
        yaml = ruamel.yaml.YAML()
        yaml.indent(mapping=2, sequence=4, offset=2)
        yaml.preserve_quotes = True

        with io.StringIO() as stream:
            yaml.dump(json_data, stream)
            yaml_str = stream.getvalue()
        assert isinstance(yaml_str, str)
        return yaml_str
    except Exception as e:
        raise ValueError(f"Failed to convert protobuf message to YAML: {e}")


def proto_to_dict(message: ProtoMsg) -> dict[str, Any]:
    """Serializes the input proto into a dictionary.

    Args:
        message: Protobuf message to be serialized.

    Returns:
        The serialized dictionary, or None if an error occurs.
    """
    try:
        # Convert the Protobuf message to JSON string.
        json_str = json_format.MessageToJson(message, preserving_proto_field_name=True)

        # Convert the JSON string to a Python dictionary.
        json_data = json.loads(json_str)

        assert isinstance(json_data, dict)
        return json_data
    except Exception as e:
        raise ValueError(f"Failed to convert protobuf message to dictionary: {e}")


def context_to_column_format(ctx: SemanticModel) -> SemanticModel:
    """
    Converts SemanticModel from a dimension/measure format to a column format.
    Returns a new SemanticModel object that's in column format.
    """
    ret = SemanticModel()
    ret.CopyFrom(ctx)
    for table in ret.tables:
        column_format = len(table.columns) > 0
        dimension_measure_format = (
            len(table.dimensions) > 0
            or len(table.time_dimensions) > 0
            or len(table.measures) > 0
        )
        if column_format and dimension_measure_format:
            raise ValueError(
                f"table {table.name} defines both columns and dimensions/time_dimensions/measures."
            )
        if column_format:
            continue
        for d in table.dimensions:
            col = semantic_model_pb2.Column()
            col.kind = semantic_model_pb2.ColumnKind.dimension
            col.name = d.name
            col.synonyms.extend(d.synonyms)
            col.description = d.description
            col.expr = d.expr
            col.data_type = d.data_type
            col.unique = d.unique
            col.sample_values.extend(d.sample_values)
            # Do in-memory indexing & and retrieval of sample values
            # for dimensions that don't have a search service defined on them.
            # The number of sample values passed to the model may be capped
            # to the first N, but retrieving the samples values
            # based on the question means that many more values can be added
            # to the semantic model, and only passed to the model when relevant.
            col.index_and_retrieve_values = not d.cortex_search_service_name
            col.cortex_search_service_name = d.cortex_search_service_name
            table.columns.append(col)
        del table.dimensions[:]

        for td in table.time_dimensions:
            col = semantic_model_pb2.Column()
            col.kind = semantic_model_pb2.ColumnKind.time_dimension
            col.name = td.name
            col.synonyms.extend(td.synonyms)
            col.description = td.description
            col.expr = td.expr
            col.data_type = td.data_type
            col.unique = td.unique
            col.sample_values.extend(td.sample_values)
            table.columns.append(col)
        del table.time_dimensions[:]

        for m in table.measures:
            col = semantic_model_pb2.Column()
            col.kind = semantic_model_pb2.ColumnKind.measure
            col.name = m.name
            col.synonyms.extend(m.synonyms)
            col.description = m.description
            col.expr = m.expr
            col.data_type = m.data_type
            col.default_aggregation = m.default_aggregation
            col.sample_values.extend(m.sample_values)
            table.columns.append(col)
        del table.measures[:]
    return ret


def _validate_metric(ctx: SemanticModel) -> None:
    """
    Validates that the semantic model metric definition matches join paths defined.
    """

    def _find_table_by_name(
        ctx: SemanticModel, table_name: str
    ) -> semantic_model_pb2.Table | None:
        for table in ctx.tables:
            if table.name.lower() == table_name.lower():
                return table
        return None

    if not ctx.metrics:
        # No metric exsiting in the definition, exit validation.
        return
    if not ctx.relationships:
        raise ValueError("Semantic model has metrics but no join paths defined.")

    join_pairs = [
        {join.left_table.lower(), join.right_table.lower()}
        for join in ctx.relationships
    ]
    for metric in ctx.metrics:
        # First find all tables referred in the metrics. All columns is supposed to be fully qualified with logical table names.
        # Raises error if:
        # 1. Found any columns not fully qualified with logical table name.
        # 2. Only one logical table referred in a metric, indicating it should be defined as a measure, not a metric.
        # 3. For now only supports metric defined across two tables. Raise error if more than two tables referred.
        # 4. The join path between the two tables must be defined in the semantic model.
        tbl_col_mapping = extract_table_columns(metric.expr)
        non_qualified_cols = tbl_col_mapping.get("")
        if non_qualified_cols and len(non_qualified_cols) > 0:
            raise ValueError(
                f"Error in {metric.name}; Columns within metric definition needs to be qualified with corresponding logical table name."
            )
        tbls_referred = set(key.lower() for key in tbl_col_mapping.keys())
        if len(tbls_referred) == 1:
            raise ValueError(
                f"Error in {metric.name}; Metric calculation only referred to one logical table, please define as a measure, instead of metric"
            )
        if len(tbls_referred) > 2:
            raise ValueError(
                f"Error in {metric.name}; Currently only accept metric defined across two tables"
            )
        if tbls_referred not in join_pairs:
            raise ValueError(
                f"Error in {metric.name}; No direct join relationship defined between {','.join(sorted(tbls_referred))}"
            )

        for k, v in tbl_col_mapping.items():
            tbl = _find_table_by_name(ctx, k)
            if tbl is None:
                raise ValueError(
                    f"Error in {metric.name}; Metric calculation referred to undefined logical table name {k}"
                )

            for col in v:
                if col.lower() not in [c.name.lower() for c in tbl.columns]:  # type: ignore
                    raise ValueError(
                        f"Error in {metric.name}; Metric calculation referred to undefined logical column name {col} in table {k}"
                    )


def yaml_to_semantic_model(yaml_str: str) -> SemanticModel:
    """
    Deserializes the input yaml into a SemanticModel Protobuf message. The
    input yaml must be fully representable as json, so yaml features like
    custom types and block scalars are not supported.

    Args:
        yaml_str: Path to the YAML file.

    Returns:
        The deserialized SemanticModel protobuf message
    """

    # strictyaml is very opinionated on the style of yaml, and rejects yamls that use flow style (e.g. lists with []
    # or maps with {}). See https://hitchdev.com/strictyaml/why/flow-style-removed/. This is purely a style preference
    # and those yamls are still parsable. To allow such yamls, we use dirty_load here, which behaves exactly as the
    # load method but allows flow style.
    parsed_yaml = dirty_load(
        yaml_str, SCHEMA, label="semantic model", allow_flow_style=True
    )
    msg = semantic_model_pb2.SemanticModel()
    ctx: SemanticModel = json_format.ParseDict(parsed_yaml.data, msg)
    col_format_ctx = context_to_column_format(ctx)
    _validate_metric(col_format_ctx)
    return col_format_ctx
