import io
import json
from typing import Any, TypeVar

import ruamel.yaml
from google.protobuf import json_format
from google.protobuf.message import Message
from strictyaml import dirty_load

from semantic_model_generator.protos import semantic_model_pb2
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


def yaml_to_semantic_model(yaml_str: str) -> semantic_model_pb2.SemanticModel:
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
    return json_format.ParseDict(parsed_yaml.data, msg)
