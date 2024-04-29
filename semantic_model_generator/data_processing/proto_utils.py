import datetime
import io
import json
from typing import Any, Dict, Type, TypeVar

import ruamel.yaml
import yaml
from google.protobuf import json_format
from google.protobuf.message import Message

from semantic_model_generator.protos import semantic_model_pb2

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


def yaml_to_semantic_model(yaml_str: str) -> semantic_model_pb2.SemanticModel:
    """
    Converts a yaml string into a SemanticModel protobuf
    """
    ctx: semantic_model_pb2.SemanticModel = yaml_to_proto(
        yaml_str, semantic_model_pb2.SemanticModel
    )
    return ctx


def yaml_to_proto(yaml_str: str, proto_type: Type[ProtoMsg]) -> ProtoMsg:
    """Deserializes the input yaml into a Protobuf message.  The input yaml
       must be fully representable as json, so yaml features like custom types
       and block scalers are not supported.

    Args:
        yaml_str: Path to the YAML file.
        proto_type: Type of the protobuf message to deserialize into.

    Returns:
        The deserialized protobuf message, or None if an error occurs.
    """

    try:
        parsed_yaml = yaml.safe_load(io.StringIO(yaml_str))
        if parsed_yaml is None:
            return proto_type()
        _yaml_date2str(parsed_yaml)
        msg = proto_type()
        return json_format.ParseDict(parsed_yaml, msg)
    except yaml.YAMLError as e:
        raise ValueError(
            f"Unable to parse input yaml. Error: {e}. Please make sure spacing is correct."
        )
    except json_format.ParseError as e:
        raise ValueError(f"Unable to parse yaml to protobuf. Error: {e}")
    except Exception as e:
        raise Exception(f"Exception in converting yaml -> protobuf. Error: {e}")


def _yaml_date2str(yaml_dict: Dict[str, Any]) -> None:
    """
    Convert some datetime instances into the correct format for parsing.

    This method converts all sample values of type datetime back to a string so we can load the yaml into the proto.
    """
    if not isinstance(yaml_dict, dict):
        return
    for t in yaml_dict.get("tables", []):
        for c in t.get("columns", []):
            _sample_value_date2str(c)
        for d in t.get("dimensions", []):
            _sample_value_date2str(d)
        for td in t.get("time_dimensions", []):
            _sample_value_date2str(td)
        for m in t.get("measures", []):
            _sample_value_date2str(m)


def _sample_value_date2str(yaml_dict: Dict[str, Any]) -> None:
    """
    helper for _yaml_date2str
    """
    for i, sv in enumerate(yaml_dict.get("sample_values", [])):
        if isinstance(sv, datetime.datetime):
            yaml_dict["sample_values"][i] = str(sv)
