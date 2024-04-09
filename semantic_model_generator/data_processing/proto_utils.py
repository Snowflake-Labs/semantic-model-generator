import io
import json
from typing import TypeVar

import ruamel.yaml
from google.protobuf import json_format
from google.protobuf.message import Message

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
