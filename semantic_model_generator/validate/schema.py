# This file is essentially doing DFS in the protobuf Descriptors and storing in the SCHEMA. We start with as the root
# SemanticModel at the bottom of this file. This will automatically pickup any changes to the protobuf (given you run
# the protoc command before to regenerate the python files. Different proto messages can have the same message type as a
# child, so we keep a dict of precomputed types to avoid double computing. This currently does not support cycles in the
# proto definition, but we can add a visited set to this if we ever need to.


from typing import Dict

from google.protobuf.descriptor import Descriptor, EnumDescriptor, FieldDescriptor
from strictyaml import Bool, Decimal, Enum, Int, Map, Optional, Seq, Str, Validator

from semantic_model_generator.protos import semantic_model_pb2

scalar_type_map = {
    FieldDescriptor.TYPE_BOOL: Bool,
    FieldDescriptor.TYPE_STRING: Str,
    FieldDescriptor.TYPE_DOUBLE: Decimal,
    FieldDescriptor.TYPE_FLOAT: Decimal,
    FieldDescriptor.TYPE_INT32: Int,
    FieldDescriptor.TYPE_INT64: Int,
}


def create_schema_for_message(
    message: Descriptor, precomputed_types: Dict[str, Validator]
) -> Validator:
    if message.name in precomputed_types:
        return precomputed_types[message.name]
    message_schema = {}
    for k, v in message.fields_by_name.items():
        if is_optional_field(v):
            message_schema[Optional(k)] = create_schema_for_field(v, precomputed_types)
        else:
            message_schema[k] = create_schema_for_field(v, precomputed_types)
    schema = Map(message_schema)
    precomputed_types[message.name] = schema
    return schema


def create_schema_for_field(
    field_descriptor: FieldDescriptor, precomputed_types: Dict[str, Validator]
) -> Validator:
    if field_descriptor.type == FieldDescriptor.TYPE_MESSAGE:
        field_type = create_schema_for_message(
            field_descriptor.message_type, precomputed_types
        )
    elif field_descriptor.type == FieldDescriptor.TYPE_ENUM:
        field_type = create_schema_for_enum(
            field_descriptor.enum_type, precomputed_types
        )
    elif field_descriptor.type in scalar_type_map:
        field_type = scalar_type_map[field_descriptor.type]()
    else:
        raise Exception(f"unsupported type: {field_descriptor.type}")

    if field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
        field_type = Seq(field_type)

    return field_type


def is_optional_field(field_descriptor: FieldDescriptor) -> bool:
    optional_option = list(
        filter(
            lambda o: o[0].name == "optional",
            field_descriptor.GetOptions().ListFields(),
        )
    )
    # ListFields returns a list of (FieldDescriptor, value) tuples. This checks that the `optional` option is present
    #  and that its value is True
    return len(optional_option) > 0 and optional_option[0][1]


def create_schema_for_enum(
    enum: EnumDescriptor, precomputed_types: Dict[str, Validator]
) -> Validator:
    if enum.name in precomputed_types:
        return precomputed_types[enum.name]
    schema = Enum([v.name for v in enum.values])
    precomputed_types[enum.name] = schema
    return schema


SCHEMA = create_schema_for_message(semantic_model_pb2.SemanticModel.DESCRIPTOR, {})
