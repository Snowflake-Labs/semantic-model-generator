# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: semantic_model.proto
# Protobuf Python Version: 5.26.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import descriptor_pb2 as google_dot_protobuf_dot_descriptor__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x14semantic_model.proto\x12\x1c\x63om.snowflake.cortex.analyst\x1a google/protobuf/descriptor.proto\"/\n\x0fRetrievalResult\x12\r\n\x05value\x18\x01 \x01(\t\x12\r\n\x05score\x18\x02 \x01(\x02\"\xc8\x04\n\x06\x43olumn\x12\x12\n\x04name\x18\x01 \x01(\tB\x04\xa0\x82\x19\x01\x12\x16\n\x08synonyms\x18\x02 \x03(\tB\x04\x90\x82\x19\x01\x12\x19\n\x0b\x64\x65scription\x18\x03 \x01(\tB\x04\x90\x82\x19\x01\x12\x12\n\x04\x65xpr\x18\x04 \x01(\tB\x04\x98\x82\x19\x01\x12\x11\n\tdata_type\x18\x05 \x01(\t\x12\x36\n\x04kind\x18\x06 \x01(\x0e\x32(.com.snowflake.cortex.analyst.ColumnKind\x12\x14\n\x06unique\x18\x07 \x01(\x08\x42\x04\x90\x82\x19\x01\x12P\n\x13\x64\x65\x66\x61ult_aggregation\x18\x08 \x01(\x0e\x32-.com.snowflake.cortex.analyst.AggregationTypeB\x04\x90\x82\x19\x01\x12\x1b\n\rsample_values\x18\t \x03(\tB\x04\x90\x82\x19\x01\x12\'\n\x19index_and_retrieve_values\x18\n \x01(\x08\x42\x04\x90\x82\x19\x01\x12O\n\x12retrieved_literals\x18\x0b \x03(\x0b\x32-.com.snowflake.cortex.analyst.RetrievalResultB\x04\x90\x82\x19\x01\x12*\n\x1a\x63ortex_search_service_name\x18\x0c \x01(\tB\x06\x18\x01\x90\x82\x19\x01\x12V\n\x15\x63ortex_search_service\x18\r \x01(\x0b\x32\x31.com.snowflake.cortex.analyst.CortexSearchServiceB\x04\x90\x82\x19\x01\x12\x15\n\x07is_enum\x18\x0e \x01(\x08\x42\x04\x90\x82\x19\x01\"\xc7\x02\n\tDimension\x12\x12\n\x04name\x18\x01 \x01(\tB\x04\xa0\x82\x19\x01\x12\x16\n\x08synonyms\x18\x02 \x03(\tB\x04\x90\x82\x19\x01\x12\x19\n\x0b\x64\x65scription\x18\x03 \x01(\tB\x04\x90\x82\x19\x01\x12\x12\n\x04\x65xpr\x18\x04 \x01(\tB\x04\x98\x82\x19\x01\x12\x11\n\tdata_type\x18\x05 \x01(\t\x12\x14\n\x06unique\x18\x06 \x01(\x08\x42\x04\x90\x82\x19\x01\x12\x1b\n\rsample_values\x18\x07 \x03(\tB\x04\x90\x82\x19\x01\x12V\n\x15\x63ortex_search_service\x18\x08 \x01(\x0b\x32\x31.com.snowflake.cortex.analyst.CortexSearchServiceB\x04\x90\x82\x19\x01\x12*\n\x1a\x63ortex_search_service_name\x18\t \x01(\tB\x06\x18\x01\x90\x82\x19\x01\x12\x15\n\x07is_enum\x18\n \x01(\x08\x42\x04\x90\x82\x19\x01\"r\n\x13\x43ortexSearchService\x12\x16\n\x08\x64\x61tabase\x18\x01 \x01(\tB\x04\x90\x82\x19\x01\x12\x14\n\x06schema\x18\x02 \x01(\tB\x04\x90\x82\x19\x01\x12\x0f\n\x07service\x18\x03 \x01(\t\x12\x1c\n\x0eliteral_column\x18\x04 \x01(\tB\x04\x90\x82\x19\x01\"\xb0\x01\n\rTimeDimension\x12\x12\n\x04name\x18\x01 \x01(\tB\x04\xa0\x82\x19\x01\x12\x16\n\x08synonyms\x18\x02 \x03(\tB\x04\x90\x82\x19\x01\x12\x19\n\x0b\x64\x65scription\x18\x03 \x01(\tB\x04\x90\x82\x19\x01\x12\x12\n\x04\x65xpr\x18\x04 \x01(\tB\x04\x98\x82\x19\x01\x12\x11\n\tdata_type\x18\x05 \x01(\t\x12\x14\n\x06unique\x18\x06 \x01(\x08\x42\x04\x90\x82\x19\x01\x12\x1b\n\rsample_values\x18\x07 \x03(\tB\x04\x90\x82\x19\x01\"\xe3\x01\n\x04\x46\x61\x63t\x12\x12\n\x04name\x18\x01 \x01(\tB\x04\xa0\x82\x19\x01\x12\x16\n\x08synonyms\x18\x02 \x03(\tB\x04\x90\x82\x19\x01\x12\x19\n\x0b\x64\x65scription\x18\x03 \x01(\tB\x04\x90\x82\x19\x01\x12\x12\n\x04\x65xpr\x18\x04 \x01(\tB\x04\x98\x82\x19\x01\x12\x11\n\tdata_type\x18\x05 \x01(\t\x12P\n\x13\x64\x65\x66\x61ult_aggregation\x18\x06 \x01(\x0e\x32-.com.snowflake.cortex.analyst.AggregationTypeB\x04\x90\x82\x19\x01\x12\x1b\n\rsample_values\x18\x07 \x03(\tB\x04\x90\x82\x19\x01\"b\n\x0bNamedFilter\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x16\n\x08synonyms\x18\x02 \x03(\tB\x04\x90\x82\x19\x01\x12\x19\n\x0b\x64\x65scription\x18\x03 \x01(\tB\x04\x90\x82\x19\x01\x12\x12\n\x04\x65xpr\x18\x04 \x01(\tB\x04\x98\x82\x19\x01\"F\n\x13\x46ullyQualifiedTable\x12\x10\n\x08\x64\x61tabase\x18\x01 \x01(\t\x12\x0e\n\x06schema\x18\x02 \x01(\t\x12\r\n\x05table\x18\x03 \x01(\t\"\x1d\n\nPrimaryKey\x12\x0f\n\x07\x63olumns\x18\x01 \x03(\t\"\x7f\n\nForeignKey\x12\x14\n\x0c\x66key_columns\x18\x01 \x03(\t\x12\x45\n\npkey_table\x18\x02 \x01(\x0b\x32\x31.com.snowflake.cortex.analyst.FullyQualifiedTable\x12\x14\n\x0cpkey_columns\x18\x03 \x03(\t\"\xe2\x05\n\x05Table\x12\x12\n\x04name\x18\x01 \x01(\tB\x04\xa0\x82\x19\x01\x12\x16\n\x08synonyms\x18\x02 \x03(\tB\x04\x90\x82\x19\x01\x12\x19\n\x0b\x64\x65scription\x18\x03 \x01(\tB\x04\x90\x82\x19\x01\x12\x45\n\nbase_table\x18\x04 \x01(\x0b\x32\x31.com.snowflake.cortex.analyst.FullyQualifiedTable\x12;\n\x07\x63olumns\x18\x05 \x03(\x0b\x32$.com.snowflake.cortex.analyst.ColumnB\x04\x90\x82\x19\x01\x12\x41\n\ndimensions\x18\t \x03(\x0b\x32\'.com.snowflake.cortex.analyst.DimensionB\x04\x90\x82\x19\x01\x12J\n\x0ftime_dimensions\x18\n \x03(\x0b\x32+.com.snowflake.cortex.analyst.TimeDimensionB\x04\x90\x82\x19\x01\x12<\n\x08measures\x18\x0b \x03(\x0b\x32\".com.snowflake.cortex.analyst.FactB\x06\x18\x01\x90\x82\x19\x01\x12\x37\n\x05\x66\x61\x63ts\x18\x0c \x03(\x0b\x32\".com.snowflake.cortex.analyst.FactB\x04\x90\x82\x19\x01\x12;\n\x07metrics\x18\r \x03(\x0b\x32$.com.snowflake.cortex.analyst.MetricB\x04\x90\x82\x19\x01\x12\x43\n\x0bprimary_key\x18\x06 \x01(\x0b\x32(.com.snowflake.cortex.analyst.PrimaryKeyB\x04\x90\x82\x19\x01\x12\x44\n\x0c\x66oreign_keys\x18\x07 \x03(\x0b\x32(.com.snowflake.cortex.analyst.ForeignKeyB\x04\x90\x82\x19\x01\x12@\n\x07\x66ilters\x18\x08 \x03(\x0b\x32).com.snowflake.cortex.analyst.NamedFilterB\x04\x90\x82\x19\x01\"\xa6\x01\n\x06Metric\x12\x12\n\x04name\x18\x01 \x01(\tB\x04\xa0\x82\x19\x01\x12\x16\n\x08synonyms\x18\x02 \x03(\tB\x04\x90\x82\x19\x01\x12\x19\n\x0b\x64\x65scription\x18\x03 \x01(\tB\x04\x90\x82\x19\x01\x12\x12\n\x04\x65xpr\x18\x04 \x01(\tB\x04\x98\x82\x19\x01\x12\x41\n\x06\x66ilter\x18\x05 \x01(\x0b\x32+.com.snowflake.cortex.analyst.MetricsFilterB\x04\x90\x82\x19\x01\"#\n\rMetricsFilter\x12\x12\n\x04\x65xpr\x18\x01 \x01(\tB\x04\x98\x82\x19\x01\"8\n\x0bRelationKey\x12\x13\n\x0bleft_column\x18\x01 \x01(\t\x12\x14\n\x0cright_column\x18\x02 \x01(\t\"\xb2\x02\n\x0cRelationship\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x12\n\nleft_table\x18\x02 \x01(\t\x12\x13\n\x0bright_table\x18\x03 \x01(\t\x12\x16\n\x04\x65xpr\x18\x04 \x01(\tB\x08\x90\x82\x19\x01\x98\x82\x19\x01\x12M\n\x14relationship_columns\x18\x07 \x03(\x0b\x32).com.snowflake.cortex.analyst.RelationKeyB\x04\x90\x82\x19\x01\x12\x39\n\tjoin_type\x18\x05 \x01(\x0e\x32&.com.snowflake.cortex.analyst.JoinType\x12I\n\x11relationship_type\x18\x06 \x01(\x0e\x32..com.snowflake.cortex.analyst.RelationshipType\"\xa6\x02\n\rSemanticModel\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x19\n\x0b\x64\x65scription\x18\x02 \x01(\tB\x04\x90\x82\x19\x01\x12\x33\n\x06tables\x18\x03 \x03(\x0b\x32#.com.snowflake.cortex.analyst.Table\x12G\n\rrelationships\x18\x05 \x03(\x0b\x32*.com.snowflake.cortex.analyst.RelationshipB\x04\x90\x82\x19\x01\x12K\n\x10verified_queries\x18\x06 \x03(\x0b\x32+.com.snowflake.cortex.analyst.VerifiedQueryB\x04\x90\x82\x19\x01\x12!\n\x13\x63ustom_instructions\x18\x07 \x01(\tB\x04\x90\x82\x19\x01\"\xc5\x01\n\rVerifiedQuery\x12\x0c\n\x04name\x18\x01 \x01(\t\x12!\n\x13semantic_model_name\x18\x02 \x01(\tB\x04\x90\x82\x19\x01\x12\x10\n\x08question\x18\x03 \x01(\t\x12\x11\n\x03sql\x18\x04 \x01(\tB\x04\x98\x82\x19\x01\x12\x19\n\x0bverified_at\x18\x05 \x01(\x03\x42\x04\x90\x82\x19\x01\x12\x19\n\x0bverified_by\x18\x06 \x01(\tB\x04\x90\x82\x19\x01\x12(\n\x1ause_as_onboarding_question\x18\x07 \x01(\x08\x42\x04\x90\x82\x19\x01\"`\n\x17VerifiedQueryRepository\x12\x45\n\x10verified_queries\x18\x01 \x03(\x0b\x32+.com.snowflake.cortex.analyst.VerifiedQuery*~\n\x0f\x41ggregationType\x12\x1c\n\x18\x61ggregation_type_unknown\x10\x00\x12\x07\n\x03sum\x10\x01\x12\x07\n\x03\x61vg\x10\x02\x12\n\n\x06median\x10\x07\x12\x07\n\x03min\x10\x03\x12\x07\n\x03max\x10\x04\x12\t\n\x05\x63ount\x10\x05\x12\x12\n\x0e\x63ount_distinct\x10\x06*a\n\nColumnKind\x12\x17\n\x13\x63olumn_kind_unknown\x10\x00\x12\r\n\tdimension\x10\x01\x12\x0b\n\x07measure\x10\x02\x12\x12\n\x0etime_dimension\x10\x03\x12\n\n\x06metric\x10\x04*t\n\x08JoinType\x12\x15\n\x11join_type_unknown\x10\x00\x12\t\n\x05inner\x10\x01\x12\x0e\n\nleft_outer\x10\x02\x12\x12\n\nfull_outer\x10\x03\x1a\x02\x08\x01\x12\r\n\x05\x63ross\x10\x04\x1a\x02\x08\x01\x12\x13\n\x0bright_outer\x10\x05\x1a\x02\x08\x01*}\n\x10RelationshipType\x12\x1d\n\x19relationship_type_unknown\x10\x00\x12\x0e\n\none_to_one\x10\x01\x12\x0f\n\x0bmany_to_one\x10\x02\x12\x13\n\x0bone_to_many\x10\x03\x1a\x02\x08\x01\x12\x14\n\x0cmany_to_many\x10\x04\x1a\x02\x08\x01:4\n\x08optional\x12\x1d.google.protobuf.FieldOptions\x18\xa2\x90\x03 \x01(\x08\x88\x01\x01::\n\x0esql_expression\x12\x1d.google.protobuf.FieldOptions\x18\xa3\x90\x03 \x01(\x08\x88\x01\x01:4\n\x08id_field\x12\x1d.google.protobuf.FieldOptions\x18\xa4\x90\x03 \x01(\x08\x88\x01\x01\x42\x38\x42\x12SemanticModelProtoZ\"neeva.co/cortexsearch/chat/analystb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'semantic_model_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  _globals['DESCRIPTOR']._loaded_options = None
  _globals['DESCRIPTOR']._serialized_options = b'B\022SemanticModelProtoZ\"neeva.co/cortexsearch/chat/analyst'
  _globals['_JOINTYPE'].values_by_name["full_outer"]._loaded_options = None
  _globals['_JOINTYPE'].values_by_name["full_outer"]._serialized_options = b'\010\001'
  _globals['_JOINTYPE'].values_by_name["cross"]._loaded_options = None
  _globals['_JOINTYPE'].values_by_name["cross"]._serialized_options = b'\010\001'
  _globals['_JOINTYPE'].values_by_name["right_outer"]._loaded_options = None
  _globals['_JOINTYPE'].values_by_name["right_outer"]._serialized_options = b'\010\001'
  _globals['_RELATIONSHIPTYPE'].values_by_name["one_to_many"]._loaded_options = None
  _globals['_RELATIONSHIPTYPE'].values_by_name["one_to_many"]._serialized_options = b'\010\001'
  _globals['_RELATIONSHIPTYPE'].values_by_name["many_to_many"]._loaded_options = None
  _globals['_RELATIONSHIPTYPE'].values_by_name["many_to_many"]._serialized_options = b'\010\001'
  _globals['_COLUMN'].fields_by_name['name']._loaded_options = None
  _globals['_COLUMN'].fields_by_name['name']._serialized_options = b'\240\202\031\001'
  _globals['_COLUMN'].fields_by_name['synonyms']._loaded_options = None
  _globals['_COLUMN'].fields_by_name['synonyms']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['description']._loaded_options = None
  _globals['_COLUMN'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['expr']._loaded_options = None
  _globals['_COLUMN'].fields_by_name['expr']._serialized_options = b'\230\202\031\001'
  _globals['_COLUMN'].fields_by_name['unique']._loaded_options = None
  _globals['_COLUMN'].fields_by_name['unique']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['default_aggregation']._loaded_options = None
  _globals['_COLUMN'].fields_by_name['default_aggregation']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['sample_values']._loaded_options = None
  _globals['_COLUMN'].fields_by_name['sample_values']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['index_and_retrieve_values']._loaded_options = None
  _globals['_COLUMN'].fields_by_name['index_and_retrieve_values']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['retrieved_literals']._loaded_options = None
  _globals['_COLUMN'].fields_by_name['retrieved_literals']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['cortex_search_service_name']._loaded_options = None
  _globals['_COLUMN'].fields_by_name['cortex_search_service_name']._serialized_options = b'\030\001\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['cortex_search_service']._loaded_options = None
  _globals['_COLUMN'].fields_by_name['cortex_search_service']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['is_enum']._loaded_options = None
  _globals['_COLUMN'].fields_by_name['is_enum']._serialized_options = b'\220\202\031\001'
  _globals['_DIMENSION'].fields_by_name['name']._loaded_options = None
  _globals['_DIMENSION'].fields_by_name['name']._serialized_options = b'\240\202\031\001'
  _globals['_DIMENSION'].fields_by_name['synonyms']._loaded_options = None
  _globals['_DIMENSION'].fields_by_name['synonyms']._serialized_options = b'\220\202\031\001'
  _globals['_DIMENSION'].fields_by_name['description']._loaded_options = None
  _globals['_DIMENSION'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_DIMENSION'].fields_by_name['expr']._loaded_options = None
  _globals['_DIMENSION'].fields_by_name['expr']._serialized_options = b'\230\202\031\001'
  _globals['_DIMENSION'].fields_by_name['unique']._loaded_options = None
  _globals['_DIMENSION'].fields_by_name['unique']._serialized_options = b'\220\202\031\001'
  _globals['_DIMENSION'].fields_by_name['sample_values']._loaded_options = None
  _globals['_DIMENSION'].fields_by_name['sample_values']._serialized_options = b'\220\202\031\001'
  _globals['_DIMENSION'].fields_by_name['cortex_search_service']._loaded_options = None
  _globals['_DIMENSION'].fields_by_name['cortex_search_service']._serialized_options = b'\220\202\031\001'
  _globals['_DIMENSION'].fields_by_name['cortex_search_service_name']._loaded_options = None
  _globals['_DIMENSION'].fields_by_name['cortex_search_service_name']._serialized_options = b'\030\001\220\202\031\001'
  _globals['_DIMENSION'].fields_by_name['is_enum']._loaded_options = None
  _globals['_DIMENSION'].fields_by_name['is_enum']._serialized_options = b'\220\202\031\001'
  _globals['_CORTEXSEARCHSERVICE'].fields_by_name['database']._loaded_options = None
  _globals['_CORTEXSEARCHSERVICE'].fields_by_name['database']._serialized_options = b'\220\202\031\001'
  _globals['_CORTEXSEARCHSERVICE'].fields_by_name['schema']._loaded_options = None
  _globals['_CORTEXSEARCHSERVICE'].fields_by_name['schema']._serialized_options = b'\220\202\031\001'
  _globals['_CORTEXSEARCHSERVICE'].fields_by_name['literal_column']._loaded_options = None
  _globals['_CORTEXSEARCHSERVICE'].fields_by_name['literal_column']._serialized_options = b'\220\202\031\001'
  _globals['_TIMEDIMENSION'].fields_by_name['name']._loaded_options = None
  _globals['_TIMEDIMENSION'].fields_by_name['name']._serialized_options = b'\240\202\031\001'
  _globals['_TIMEDIMENSION'].fields_by_name['synonyms']._loaded_options = None
  _globals['_TIMEDIMENSION'].fields_by_name['synonyms']._serialized_options = b'\220\202\031\001'
  _globals['_TIMEDIMENSION'].fields_by_name['description']._loaded_options = None
  _globals['_TIMEDIMENSION'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_TIMEDIMENSION'].fields_by_name['expr']._loaded_options = None
  _globals['_TIMEDIMENSION'].fields_by_name['expr']._serialized_options = b'\230\202\031\001'
  _globals['_TIMEDIMENSION'].fields_by_name['unique']._loaded_options = None
  _globals['_TIMEDIMENSION'].fields_by_name['unique']._serialized_options = b'\220\202\031\001'
  _globals['_TIMEDIMENSION'].fields_by_name['sample_values']._loaded_options = None
  _globals['_TIMEDIMENSION'].fields_by_name['sample_values']._serialized_options = b'\220\202\031\001'
  _globals['_FACT'].fields_by_name['name']._loaded_options = None
  _globals['_FACT'].fields_by_name['name']._serialized_options = b'\240\202\031\001'
  _globals['_FACT'].fields_by_name['synonyms']._loaded_options = None
  _globals['_FACT'].fields_by_name['synonyms']._serialized_options = b'\220\202\031\001'
  _globals['_FACT'].fields_by_name['description']._loaded_options = None
  _globals['_FACT'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_FACT'].fields_by_name['expr']._loaded_options = None
  _globals['_FACT'].fields_by_name['expr']._serialized_options = b'\230\202\031\001'
  _globals['_FACT'].fields_by_name['default_aggregation']._loaded_options = None
  _globals['_FACT'].fields_by_name['default_aggregation']._serialized_options = b'\220\202\031\001'
  _globals['_FACT'].fields_by_name['sample_values']._loaded_options = None
  _globals['_FACT'].fields_by_name['sample_values']._serialized_options = b'\220\202\031\001'
  _globals['_NAMEDFILTER'].fields_by_name['synonyms']._loaded_options = None
  _globals['_NAMEDFILTER'].fields_by_name['synonyms']._serialized_options = b'\220\202\031\001'
  _globals['_NAMEDFILTER'].fields_by_name['description']._loaded_options = None
  _globals['_NAMEDFILTER'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_NAMEDFILTER'].fields_by_name['expr']._loaded_options = None
  _globals['_NAMEDFILTER'].fields_by_name['expr']._serialized_options = b'\230\202\031\001'
  _globals['_TABLE'].fields_by_name['name']._loaded_options = None
  _globals['_TABLE'].fields_by_name['name']._serialized_options = b'\240\202\031\001'
  _globals['_TABLE'].fields_by_name['synonyms']._loaded_options = None
  _globals['_TABLE'].fields_by_name['synonyms']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['description']._loaded_options = None
  _globals['_TABLE'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['columns']._loaded_options = None
  _globals['_TABLE'].fields_by_name['columns']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['dimensions']._loaded_options = None
  _globals['_TABLE'].fields_by_name['dimensions']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['time_dimensions']._loaded_options = None
  _globals['_TABLE'].fields_by_name['time_dimensions']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['measures']._loaded_options = None
  _globals['_TABLE'].fields_by_name['measures']._serialized_options = b'\030\001\220\202\031\001'
  _globals['_TABLE'].fields_by_name['facts']._loaded_options = None
  _globals['_TABLE'].fields_by_name['facts']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['metrics']._loaded_options = None
  _globals['_TABLE'].fields_by_name['metrics']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['primary_key']._loaded_options = None
  _globals['_TABLE'].fields_by_name['primary_key']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['foreign_keys']._loaded_options = None
  _globals['_TABLE'].fields_by_name['foreign_keys']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['filters']._loaded_options = None
  _globals['_TABLE'].fields_by_name['filters']._serialized_options = b'\220\202\031\001'
  _globals['_METRIC'].fields_by_name['name']._loaded_options = None
  _globals['_METRIC'].fields_by_name['name']._serialized_options = b'\240\202\031\001'
  _globals['_METRIC'].fields_by_name['synonyms']._loaded_options = None
  _globals['_METRIC'].fields_by_name['synonyms']._serialized_options = b'\220\202\031\001'
  _globals['_METRIC'].fields_by_name['description']._loaded_options = None
  _globals['_METRIC'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_METRIC'].fields_by_name['expr']._loaded_options = None
  _globals['_METRIC'].fields_by_name['expr']._serialized_options = b'\230\202\031\001'
  _globals['_METRIC'].fields_by_name['filter']._loaded_options = None
  _globals['_METRIC'].fields_by_name['filter']._serialized_options = b'\220\202\031\001'
  _globals['_METRICSFILTER'].fields_by_name['expr']._loaded_options = None
  _globals['_METRICSFILTER'].fields_by_name['expr']._serialized_options = b'\230\202\031\001'
  _globals['_RELATIONSHIP'].fields_by_name['expr']._loaded_options = None
  _globals['_RELATIONSHIP'].fields_by_name['expr']._serialized_options = b'\220\202\031\001\230\202\031\001'
  _globals['_RELATIONSHIP'].fields_by_name['relationship_columns']._loaded_options = None
  _globals['_RELATIONSHIP'].fields_by_name['relationship_columns']._serialized_options = b'\220\202\031\001'
  _globals['_SEMANTICMODEL'].fields_by_name['description']._loaded_options = None
  _globals['_SEMANTICMODEL'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_SEMANTICMODEL'].fields_by_name['relationships']._loaded_options = None
  _globals['_SEMANTICMODEL'].fields_by_name['relationships']._serialized_options = b'\220\202\031\001'
  _globals['_SEMANTICMODEL'].fields_by_name['verified_queries']._loaded_options = None
  _globals['_SEMANTICMODEL'].fields_by_name['verified_queries']._serialized_options = b'\220\202\031\001'
  _globals['_SEMANTICMODEL'].fields_by_name['custom_instructions']._loaded_options = None
  _globals['_SEMANTICMODEL'].fields_by_name['custom_instructions']._serialized_options = b'\220\202\031\001'
  _globals['_VERIFIEDQUERY'].fields_by_name['semantic_model_name']._loaded_options = None
  _globals['_VERIFIEDQUERY'].fields_by_name['semantic_model_name']._serialized_options = b'\220\202\031\001'
  _globals['_VERIFIEDQUERY'].fields_by_name['sql']._loaded_options = None
  _globals['_VERIFIEDQUERY'].fields_by_name['sql']._serialized_options = b'\230\202\031\001'
  _globals['_VERIFIEDQUERY'].fields_by_name['verified_at']._loaded_options = None
  _globals['_VERIFIEDQUERY'].fields_by_name['verified_at']._serialized_options = b'\220\202\031\001'
  _globals['_VERIFIEDQUERY'].fields_by_name['verified_by']._loaded_options = None
  _globals['_VERIFIEDQUERY'].fields_by_name['verified_by']._serialized_options = b'\220\202\031\001'
  _globals['_VERIFIEDQUERY'].fields_by_name['use_as_onboarding_question']._loaded_options = None
  _globals['_VERIFIEDQUERY'].fields_by_name['use_as_onboarding_question']._serialized_options = b'\220\202\031\001'
  _globals['_AGGREGATIONTYPE']._serialized_start=3820
  _globals['_AGGREGATIONTYPE']._serialized_end=3946
  _globals['_COLUMNKIND']._serialized_start=3948
  _globals['_COLUMNKIND']._serialized_end=4045
  _globals['_JOINTYPE']._serialized_start=4047
  _globals['_JOINTYPE']._serialized_end=4163
  _globals['_RELATIONSHIPTYPE']._serialized_start=4165
  _globals['_RELATIONSHIPTYPE']._serialized_end=4290
  _globals['_RETRIEVALRESULT']._serialized_start=88
  _globals['_RETRIEVALRESULT']._serialized_end=135
  _globals['_COLUMN']._serialized_start=138
  _globals['_COLUMN']._serialized_end=722
  _globals['_DIMENSION']._serialized_start=725
  _globals['_DIMENSION']._serialized_end=1052
  _globals['_CORTEXSEARCHSERVICE']._serialized_start=1054
  _globals['_CORTEXSEARCHSERVICE']._serialized_end=1168
  _globals['_TIMEDIMENSION']._serialized_start=1171
  _globals['_TIMEDIMENSION']._serialized_end=1347
  _globals['_FACT']._serialized_start=1350
  _globals['_FACT']._serialized_end=1577
  _globals['_NAMEDFILTER']._serialized_start=1579
  _globals['_NAMEDFILTER']._serialized_end=1677
  _globals['_FULLYQUALIFIEDTABLE']._serialized_start=1679
  _globals['_FULLYQUALIFIEDTABLE']._serialized_end=1749
  _globals['_PRIMARYKEY']._serialized_start=1751
  _globals['_PRIMARYKEY']._serialized_end=1780
  _globals['_FOREIGNKEY']._serialized_start=1782
  _globals['_FOREIGNKEY']._serialized_end=1909
  _globals['_TABLE']._serialized_start=1912
  _globals['_TABLE']._serialized_end=2650
  _globals['_METRIC']._serialized_start=2653
  _globals['_METRIC']._serialized_end=2819
  _globals['_METRICSFILTER']._serialized_start=2821
  _globals['_METRICSFILTER']._serialized_end=2856
  _globals['_RELATIONKEY']._serialized_start=2858
  _globals['_RELATIONKEY']._serialized_end=2914
  _globals['_RELATIONSHIP']._serialized_start=2917
  _globals['_RELATIONSHIP']._serialized_end=3223
  _globals['_SEMANTICMODEL']._serialized_start=3226
  _globals['_SEMANTICMODEL']._serialized_end=3520
  _globals['_VERIFIEDQUERY']._serialized_start=3523
  _globals['_VERIFIEDQUERY']._serialized_end=3720
  _globals['_VERIFIEDQUERYREPOSITORY']._serialized_start=3722
  _globals['_VERIFIEDQUERYREPOSITORY']._serialized_end=3818
# @@protoc_insertion_point(module_scope)
