# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: semantic_model.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import descriptor_pb2 as google_dot_protobuf_dot_descriptor__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x14semantic_model.proto\x12\x18semantic_model_generator\x1a google/protobuf/descriptor.proto\"/\n\x0fRetrievalResult\x12\r\n\x05value\x18\x01 \x01(\t\x12\r\n\x05score\x18\x02 \x01(\x02\"\xa1\x04\n\x06\x43olumn\x12\x12\n\x04name\x18\x01 \x01(\tB\x04\xa0\x82\x19\x01\x12\x16\n\x08synonyms\x18\x02 \x03(\tB\x04\x90\x82\x19\x01\x12\x19\n\x0b\x64\x65scription\x18\x03 \x01(\tB\x04\x90\x82\x19\x01\x12\x12\n\x04\x65xpr\x18\x04 \x01(\tB\x04\x98\x82\x19\x01\x12\x11\n\tdata_type\x18\x05 \x01(\t\x12\x32\n\x04kind\x18\x06 \x01(\x0e\x32$.semantic_model_generator.ColumnKind\x12\x14\n\x06unique\x18\x07 \x01(\x08\x42\x04\x90\x82\x19\x01\x12L\n\x13\x64\x65\x66\x61ult_aggregation\x18\x08 \x01(\x0e\x32).semantic_model_generator.AggregationTypeB\x04\x90\x82\x19\x01\x12\x1b\n\rsample_values\x18\t \x03(\tB\x04\x90\x82\x19\x01\x12\'\n\x19index_and_retrieve_values\x18\n \x01(\x08\x42\x04\x90\x82\x19\x01\x12K\n\x12retrieved_literals\x18\x0b \x03(\x0b\x32).semantic_model_generator.RetrievalResultB\x04\x90\x82\x19\x01\x12*\n\x1a\x63ortex_search_service_name\x18\x0c \x01(\tB\x06\x18\x01\x90\x82\x19\x01\x12R\n\x15\x63ortex_search_service\x18\r \x01(\x0b\x32-.semantic_model_generator.CortexSearchServiceB\x04\x90\x82\x19\x01\"\xac\x02\n\tDimension\x12\x12\n\x04name\x18\x01 \x01(\tB\x04\xa0\x82\x19\x01\x12\x16\n\x08synonyms\x18\x02 \x03(\tB\x04\x90\x82\x19\x01\x12\x19\n\x0b\x64\x65scription\x18\x03 \x01(\tB\x04\x90\x82\x19\x01\x12\x12\n\x04\x65xpr\x18\x04 \x01(\tB\x04\x98\x82\x19\x01\x12\x11\n\tdata_type\x18\x05 \x01(\t\x12\x14\n\x06unique\x18\x06 \x01(\x08\x42\x04\x90\x82\x19\x01\x12\x1b\n\rsample_values\x18\x07 \x03(\tB\x04\x90\x82\x19\x01\x12R\n\x15\x63ortex_search_service\x18\x08 \x01(\x0b\x32-.semantic_model_generator.CortexSearchServiceB\x04\x90\x82\x19\x01\x12*\n\x1a\x63ortex_search_service_name\x18\t \x01(\tB\x06\x18\x01\x90\x82\x19\x01\"r\n\x13\x43ortexSearchService\x12\x16\n\x08\x64\x61tabase\x18\x01 \x01(\tB\x04\x90\x82\x19\x01\x12\x14\n\x06schema\x18\x02 \x01(\tB\x04\x90\x82\x19\x01\x12\x0f\n\x07service\x18\x03 \x01(\t\x12\x1c\n\x0eliteral_column\x18\x04 \x01(\tB\x04\x90\x82\x19\x01\"\xb0\x01\n\rTimeDimension\x12\x12\n\x04name\x18\x01 \x01(\tB\x04\xa0\x82\x19\x01\x12\x16\n\x08synonyms\x18\x02 \x03(\tB\x04\x90\x82\x19\x01\x12\x19\n\x0b\x64\x65scription\x18\x03 \x01(\tB\x04\x90\x82\x19\x01\x12\x12\n\x04\x65xpr\x18\x04 \x01(\tB\x04\x98\x82\x19\x01\x12\x11\n\tdata_type\x18\x05 \x01(\t\x12\x14\n\x06unique\x18\x06 \x01(\x08\x42\x04\x90\x82\x19\x01\x12\x1b\n\rsample_values\x18\x07 \x03(\tB\x04\x90\x82\x19\x01\"\xe2\x01\n\x07Measure\x12\x12\n\x04name\x18\x01 \x01(\tB\x04\xa0\x82\x19\x01\x12\x16\n\x08synonyms\x18\x02 \x03(\tB\x04\x90\x82\x19\x01\x12\x19\n\x0b\x64\x65scription\x18\x03 \x01(\tB\x04\x90\x82\x19\x01\x12\x12\n\x04\x65xpr\x18\x04 \x01(\tB\x04\x98\x82\x19\x01\x12\x11\n\tdata_type\x18\x05 \x01(\t\x12L\n\x13\x64\x65\x66\x61ult_aggregation\x18\x06 \x01(\x0e\x32).semantic_model_generator.AggregationTypeB\x04\x90\x82\x19\x01\x12\x1b\n\rsample_values\x18\x07 \x03(\tB\x04\x90\x82\x19\x01\"b\n\x0bNamedFilter\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x16\n\x08synonyms\x18\x02 \x03(\tB\x04\x90\x82\x19\x01\x12\x19\n\x0b\x64\x65scription\x18\x03 \x01(\tB\x04\x90\x82\x19\x01\x12\x12\n\x04\x65xpr\x18\x04 \x01(\tB\x04\x98\x82\x19\x01\"F\n\x13\x46ullyQualifiedTable\x12\x10\n\x08\x64\x61tabase\x18\x01 \x01(\t\x12\x0e\n\x06schema\x18\x02 \x01(\t\x12\r\n\x05table\x18\x03 \x01(\t\"\x1d\n\nPrimaryKey\x12\x0f\n\x07\x63olumns\x18\x01 \x03(\t\"\x8b\x04\n\x05Table\x12\x12\n\x04name\x18\x01 \x01(\tB\x04\xa0\x82\x19\x01\x12\x16\n\x08synonyms\x18\x02 \x03(\tB\x04\x90\x82\x19\x01\x12\x19\n\x0b\x64\x65scription\x18\x03 \x01(\tB\x04\x90\x82\x19\x01\x12\x41\n\nbase_table\x18\x04 \x01(\x0b\x32-.semantic_model_generator.FullyQualifiedTable\x12\x37\n\x07\x63olumns\x18\x05 \x03(\x0b\x32 .semantic_model_generator.ColumnB\x04\x90\x82\x19\x01\x12=\n\ndimensions\x18\t \x03(\x0b\x32#.semantic_model_generator.DimensionB\x04\x90\x82\x19\x01\x12\x46\n\x0ftime_dimensions\x18\n \x03(\x0b\x32\'.semantic_model_generator.TimeDimensionB\x04\x90\x82\x19\x01\x12\x39\n\x08measures\x18\x0b \x03(\x0b\x32!.semantic_model_generator.MeasureB\x04\x90\x82\x19\x01\x12?\n\x0bprimary_key\x18\x06 \x01(\x0b\x32$.semantic_model_generator.PrimaryKeyB\x04\x90\x82\x19\x01\x12<\n\x07\x66ilters\x18\x08 \x03(\x0b\x32%.semantic_model_generator.NamedFilterB\x04\x90\x82\x19\x01\"\xa2\x01\n\x06Metric\x12\x12\n\x04name\x18\x01 \x01(\tB\x04\xa0\x82\x19\x01\x12\x16\n\x08synonyms\x18\x02 \x03(\tB\x04\x90\x82\x19\x01\x12\x19\n\x0b\x64\x65scription\x18\x03 \x01(\tB\x04\x90\x82\x19\x01\x12\x12\n\x04\x65xpr\x18\x04 \x01(\tB\x04\x98\x82\x19\x01\x12=\n\x06\x66ilter\x18\x05 \x01(\x0b\x32\'.semantic_model_generator.MetricsFilterB\x04\x90\x82\x19\x01\"#\n\rMetricsFilter\x12\x12\n\x04\x65xpr\x18\x01 \x01(\tB\x04\x98\x82\x19\x01\"8\n\x0bRelationKey\x12\x13\n\x0bleft_column\x18\x01 \x01(\t\x12\x14\n\x0cright_column\x18\x02 \x01(\t\"\x88\x02\n\x0cRelationship\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x12\n\nleft_table\x18\x02 \x01(\t\x12\x13\n\x0bright_table\x18\x03 \x01(\t\x12\x43\n\x14relationship_columns\x18\x07 \x03(\x0b\x32%.semantic_model_generator.RelationKey\x12\x35\n\tjoin_type\x18\x05 \x01(\x0e\x32\".semantic_model_generator.JoinType\x12\x45\n\x11relationship_type\x18\x06 \x01(\x0e\x32*.semantic_model_generator.RelationshipType\"\xd3\x02\n\rSemanticModel\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x19\n\x0b\x64\x65scription\x18\x02 \x01(\tB\x04\x90\x82\x19\x01\x12/\n\x06tables\x18\x03 \x03(\x0b\x32\x1f.semantic_model_generator.Table\x12\x37\n\x07metrics\x18\x04 \x03(\x0b\x32 .semantic_model_generator.MetricB\x04\x90\x82\x19\x01\x12\x43\n\rrelationships\x18\x05 \x03(\x0b\x32&.semantic_model_generator.RelationshipB\x04\x90\x82\x19\x01\x12G\n\x10verified_queries\x18\x06 \x03(\x0b\x32\'.semantic_model_generator.VerifiedQueryB\x04\x90\x82\x19\x01\x12!\n\x13\x63ustom_instructions\x18\x07 \x01(\tB\x04\x90\x82\x19\x01\"\xc5\x01\n\rVerifiedQuery\x12\x0c\n\x04name\x18\x01 \x01(\t\x12!\n\x13semantic_model_name\x18\x02 \x01(\tB\x04\x90\x82\x19\x01\x12\x10\n\x08question\x18\x03 \x01(\t\x12\x11\n\x03sql\x18\x04 \x01(\tB\x04\x98\x82\x19\x01\x12\x19\n\x0bverified_at\x18\x05 \x01(\x03\x42\x04\x90\x82\x19\x01\x12\x19\n\x0bverified_by\x18\x06 \x01(\tB\x04\x90\x82\x19\x01\x12(\n\x1ause_as_onboarding_question\x18\x07 \x01(\x08\x42\x04\x90\x82\x19\x01\"\\\n\x17VerifiedQueryRepository\x12\x41\n\x10verified_queries\x18\x01 \x03(\x0b\x32\'.semantic_model_generator.VerifiedQuery*~\n\x0f\x41ggregationType\x12\x1c\n\x18\x61ggregation_type_unknown\x10\x00\x12\x07\n\x03sum\x10\x01\x12\x07\n\x03\x61vg\x10\x02\x12\n\n\x06median\x10\x07\x12\x07\n\x03min\x10\x03\x12\x07\n\x03max\x10\x04\x12\t\n\x05\x63ount\x10\x05\x12\x12\n\x0e\x63ount_distinct\x10\x06*U\n\nColumnKind\x12\x17\n\x13\x63olumn_kind_unknown\x10\x00\x12\r\n\tdimension\x10\x01\x12\x0b\n\x07measure\x10\x02\x12\x12\n\x0etime_dimension\x10\x03*<\n\x08JoinType\x12\x15\n\x11join_type_unknown\x10\x00\x12\t\n\x05inner\x10\x01\x12\x0e\n\nleft_outer\x10\x02*R\n\x10RelationshipType\x12\x1d\n\x19relationship_type_unknown\x10\x00\x12\x0e\n\none_to_one\x10\x01\x12\x0f\n\x0bmany_to_one\x10\x02:4\n\x08optional\x12\x1d.google.protobuf.FieldOptions\x18\xa2\x90\x03 \x01(\x08\x88\x01\x01::\n\x0esql_expression\x12\x1d.google.protobuf.FieldOptions\x18\xa3\x90\x03 \x01(\x08\x88\x01\x01:4\n\x08id_field\x12\x1d.google.protobuf.FieldOptions\x18\xa4\x90\x03 \x01(\x08\x88\x01\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'semantic_model_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_COLUMN'].fields_by_name['name']._options = None
  _globals['_COLUMN'].fields_by_name['name']._serialized_options = b'\240\202\031\001'
  _globals['_COLUMN'].fields_by_name['synonyms']._options = None
  _globals['_COLUMN'].fields_by_name['synonyms']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['description']._options = None
  _globals['_COLUMN'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['expr']._options = None
  _globals['_COLUMN'].fields_by_name['expr']._serialized_options = b'\230\202\031\001'
  _globals['_COLUMN'].fields_by_name['unique']._options = None
  _globals['_COLUMN'].fields_by_name['unique']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['default_aggregation']._options = None
  _globals['_COLUMN'].fields_by_name['default_aggregation']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['sample_values']._options = None
  _globals['_COLUMN'].fields_by_name['sample_values']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['index_and_retrieve_values']._options = None
  _globals['_COLUMN'].fields_by_name['index_and_retrieve_values']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['retrieved_literals']._options = None
  _globals['_COLUMN'].fields_by_name['retrieved_literals']._serialized_options = b'\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['cortex_search_service_name']._options = None
  _globals['_COLUMN'].fields_by_name['cortex_search_service_name']._serialized_options = b'\030\001\220\202\031\001'
  _globals['_COLUMN'].fields_by_name['cortex_search_service']._options = None
  _globals['_COLUMN'].fields_by_name['cortex_search_service']._serialized_options = b'\220\202\031\001'
  _globals['_DIMENSION'].fields_by_name['name']._options = None
  _globals['_DIMENSION'].fields_by_name['name']._serialized_options = b'\240\202\031\001'
  _globals['_DIMENSION'].fields_by_name['synonyms']._options = None
  _globals['_DIMENSION'].fields_by_name['synonyms']._serialized_options = b'\220\202\031\001'
  _globals['_DIMENSION'].fields_by_name['description']._options = None
  _globals['_DIMENSION'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_DIMENSION'].fields_by_name['expr']._options = None
  _globals['_DIMENSION'].fields_by_name['expr']._serialized_options = b'\230\202\031\001'
  _globals['_DIMENSION'].fields_by_name['unique']._options = None
  _globals['_DIMENSION'].fields_by_name['unique']._serialized_options = b'\220\202\031\001'
  _globals['_DIMENSION'].fields_by_name['sample_values']._options = None
  _globals['_DIMENSION'].fields_by_name['sample_values']._serialized_options = b'\220\202\031\001'
  _globals['_DIMENSION'].fields_by_name['cortex_search_service']._options = None
  _globals['_DIMENSION'].fields_by_name['cortex_search_service']._serialized_options = b'\220\202\031\001'
  _globals['_DIMENSION'].fields_by_name['cortex_search_service_name']._options = None
  _globals['_DIMENSION'].fields_by_name['cortex_search_service_name']._serialized_options = b'\030\001\220\202\031\001'
  _globals['_CORTEXSEARCHSERVICE'].fields_by_name['database']._options = None
  _globals['_CORTEXSEARCHSERVICE'].fields_by_name['database']._serialized_options = b'\220\202\031\001'
  _globals['_CORTEXSEARCHSERVICE'].fields_by_name['schema']._options = None
  _globals['_CORTEXSEARCHSERVICE'].fields_by_name['schema']._serialized_options = b'\220\202\031\001'
  _globals['_CORTEXSEARCHSERVICE'].fields_by_name['literal_column']._options = None
  _globals['_CORTEXSEARCHSERVICE'].fields_by_name['literal_column']._serialized_options = b'\220\202\031\001'
  _globals['_TIMEDIMENSION'].fields_by_name['name']._options = None
  _globals['_TIMEDIMENSION'].fields_by_name['name']._serialized_options = b'\240\202\031\001'
  _globals['_TIMEDIMENSION'].fields_by_name['synonyms']._options = None
  _globals['_TIMEDIMENSION'].fields_by_name['synonyms']._serialized_options = b'\220\202\031\001'
  _globals['_TIMEDIMENSION'].fields_by_name['description']._options = None
  _globals['_TIMEDIMENSION'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_TIMEDIMENSION'].fields_by_name['expr']._options = None
  _globals['_TIMEDIMENSION'].fields_by_name['expr']._serialized_options = b'\230\202\031\001'
  _globals['_TIMEDIMENSION'].fields_by_name['unique']._options = None
  _globals['_TIMEDIMENSION'].fields_by_name['unique']._serialized_options = b'\220\202\031\001'
  _globals['_TIMEDIMENSION'].fields_by_name['sample_values']._options = None
  _globals['_TIMEDIMENSION'].fields_by_name['sample_values']._serialized_options = b'\220\202\031\001'
  _globals['_MEASURE'].fields_by_name['name']._options = None
  _globals['_MEASURE'].fields_by_name['name']._serialized_options = b'\240\202\031\001'
  _globals['_MEASURE'].fields_by_name['synonyms']._options = None
  _globals['_MEASURE'].fields_by_name['synonyms']._serialized_options = b'\220\202\031\001'
  _globals['_MEASURE'].fields_by_name['description']._options = None
  _globals['_MEASURE'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_MEASURE'].fields_by_name['expr']._options = None
  _globals['_MEASURE'].fields_by_name['expr']._serialized_options = b'\230\202\031\001'
  _globals['_MEASURE'].fields_by_name['default_aggregation']._options = None
  _globals['_MEASURE'].fields_by_name['default_aggregation']._serialized_options = b'\220\202\031\001'
  _globals['_MEASURE'].fields_by_name['sample_values']._options = None
  _globals['_MEASURE'].fields_by_name['sample_values']._serialized_options = b'\220\202\031\001'
  _globals['_NAMEDFILTER'].fields_by_name['synonyms']._options = None
  _globals['_NAMEDFILTER'].fields_by_name['synonyms']._serialized_options = b'\220\202\031\001'
  _globals['_NAMEDFILTER'].fields_by_name['description']._options = None
  _globals['_NAMEDFILTER'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_NAMEDFILTER'].fields_by_name['expr']._options = None
  _globals['_NAMEDFILTER'].fields_by_name['expr']._serialized_options = b'\230\202\031\001'
  _globals['_TABLE'].fields_by_name['name']._options = None
  _globals['_TABLE'].fields_by_name['name']._serialized_options = b'\240\202\031\001'
  _globals['_TABLE'].fields_by_name['synonyms']._options = None
  _globals['_TABLE'].fields_by_name['synonyms']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['description']._options = None
  _globals['_TABLE'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['columns']._options = None
  _globals['_TABLE'].fields_by_name['columns']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['dimensions']._options = None
  _globals['_TABLE'].fields_by_name['dimensions']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['time_dimensions']._options = None
  _globals['_TABLE'].fields_by_name['time_dimensions']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['measures']._options = None
  _globals['_TABLE'].fields_by_name['measures']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['primary_key']._options = None
  _globals['_TABLE'].fields_by_name['primary_key']._serialized_options = b'\220\202\031\001'
  _globals['_TABLE'].fields_by_name['filters']._options = None
  _globals['_TABLE'].fields_by_name['filters']._serialized_options = b'\220\202\031\001'
  _globals['_METRIC'].fields_by_name['name']._options = None
  _globals['_METRIC'].fields_by_name['name']._serialized_options = b'\240\202\031\001'
  _globals['_METRIC'].fields_by_name['synonyms']._options = None
  _globals['_METRIC'].fields_by_name['synonyms']._serialized_options = b'\220\202\031\001'
  _globals['_METRIC'].fields_by_name['description']._options = None
  _globals['_METRIC'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_METRIC'].fields_by_name['expr']._options = None
  _globals['_METRIC'].fields_by_name['expr']._serialized_options = b'\230\202\031\001'
  _globals['_METRIC'].fields_by_name['filter']._options = None
  _globals['_METRIC'].fields_by_name['filter']._serialized_options = b'\220\202\031\001'
  _globals['_METRICSFILTER'].fields_by_name['expr']._options = None
  _globals['_METRICSFILTER'].fields_by_name['expr']._serialized_options = b'\230\202\031\001'
  _globals['_SEMANTICMODEL'].fields_by_name['description']._options = None
  _globals['_SEMANTICMODEL'].fields_by_name['description']._serialized_options = b'\220\202\031\001'
  _globals['_SEMANTICMODEL'].fields_by_name['metrics']._options = None
  _globals['_SEMANTICMODEL'].fields_by_name['metrics']._serialized_options = b'\220\202\031\001'
  _globals['_SEMANTICMODEL'].fields_by_name['relationships']._options = None
  _globals['_SEMANTICMODEL'].fields_by_name['relationships']._serialized_options = b'\220\202\031\001'
  _globals['_SEMANTICMODEL'].fields_by_name['verified_queries']._options = None
  _globals['_SEMANTICMODEL'].fields_by_name['verified_queries']._serialized_options = b'\220\202\031\001'
  _globals['_SEMANTICMODEL'].fields_by_name['custom_instructions']._options = None
  _globals['_SEMANTICMODEL'].fields_by_name['custom_instructions']._serialized_options = b'\220\202\031\001'
  _globals['_VERIFIEDQUERY'].fields_by_name['semantic_model_name']._options = None
  _globals['_VERIFIEDQUERY'].fields_by_name['semantic_model_name']._serialized_options = b'\220\202\031\001'
  _globals['_VERIFIEDQUERY'].fields_by_name['sql']._options = None
  _globals['_VERIFIEDQUERY'].fields_by_name['sql']._serialized_options = b'\230\202\031\001'
  _globals['_VERIFIEDQUERY'].fields_by_name['verified_at']._options = None
  _globals['_VERIFIEDQUERY'].fields_by_name['verified_at']._serialized_options = b'\220\202\031\001'
  _globals['_VERIFIEDQUERY'].fields_by_name['verified_by']._options = None
  _globals['_VERIFIEDQUERY'].fields_by_name['verified_by']._serialized_options = b'\220\202\031\001'
  _globals['_VERIFIEDQUERY'].fields_by_name['use_as_onboarding_question']._options = None
  _globals['_VERIFIEDQUERY'].fields_by_name['use_as_onboarding_question']._serialized_options = b'\220\202\031\001'
  _globals['_AGGREGATIONTYPE']._serialized_start=3400
  _globals['_AGGREGATIONTYPE']._serialized_end=3526
  _globals['_COLUMNKIND']._serialized_start=3528
  _globals['_COLUMNKIND']._serialized_end=3613
  _globals['_JOINTYPE']._serialized_start=3615
  _globals['_JOINTYPE']._serialized_end=3675
  _globals['_RELATIONSHIPTYPE']._serialized_start=3677
  _globals['_RELATIONSHIPTYPE']._serialized_end=3759
  _globals['_RETRIEVALRESULT']._serialized_start=84
  _globals['_RETRIEVALRESULT']._serialized_end=131
  _globals['_COLUMN']._serialized_start=134
  _globals['_COLUMN']._serialized_end=679
  _globals['_DIMENSION']._serialized_start=682
  _globals['_DIMENSION']._serialized_end=982
  _globals['_CORTEXSEARCHSERVICE']._serialized_start=984
  _globals['_CORTEXSEARCHSERVICE']._serialized_end=1098
  _globals['_TIMEDIMENSION']._serialized_start=1101
  _globals['_TIMEDIMENSION']._serialized_end=1277
  _globals['_MEASURE']._serialized_start=1280
  _globals['_MEASURE']._serialized_end=1506
  _globals['_NAMEDFILTER']._serialized_start=1508
  _globals['_NAMEDFILTER']._serialized_end=1606
  _globals['_FULLYQUALIFIEDTABLE']._serialized_start=1608
  _globals['_FULLYQUALIFIEDTABLE']._serialized_end=1678
  _globals['_PRIMARYKEY']._serialized_start=1680
  _globals['_PRIMARYKEY']._serialized_end=1709
  _globals['_TABLE']._serialized_start=1712
  _globals['_TABLE']._serialized_end=2235
  _globals['_METRIC']._serialized_start=2238
  _globals['_METRIC']._serialized_end=2400
  _globals['_METRICSFILTER']._serialized_start=2402
  _globals['_METRICSFILTER']._serialized_end=2437
  _globals['_RELATIONKEY']._serialized_start=2439
  _globals['_RELATIONKEY']._serialized_end=2495
  _globals['_RELATIONSHIP']._serialized_start=2498
  _globals['_RELATIONSHIP']._serialized_end=2762
  _globals['_SEMANTICMODEL']._serialized_start=2765
  _globals['_SEMANTICMODEL']._serialized_end=3104
  _globals['_VERIFIEDQUERY']._serialized_start=3107
  _globals['_VERIFIEDQUERY']._serialized_end=3304
  _globals['_VERIFIEDQUERYREPOSITORY']._serialized_start=3306
  _globals['_VERIFIEDQUERYREPOSITORY']._serialized_end=3398
# @@protoc_insertion_point(module_scope)
