from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AggregationType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    aggregation_type_unknown: _ClassVar[AggregationType]
    sum: _ClassVar[AggregationType]
    avg: _ClassVar[AggregationType]
    median: _ClassVar[AggregationType]
    min: _ClassVar[AggregationType]
    max: _ClassVar[AggregationType]
    count: _ClassVar[AggregationType]
    count_distinct: _ClassVar[AggregationType]

class ColumnKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    column_kind_unknown: _ClassVar[ColumnKind]
    dimension: _ClassVar[ColumnKind]
    measure: _ClassVar[ColumnKind]
    time_dimension: _ClassVar[ColumnKind]
    metric: _ClassVar[ColumnKind]

class JoinType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    join_type_unknown: _ClassVar[JoinType]
    inner: _ClassVar[JoinType]
    left_outer: _ClassVar[JoinType]
    full_outer: _ClassVar[JoinType]
    cross: _ClassVar[JoinType]
    right_outer: _ClassVar[JoinType]

class RelationshipType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    relationship_type_unknown: _ClassVar[RelationshipType]
    one_to_one: _ClassVar[RelationshipType]
    many_to_one: _ClassVar[RelationshipType]
    one_to_many: _ClassVar[RelationshipType]
    many_to_many: _ClassVar[RelationshipType]
aggregation_type_unknown: AggregationType
sum: AggregationType
avg: AggregationType
median: AggregationType
min: AggregationType
max: AggregationType
count: AggregationType
count_distinct: AggregationType
column_kind_unknown: ColumnKind
dimension: ColumnKind
measure: ColumnKind
time_dimension: ColumnKind
metric: ColumnKind
join_type_unknown: JoinType
inner: JoinType
left_outer: JoinType
full_outer: JoinType
cross: JoinType
right_outer: JoinType
relationship_type_unknown: RelationshipType
one_to_one: RelationshipType
many_to_one: RelationshipType
one_to_many: RelationshipType
many_to_many: RelationshipType
OPTIONAL_FIELD_NUMBER: _ClassVar[int]
optional: _descriptor.FieldDescriptor
SQL_EXPRESSION_FIELD_NUMBER: _ClassVar[int]
sql_expression: _descriptor.FieldDescriptor
ID_FIELD_FIELD_NUMBER: _ClassVar[int]
id_field: _descriptor.FieldDescriptor

class RetrievalResult(_message.Message):
    __slots__ = ("value", "score")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    value: str
    score: float
    def __init__(self, value: _Optional[str] = ..., score: _Optional[float] = ...) -> None: ...

class Column(_message.Message):
    __slots__ = ("name", "synonyms", "description", "expr", "data_type", "kind", "unique", "default_aggregation", "sample_values", "index_and_retrieve_values", "retrieved_literals", "cortex_search_service_name", "cortex_search_service", "is_enum")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SYNONYMS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_AGGREGATION_FIELD_NUMBER: _ClassVar[int]
    SAMPLE_VALUES_FIELD_NUMBER: _ClassVar[int]
    INDEX_AND_RETRIEVE_VALUES_FIELD_NUMBER: _ClassVar[int]
    RETRIEVED_LITERALS_FIELD_NUMBER: _ClassVar[int]
    CORTEX_SEARCH_SERVICE_NAME_FIELD_NUMBER: _ClassVar[int]
    CORTEX_SEARCH_SERVICE_FIELD_NUMBER: _ClassVar[int]
    IS_ENUM_FIELD_NUMBER: _ClassVar[int]
    name: str
    synonyms: _containers.RepeatedScalarFieldContainer[str]
    description: str
    expr: str
    data_type: str
    kind: ColumnKind
    unique: bool
    default_aggregation: AggregationType
    sample_values: _containers.RepeatedScalarFieldContainer[str]
    index_and_retrieve_values: bool
    retrieved_literals: _containers.RepeatedCompositeFieldContainer[RetrievalResult]
    cortex_search_service_name: str
    cortex_search_service: CortexSearchService
    is_enum: bool
    def __init__(self, name: _Optional[str] = ..., synonyms: _Optional[_Iterable[str]] = ..., description: _Optional[str] = ..., expr: _Optional[str] = ..., data_type: _Optional[str] = ..., kind: _Optional[_Union[ColumnKind, str]] = ..., unique: bool = ..., default_aggregation: _Optional[_Union[AggregationType, str]] = ..., sample_values: _Optional[_Iterable[str]] = ..., index_and_retrieve_values: bool = ..., retrieved_literals: _Optional[_Iterable[_Union[RetrievalResult, _Mapping]]] = ..., cortex_search_service_name: _Optional[str] = ..., cortex_search_service: _Optional[_Union[CortexSearchService, _Mapping]] = ..., is_enum: bool = ...) -> None: ...

class Dimension(_message.Message):
    __slots__ = ("name", "synonyms", "description", "expr", "data_type", "unique", "sample_values", "cortex_search_service", "cortex_search_service_name", "is_enum")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SYNONYMS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_FIELD_NUMBER: _ClassVar[int]
    SAMPLE_VALUES_FIELD_NUMBER: _ClassVar[int]
    CORTEX_SEARCH_SERVICE_FIELD_NUMBER: _ClassVar[int]
    CORTEX_SEARCH_SERVICE_NAME_FIELD_NUMBER: _ClassVar[int]
    IS_ENUM_FIELD_NUMBER: _ClassVar[int]
    name: str
    synonyms: _containers.RepeatedScalarFieldContainer[str]
    description: str
    expr: str
    data_type: str
    unique: bool
    sample_values: _containers.RepeatedScalarFieldContainer[str]
    cortex_search_service: CortexSearchService
    cortex_search_service_name: str
    is_enum: bool
    def __init__(self, name: _Optional[str] = ..., synonyms: _Optional[_Iterable[str]] = ..., description: _Optional[str] = ..., expr: _Optional[str] = ..., data_type: _Optional[str] = ..., unique: bool = ..., sample_values: _Optional[_Iterable[str]] = ..., cortex_search_service: _Optional[_Union[CortexSearchService, _Mapping]] = ..., cortex_search_service_name: _Optional[str] = ..., is_enum: bool = ...) -> None: ...

class CortexSearchService(_message.Message):
    __slots__ = ("database", "schema", "service", "literal_column")
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    LITERAL_COLUMN_FIELD_NUMBER: _ClassVar[int]
    database: str
    schema: str
    service: str
    literal_column: str
    def __init__(self, database: _Optional[str] = ..., schema: _Optional[str] = ..., service: _Optional[str] = ..., literal_column: _Optional[str] = ...) -> None: ...

class TimeDimension(_message.Message):
    __slots__ = ("name", "synonyms", "description", "expr", "data_type", "unique", "sample_values")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SYNONYMS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_FIELD_NUMBER: _ClassVar[int]
    SAMPLE_VALUES_FIELD_NUMBER: _ClassVar[int]
    name: str
    synonyms: _containers.RepeatedScalarFieldContainer[str]
    description: str
    expr: str
    data_type: str
    unique: bool
    sample_values: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, name: _Optional[str] = ..., synonyms: _Optional[_Iterable[str]] = ..., description: _Optional[str] = ..., expr: _Optional[str] = ..., data_type: _Optional[str] = ..., unique: bool = ..., sample_values: _Optional[_Iterable[str]] = ...) -> None: ...

class Fact(_message.Message):
    __slots__ = ("name", "synonyms", "description", "expr", "data_type", "default_aggregation", "sample_values")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SYNONYMS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_AGGREGATION_FIELD_NUMBER: _ClassVar[int]
    SAMPLE_VALUES_FIELD_NUMBER: _ClassVar[int]
    name: str
    synonyms: _containers.RepeatedScalarFieldContainer[str]
    description: str
    expr: str
    data_type: str
    default_aggregation: AggregationType
    sample_values: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, name: _Optional[str] = ..., synonyms: _Optional[_Iterable[str]] = ..., description: _Optional[str] = ..., expr: _Optional[str] = ..., data_type: _Optional[str] = ..., default_aggregation: _Optional[_Union[AggregationType, str]] = ..., sample_values: _Optional[_Iterable[str]] = ...) -> None: ...

class NamedFilter(_message.Message):
    __slots__ = ("name", "synonyms", "description", "expr")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SYNONYMS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    name: str
    synonyms: _containers.RepeatedScalarFieldContainer[str]
    description: str
    expr: str
    def __init__(self, name: _Optional[str] = ..., synonyms: _Optional[_Iterable[str]] = ..., description: _Optional[str] = ..., expr: _Optional[str] = ...) -> None: ...

class FullyQualifiedTable(_message.Message):
    __slots__ = ("database", "schema", "table")
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    database: str
    schema: str
    table: str
    def __init__(self, database: _Optional[str] = ..., schema: _Optional[str] = ..., table: _Optional[str] = ...) -> None: ...

class PrimaryKey(_message.Message):
    __slots__ = ("columns",)
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, columns: _Optional[_Iterable[str]] = ...) -> None: ...

class ForeignKey(_message.Message):
    __slots__ = ("fkey_columns", "pkey_table", "pkey_columns")
    FKEY_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    PKEY_TABLE_FIELD_NUMBER: _ClassVar[int]
    PKEY_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    fkey_columns: _containers.RepeatedScalarFieldContainer[str]
    pkey_table: FullyQualifiedTable
    pkey_columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, fkey_columns: _Optional[_Iterable[str]] = ..., pkey_table: _Optional[_Union[FullyQualifiedTable, _Mapping]] = ..., pkey_columns: _Optional[_Iterable[str]] = ...) -> None: ...

class Table(_message.Message):
    __slots__ = ("name", "synonyms", "description", "base_table", "columns", "dimensions", "time_dimensions", "measures", "facts", "metrics", "primary_key", "foreign_keys", "filters")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SYNONYMS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    BASE_TABLE_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    DIMENSIONS_FIELD_NUMBER: _ClassVar[int]
    TIME_DIMENSIONS_FIELD_NUMBER: _ClassVar[int]
    MEASURES_FIELD_NUMBER: _ClassVar[int]
    FACTS_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_KEY_FIELD_NUMBER: _ClassVar[int]
    FOREIGN_KEYS_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    name: str
    synonyms: _containers.RepeatedScalarFieldContainer[str]
    description: str
    base_table: FullyQualifiedTable
    columns: _containers.RepeatedCompositeFieldContainer[Column]
    dimensions: _containers.RepeatedCompositeFieldContainer[Dimension]
    time_dimensions: _containers.RepeatedCompositeFieldContainer[TimeDimension]
    measures: _containers.RepeatedCompositeFieldContainer[Fact]
    facts: _containers.RepeatedCompositeFieldContainer[Fact]
    metrics: _containers.RepeatedCompositeFieldContainer[Metric]
    primary_key: PrimaryKey
    foreign_keys: _containers.RepeatedCompositeFieldContainer[ForeignKey]
    filters: _containers.RepeatedCompositeFieldContainer[NamedFilter]
    def __init__(self, name: _Optional[str] = ..., synonyms: _Optional[_Iterable[str]] = ..., description: _Optional[str] = ..., base_table: _Optional[_Union[FullyQualifiedTable, _Mapping]] = ..., columns: _Optional[_Iterable[_Union[Column, _Mapping]]] = ..., dimensions: _Optional[_Iterable[_Union[Dimension, _Mapping]]] = ..., time_dimensions: _Optional[_Iterable[_Union[TimeDimension, _Mapping]]] = ..., measures: _Optional[_Iterable[_Union[Fact, _Mapping]]] = ..., facts: _Optional[_Iterable[_Union[Fact, _Mapping]]] = ..., metrics: _Optional[_Iterable[_Union[Metric, _Mapping]]] = ..., primary_key: _Optional[_Union[PrimaryKey, _Mapping]] = ..., foreign_keys: _Optional[_Iterable[_Union[ForeignKey, _Mapping]]] = ..., filters: _Optional[_Iterable[_Union[NamedFilter, _Mapping]]] = ...) -> None: ...

class Metric(_message.Message):
    __slots__ = ("name", "synonyms", "description", "expr", "filter")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SYNONYMS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    name: str
    synonyms: _containers.RepeatedScalarFieldContainer[str]
    description: str
    expr: str
    filter: MetricsFilter
    def __init__(self, name: _Optional[str] = ..., synonyms: _Optional[_Iterable[str]] = ..., description: _Optional[str] = ..., expr: _Optional[str] = ..., filter: _Optional[_Union[MetricsFilter, _Mapping]] = ...) -> None: ...

class MetricsFilter(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: str
    def __init__(self, expr: _Optional[str] = ...) -> None: ...

class RelationKey(_message.Message):
    __slots__ = ("left_column", "right_column")
    LEFT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    RIGHT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    left_column: str
    right_column: str
    def __init__(self, left_column: _Optional[str] = ..., right_column: _Optional[str] = ...) -> None: ...

class Relationship(_message.Message):
    __slots__ = ("name", "left_table", "right_table", "expr", "relationship_columns", "join_type", "relationship_type")
    NAME_FIELD_NUMBER: _ClassVar[int]
    LEFT_TABLE_FIELD_NUMBER: _ClassVar[int]
    RIGHT_TABLE_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIP_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    JOIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIP_TYPE_FIELD_NUMBER: _ClassVar[int]
    name: str
    left_table: str
    right_table: str
    expr: str
    relationship_columns: _containers.RepeatedCompositeFieldContainer[RelationKey]
    join_type: JoinType
    relationship_type: RelationshipType
    def __init__(self, name: _Optional[str] = ..., left_table: _Optional[str] = ..., right_table: _Optional[str] = ..., expr: _Optional[str] = ..., relationship_columns: _Optional[_Iterable[_Union[RelationKey, _Mapping]]] = ..., join_type: _Optional[_Union[JoinType, str]] = ..., relationship_type: _Optional[_Union[RelationshipType, str]] = ...) -> None: ...

class ModuleCustomInstructions(_message.Message):
    __slots__ = ("sql_generation", "question_categorization")
    SQL_GENERATION_FIELD_NUMBER: _ClassVar[int]
    QUESTION_CATEGORIZATION_FIELD_NUMBER: _ClassVar[int]
    sql_generation: str
    question_categorization: str
    def __init__(self, sql_generation: _Optional[str] = ..., question_categorization: _Optional[str] = ...) -> None: ...

class SemanticModel(_message.Message):
    __slots__ = ("name", "description", "tables", "relationships", "verified_queries", "custom_instructions", "module_custom_instructions")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIPS_FIELD_NUMBER: _ClassVar[int]
    VERIFIED_QUERIES_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_INSTRUCTIONS_FIELD_NUMBER: _ClassVar[int]
    MODULE_CUSTOM_INSTRUCTIONS_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    tables: _containers.RepeatedCompositeFieldContainer[Table]
    relationships: _containers.RepeatedCompositeFieldContainer[Relationship]
    verified_queries: _containers.RepeatedCompositeFieldContainer[VerifiedQuery]
    custom_instructions: str
    module_custom_instructions: ModuleCustomInstructions
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., tables: _Optional[_Iterable[_Union[Table, _Mapping]]] = ..., relationships: _Optional[_Iterable[_Union[Relationship, _Mapping]]] = ..., verified_queries: _Optional[_Iterable[_Union[VerifiedQuery, _Mapping]]] = ..., custom_instructions: _Optional[str] = ..., module_custom_instructions: _Optional[_Union[ModuleCustomInstructions, _Mapping]] = ...) -> None: ...

class VerifiedQuery(_message.Message):
    __slots__ = ("name", "semantic_model_name", "question", "sql", "verified_at", "verified_by", "use_as_onboarding_question")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SEMANTIC_MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    SQL_FIELD_NUMBER: _ClassVar[int]
    VERIFIED_AT_FIELD_NUMBER: _ClassVar[int]
    VERIFIED_BY_FIELD_NUMBER: _ClassVar[int]
    USE_AS_ONBOARDING_QUESTION_FIELD_NUMBER: _ClassVar[int]
    name: str
    semantic_model_name: str
    question: str
    sql: str
    verified_at: int
    verified_by: str
    use_as_onboarding_question: bool
    def __init__(self, name: _Optional[str] = ..., semantic_model_name: _Optional[str] = ..., question: _Optional[str] = ..., sql: _Optional[str] = ..., verified_at: _Optional[int] = ..., verified_by: _Optional[str] = ..., use_as_onboarding_question: bool = ...) -> None: ...

class VerifiedQueryRepository(_message.Message):
    __slots__ = ("verified_queries",)
    VERIFIED_QUERIES_FIELD_NUMBER: _ClassVar[int]
    verified_queries: _containers.RepeatedCompositeFieldContainer[VerifiedQuery]
    def __init__(self, verified_queries: _Optional[_Iterable[_Union[VerifiedQuery, _Mapping]]] = ...) -> None: ...
