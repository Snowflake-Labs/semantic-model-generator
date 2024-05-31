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
OPTIONAL_FIELD_NUMBER: _ClassVar[int]
optional: _descriptor.FieldDescriptor

class Dimension(_message.Message):
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

class Measure(_message.Message):
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

class Table(_message.Message):
    __slots__ = ("name", "synonyms", "description", "base_table", "columns", "dimensions", "time_dimensions", "measures", "filters")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SYNONYMS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    BASE_TABLE_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    DIMENSIONS_FIELD_NUMBER: _ClassVar[int]
    TIME_DIMENSIONS_FIELD_NUMBER: _ClassVar[int]
    MEASURES_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    name: str
    synonyms: _containers.RepeatedScalarFieldContainer[str]
    description: str
    base_table: FullyQualifiedTable
    columns: _containers.RepeatedCompositeFieldContainer[Column]
    dimensions: _containers.RepeatedCompositeFieldContainer[Dimension]
    time_dimensions: _containers.RepeatedCompositeFieldContainer[TimeDimension]
    measures: _containers.RepeatedCompositeFieldContainer[Measure]
    filters: _containers.RepeatedCompositeFieldContainer[NamedFilter]
    def __init__(self, name: _Optional[str] = ..., synonyms: _Optional[_Iterable[str]] = ..., description: _Optional[str] = ..., base_table: _Optional[_Union[FullyQualifiedTable, _Mapping]] = ..., columns: _Optional[_Iterable[_Union[Column, _Mapping]]] = ..., dimensions: _Optional[_Iterable[_Union[Dimension, _Mapping]]] = ..., time_dimensions: _Optional[_Iterable[_Union[TimeDimension, _Mapping]]] = ..., measures: _Optional[_Iterable[_Union[Measure, _Mapping]]] = ..., filters: _Optional[_Iterable[_Union[NamedFilter, _Mapping]]] = ...) -> None: ...

class SemanticModel(_message.Message):
    __slots__ = ("name", "description", "tables")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    tables: _containers.RepeatedCompositeFieldContainer[Table]
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., tables: _Optional[_Iterable[_Union[Table, _Mapping]]] = ...) -> None: ...

class Column(_message.Message):
    __slots__ = ("name", "synonyms", "description", "expr", "data_type", "kind", "unique", "default_aggregation", "sample_values")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SYNONYMS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_AGGREGATION_FIELD_NUMBER: _ClassVar[int]
    SAMPLE_VALUES_FIELD_NUMBER: _ClassVar[int]
    name: str
    synonyms: _containers.RepeatedScalarFieldContainer[str]
    description: str
    expr: str
    data_type: str
    kind: ColumnKind
    unique: bool
    default_aggregation: AggregationType
    sample_values: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, name: _Optional[str] = ..., synonyms: _Optional[_Iterable[str]] = ..., description: _Optional[str] = ..., expr: _Optional[str] = ..., data_type: _Optional[str] = ..., kind: _Optional[_Union[ColumnKind, str]] = ..., unique: bool = ..., default_aggregation: _Optional[_Union[AggregationType, str]] = ..., sample_values: _Optional[_Iterable[str]] = ...) -> None: ...

class VerifiedQuery(_message.Message):
    __slots__ = ("name", "semantic_model_name", "question", "sql", "verified_at", "verified_by")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SEMANTIC_MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    SQL_FIELD_NUMBER: _ClassVar[int]
    VERIFIED_AT_FIELD_NUMBER: _ClassVar[int]
    VERIFIED_BY_FIELD_NUMBER: _ClassVar[int]
    name: str
    semantic_model_name: str
    question: str
    sql: str
    verified_at: int
    verified_by: str
    def __init__(self, name: _Optional[str] = ..., semantic_model_name: _Optional[str] = ..., question: _Optional[str] = ..., sql: _Optional[str] = ..., verified_at: _Optional[int] = ..., verified_by: _Optional[str] = ...) -> None: ...

class VerifiedQueryRepository(_message.Message):
    __slots__ = ("verified_queries",)
    VERIFIED_QUERIES_FIELD_NUMBER: _ClassVar[int]
    verified_queries: _containers.RepeatedCompositeFieldContainer[VerifiedQuery]
    def __init__(self, verified_queries: _Optional[_Iterable[_Union[VerifiedQuery, _Mapping]]] = ...) -> None: ...
