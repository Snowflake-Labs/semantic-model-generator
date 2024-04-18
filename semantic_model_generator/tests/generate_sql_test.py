import pytest

from semantic_model_generator.protos.semantic_model_pb2 import (
    AggregationType,
    Dimension,
    FullyQualifiedTable,
    Measure,
    Table,
    TimeDimension,
)
from semantic_model_generator.sqlgen.generate_sql import generate_select_with_all_cols

dimension_example = Dimension(
    name="Region",
    synonyms=["Area", "Locale"],
    description="Geographical region",
    expr="region_code",
    data_type="string",
    unique=False,
    sample_values=["North", "South", "East", "West"],
)

dimension_example_no_cols = Dimension(
    synonyms=["Area", "Locale"],
    description="Geographical region",
)

dimension_example_invalid_name = Dimension(
    name="Regions In The World",
    synonyms=["Area", "Locale"],
    description="Geographical region",
    expr="region_code",
    data_type="string",
    unique=False,
    sample_values=["North", "South", "East", "West"],
)

time_dimension_example = TimeDimension(
    name="Date",
    synonyms=["Time"],
    description="Transaction date",
    expr="transaction_date",
    data_type="date",
    unique=False,
    sample_values=["2022-01-01", "2022-01-02", "2022-01-03"],
)

measure_example = Measure(
    name="Total_Sales",
    synonyms=["Sales", "Revenue"],
    description="Total sales amount",
    expr="sales_amount - sales_total",
    data_type="float",
    default_aggregation=AggregationType.sum,
    sample_values=["1000.50", "2000.75", "1500.00"],
)


fully_qualified_table_example = FullyQualifiedTable(
    database="SalesDB", schema="public", table="transactions"
)

_TEST_VALID_TABLE = Table(
    name="Transactions",
    synonyms=["Transaction Records"],
    description="Table containing transaction records",
    base_table=fully_qualified_table_example,
    dimensions=[dimension_example],
    time_dimensions=[time_dimension_example],
    measures=[measure_example],
)


_TEST_TABLE_NO_COLS = Table(
    name="Transactions",
    synonyms=["Transaction Records"],
    description="Table containing transaction records",
    base_table=fully_qualified_table_example,
    dimensions=[dimension_example_no_cols],
)

_TEST_TABLE_INVALID_NAME = Table(
    name="Transactions",
    synonyms=["Transaction Records"],
    description="Table containing transaction records",
    base_table=fully_qualified_table_example,
    dimensions=[dimension_example_invalid_name],
)


def test_valid_table_sql_with_expr():
    want = "SELECT region_code AS Region, sales_amount - sales_total AS Total_Sales, transaction_date AS Date FROM SalesDB.public.transactions LIMIT 100"
    generated_sql = generate_select_with_all_cols(_TEST_VALID_TABLE, 100)
    assert generated_sql == want


def test_table_no_cols():
    with pytest.raises(ValueError) as excinfo:
        _ = generate_select_with_all_cols(_TEST_TABLE_NO_COLS, 100)
    assert (
        str(excinfo.value)
        == "No columns found for table Transactions. Please remove this"
    )


def test_table_invalid_col_name():
    with pytest.raises(ValueError) as excinfo:
        _ = generate_select_with_all_cols(_TEST_TABLE_INVALID_NAME, 100)
    assert (
        str(excinfo.value)
        == "Column names should not have spaces in them. Passed = Regions In The World"
    )
