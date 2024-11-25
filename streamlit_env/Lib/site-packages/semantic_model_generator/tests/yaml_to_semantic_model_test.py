import pytest
from strictyaml import YAMLValidationError

from semantic_model_generator.data_processing.proto_utils import yaml_to_semantic_model


def test_valid_yaml():
    yaml = """
name: jaffle_shop
tables:
  - name: orders
    description: Order overview data mart, offering key details for each order including
      if it's a customer's first order and a food vs. drink item breakdown. One row
      per order.
    base_table:
      database: autosql_dataset_dbt_jaffle_shop
      schema: data
      table: orders
    filters:
      - name: large_order
        expr: cogs > 100
      - name: custom_filter
        expr: my_udf(col1, col2)
      - name: window_func
        expr: COUNT(i) OVER (PARTITION BY p ORDER BY o) count_i_Range_Pre
"""
    assert yaml_to_semantic_model(yaml) is not None


def test_invalid_sql():
    yaml = """
name: jaffle_shop
tables:
  - name: orders
    description: Order overview data mart, offering key details for each order including
      if it's a customer's first order and a food vs. drink item breakdown. One row
      per order.
    base_table:
      database: autosql_dataset_dbt_jaffle_shop
      schema: data
      table: orders
    filters:
      - name: large_order
        expr: (cogs > 100
"""
    with pytest.raises(YAMLValidationError, match=r".*invalid SQL expression.*"):
        yaml_to_semantic_model(yaml)


def test_required_field_missing():
    yaml = """
name: jaffle_shop
tables:
  - name: orders
    description: Order overview data mart, offering key details for each order including
      if it's a customer's first order and a food vs. drink item breakdown. One row
      per order.
    base_table:
      database: autosql_dataset_dbt_jaffle_shop
      schema: data
"""
    with pytest.raises(
        YAMLValidationError, match=r".*required key.*table.*not found.*"
    ):
        yaml_to_semantic_model(yaml)


def test_non_string_sample_value():
    yaml = """
name: jaffle_shop
tables:
  - name: orders
    description: Order overview data mart, offering key details for each order including
      if it's a customer's first order and a food vs. drink item breakdown. One row
      per order.
    base_table:
      database: autosql_dataset_dbt_jaffle_shop
      schema: data
      table: orders
    columns:
      - name: order_id
        expr: order_id
        data_type: TEXT
        kind: dimension
        unique: true
        sample_values:
          - yes
          - 1
          - 05-17-2024
"""
    ctx = yaml_to_semantic_model(yaml)
    for sample_value in ctx.tables[0].columns[0].sample_values:
        assert isinstance(sample_value, str)
