from unittest import TestCase

import pytest
import sqlglot

from semantic_model_generator.data_processing.cte_utils import (
    _enrich_column_in_expr_with_aggregation,
    _get_col_expr,
    _validate_col,
    context_to_column_format,
    expand_all_logical_tables_as_ctes,
    generate_select,
    is_aggregation_expr,
)
from semantic_model_generator.protos import semantic_model_pb2


def get_test_ctx() -> semantic_model_pb2.SemanticModel:
    return semantic_model_pb2.SemanticModel(
        name="test_ctx",
        tables=[
            semantic_model_pb2.Table(
                name="t1",
                base_table=semantic_model_pb2.FullyQualifiedTable(
                    database="db", schema="sc", table="t1"
                ),
                dimensions=[
                    semantic_model_pb2.Dimension(
                        name="d1",
                        description="d1_description",
                        synonyms=["d1_synonym1", "d1_synonym2"],
                        expr="d1_expr",
                        data_type="d1_data_type",
                        unique=True,
                        sample_values=["d1_sample_value1", "d1_sample_value2"],
                    ),
                    semantic_model_pb2.Dimension(
                        name="d2",
                        description="d2_description",
                        expr="d2_expr",
                    ),
                ],
            ),
            semantic_model_pb2.Table(
                name="t2",
                base_table=semantic_model_pb2.FullyQualifiedTable(
                    database="db", schema="sc", table="t2"
                ),
                time_dimensions=[
                    semantic_model_pb2.TimeDimension(
                        name="td1",
                        description="td1_description",
                        synonyms=["td1_synonym1", "td1_synonym2"],
                        expr="td1_expr",
                        data_type="td1_data_type",
                        unique=True,
                        sample_values=["td1_sample_value1", "td1_sample_value2"],
                    ),
                ],
                measures=[
                    semantic_model_pb2.Fact(
                        name="m1",
                        description="m1_description",
                        synonyms=["m1_synonym1", "m1_synonym2"],
                        expr="m1_expr",
                        data_type="m1_data_type",
                        default_aggregation=semantic_model_pb2.AggregationType.avg,
                        sample_values=["m1_sample_value1", "m1_sample_value2"],
                    ),
                    semantic_model_pb2.Fact(
                        name="m2",
                        description="m1_description",
                        expr="m1_expr",
                    ),
                    semantic_model_pb2.Fact(
                        name="total_m3",
                        description="m3_description",
                        expr="sum(m3_expr)",
                    ),
                ],
            ),
        ],
    )


def get_test_ctx_col_format() -> semantic_model_pb2.SemanticModel:
    return semantic_model_pb2.SemanticModel(
        name="test_ctx",
        tables=[
            semantic_model_pb2.Table(
                name="t1",
                base_table=semantic_model_pb2.FullyQualifiedTable(
                    database="db", schema="sc", table="t1"
                ),
                columns=[
                    semantic_model_pb2.Column(
                        name="d1",
                        kind=semantic_model_pb2.ColumnKind.dimension,
                        description="d1_description",
                        synonyms=["d1_synonym1", "d1_synonym2"],
                        expr="d1_expr",
                        data_type="d1_data_type",
                        unique=True,
                        sample_values=["d1_sample_value1", "d1_sample_value2"],
                    ),
                    semantic_model_pb2.Column(
                        name="d2",
                        kind=semantic_model_pb2.ColumnKind.dimension,
                        description="d2_description",
                        expr="d2_expr",
                    ),
                ],
            ),
            semantic_model_pb2.Table(
                name="t2",
                base_table=semantic_model_pb2.FullyQualifiedTable(
                    database="db", schema="sc", table="t2"
                ),
                columns=[
                    semantic_model_pb2.Column(
                        name="td1",
                        kind=semantic_model_pb2.ColumnKind.time_dimension,
                        description="td1_description",
                        synonyms=["td1_synonym1", "td1_synonym2"],
                        expr="td1_expr",
                        data_type="td1_data_type",
                        unique=True,
                        sample_values=["td1_sample_value1", "td1_sample_value2"],
                    ),
                    semantic_model_pb2.Column(
                        name="m1",
                        kind=semantic_model_pb2.ColumnKind.measure,
                        description="m1_description",
                        synonyms=["m1_synonym1", "m1_synonym2"],
                        expr="m1_expr",
                        data_type="m1_data_type",
                        default_aggregation=semantic_model_pb2.AggregationType.avg,
                        sample_values=["m1_sample_value1", "m1_sample_value2"],
                    ),
                    semantic_model_pb2.Column(
                        name="m2",
                        kind=semantic_model_pb2.ColumnKind.measure,
                        description="m1_description",
                        expr="m1_expr",
                    ),
                    semantic_model_pb2.Column(
                        name="total_m3",
                        kind=semantic_model_pb2.ColumnKind.measure,
                        description="m3_description",
                        expr="sum(m3_expr)",
                    ),
                ],
            ),
        ],
    )


def get_test_table_col_format() -> semantic_model_pb2.Table:
    return semantic_model_pb2.Table(
        name="t1",
        base_table=semantic_model_pb2.FullyQualifiedTable(
            database="db", schema="sc", table="t1"
        ),
        columns=[
            semantic_model_pb2.Column(
                name="d1",
                kind=semantic_model_pb2.ColumnKind.dimension,
                description="d1_description",
                synonyms=["d1_synonym1", "d1_synonym2"],
                expr="d1_expr",
                data_type="d1_data_type",
                unique=True,
                sample_values=["d1_sample_value1", "d1_sample_value2"],
            ),
            semantic_model_pb2.Column(
                name="d2",
                kind=semantic_model_pb2.ColumnKind.dimension,
                description="d2_description",
                expr="d2_expr",
            ),
        ],
    )


def get_test_table_col_format_w_agg() -> semantic_model_pb2.Table:
    return semantic_model_pb2.Table(
        name="t1",
        base_table=semantic_model_pb2.FullyQualifiedTable(
            database="db", schema="sc", table="t1"
        ),
        columns=[
            semantic_model_pb2.Column(
                name="d1",
                kind=semantic_model_pb2.ColumnKind.dimension,
                description="d1_description",
                synonyms=["d1_synonym1", "d1_synonym2"],
                expr="d1_expr",
                data_type="d1_data_type",
                unique=True,
                sample_values=["d1_sample_value1", "d1_sample_value2"],
            ),
            semantic_model_pb2.Column(
                name="d2_total",
                kind=semantic_model_pb2.ColumnKind.measure,
                description="d2_description",
                expr="sum(d2)",
            ),
            semantic_model_pb2.Column(
                name="d3",
                kind=semantic_model_pb2.ColumnKind.measure,
                description="d3_description",
                expr="sum(d3) over (partition by d1)",
            ),
        ],
    )


def get_test_table_col_format_w_agg_only() -> semantic_model_pb2.Table:
    return semantic_model_pb2.Table(
        name="t1",
        base_table=semantic_model_pb2.FullyQualifiedTable(
            database="db", schema="sc", table="t1"
        ),
        columns=[
            semantic_model_pb2.Column(
                name="d2_total",
                kind=semantic_model_pb2.ColumnKind.measure,
                description="d2_description",
                expr="sum(d2)",
            ),
        ],
    )


def get_test_table_col_format_agg_and_renaming() -> semantic_model_pb2.Table:
    return semantic_model_pb2.Table(
        name="t1",
        base_table=semantic_model_pb2.FullyQualifiedTable(
            database="db", schema="sc", table="t1"
        ),
        columns=[
            semantic_model_pb2.Column(
                name="cost",
                kind=semantic_model_pb2.ColumnKind.measure,
                expr="cst",
            ),
            semantic_model_pb2.Column(
                name="clicks",
                kind=semantic_model_pb2.ColumnKind.measure,
                expr="clcks",
            ),
            semantic_model_pb2.Column(
                name="cpc",
                kind=semantic_model_pb2.ColumnKind.measure,
                expr="sum(cst) / sum(clcks)",
            ),
        ],
    )


class SemanticModelTest(TestCase):
    def test_convert_to_column_format(self) -> None:
        """
        Verifies that Dimension/time_dimension/measure are appropriately
        converted into corresponding columns.
        """
        ctx = get_test_ctx()
        want = get_test_ctx_col_format()
        got = context_to_column_format(ctx)
        self.assertEqual(want, got)

    def test_convert_to_column_format_noop(self) -> None:
        """
        Verify that context_to_column_format() is a no-op if the context is already
        in column format.
        """
        # A context already in column format.
        ctx = get_test_ctx_col_format()
        got = context_to_column_format(ctx)
        self.assertEqual(ctx, got)

    def test_is_aggregation_expr(self) -> None:
        for col, want in [
            (semantic_model_pb2.Column(expr="foo", kind="measure"), False),
            (semantic_model_pb2.Column(expr="sum(foo)", kind="measure"), True),
            (
                semantic_model_pb2.Column(expr="sum(foo)/sum(bar)", kind="measure"),
                True,
            ),
            (semantic_model_pb2.Column(expr="avg(foo)", kind="measure"), True),
            (semantic_model_pb2.Column(expr="foo + bar", kind="measure"), False),
            (
                semantic_model_pb2.Column(
                    expr="sum(foo) over (partition by bar)", kind="measure"
                ),
                False,
            ),
        ]:
            with self.subTest():
                self.assertEqual(is_aggregation_expr(col), want)

    def test_generate_select(self) -> None:
        col_format_tbl = get_test_table_col_format()
        got = generate_select(col_format_tbl, 100)
        want = [
            "WITH __t1 AS (SELECT d1_expr AS d1, d2_expr AS d2 FROM db.sc.t1) SELECT * FROM __t1 LIMIT 100"
        ]
        assert got == want

    def test_generate_select_w_agg(self) -> None:
        col_format_tbl = get_test_table_col_format_w_agg()
        got = generate_select(col_format_tbl, 100)
        want = [
            "WITH __t1 AS (SELECT SUM(d2) AS d2_total FROM db.sc.t1) SELECT * FROM __t1 LIMIT 100",
            "WITH __t1 AS (SELECT d1_expr AS d1, SUM(d3) OVER (PARTITION BY d1) AS d3 FROM db.sc.t1) SELECT * FROM __t1 LIMIT 100",
        ]
        assert sorted(got) == sorted(want)

    def test_generate_select_w_agg_only(self) -> None:
        col_format_tbl = get_test_table_col_format_w_agg_only()
        got = generate_select(col_format_tbl, 100)
        want = [
            "WITH __t1 AS (SELECT SUM(d2) AS d2_total FROM db.sc.t1) SELECT * FROM __t1 LIMIT 100"
        ]
        assert sorted(got) == sorted(want)

    def test_col_expr_w_space(self) -> None:
        col = semantic_model_pb2.Column(
            name="d 1",
            kind=semantic_model_pb2.ColumnKind.dimension,
            description="d1_description",
            synonyms=["d1_synonym1", "d1_synonym2"],
            expr="d1_expr",
            data_type="d1_data_type",
            unique=True,
            sample_values=["d1_sample_value1", "d1_sample_value2"],
        )
        with pytest.raises(
            ValueError,
            match=f"Please do not include spaces in your column name: {col.name}",
        ):
            _validate_col(col)

    def test_col_expr_w_space_v2(self) -> None:
        col = semantic_model_pb2.Column(
            name="d1 ",
            kind=semantic_model_pb2.ColumnKind.dimension,
            description="d1_description",
            synonyms=["d1_synonym1", "d1_synonym2"],
            expr="d1_expr",
            data_type="d1_data_type",
            unique=True,
            sample_values=["d1_sample_value1", "d1_sample_value2"],
        )
        got = _get_col_expr(col)
        want = "d1_expr as d1"
        assert got == want

    def test_col_expr_object_type(self) -> None:
        col = semantic_model_pb2.Column(
            name="d1",
            kind=semantic_model_pb2.ColumnKind.dimension,
            description="d1_description",
            synonyms=["d1_synonym1", "d1_synonym2"],
            expr="d1_expr",
            data_type="variant",
            unique=True,
            sample_values=["d1_sample_value1", "d1_sample_value2"],
        )
        with pytest.raises(
            ValueError,
            match=f"We do not support object datatypes in the semantic model. Col {col.name} has data type {col.data_type}. Please remove this column from your semantic model or flatten it to non-object type.",
        ):
            _validate_col(col)

    def test_enrich_column_in_expr_with_aggregation(self) -> None:
        col_format_tbl = get_test_table_col_format_w_agg_only()
        got = _enrich_column_in_expr_with_aggregation(col_format_tbl)
        want = semantic_model_pb2.Table(
            name="t1",
            base_table=semantic_model_pb2.FullyQualifiedTable(
                database="db", schema="sc", table="t1"
            ),
            columns=[
                semantic_model_pb2.Column(
                    name="d2_total",
                    kind=semantic_model_pb2.ColumnKind.measure,
                    description="d2_description",
                    expr="sum(d2)",
                ),
                # Expand the column referred in expr.
                semantic_model_pb2.Column(
                    name="d2",
                    expr="d2",
                ),
            ],
        )
        assert got == want

    def test_enrich_column_in_expr_with_aggregation_and_renaming(self) -> None:
        tbl = get_test_table_col_format_agg_and_renaming()
        got = [c for c in _enrich_column_in_expr_with_aggregation(tbl).columns]
        want = [
            semantic_model_pb2.Column(
                name="cost",
                kind=semantic_model_pb2.ColumnKind.measure,
                expr="cst",
            ),
            semantic_model_pb2.Column(
                name="clicks",
                kind=semantic_model_pb2.ColumnKind.measure,
                expr="clcks",
            ),
            semantic_model_pb2.Column(
                name="cpc",
                kind=semantic_model_pb2.ColumnKind.measure,
                expr="sum(cst) / sum(clcks)",
            ),
            semantic_model_pb2.Column(
                name="clcks",
                expr="clcks",
            ),
            semantic_model_pb2.Column(
                name="cst",
                expr="cst",
            ),
        ]
        got.sort(key=lambda c: c.name.lower())
        want.sort(key=lambda c: c.name.lower())
        assert got == want

    def test_expand_all_logical_tables_as_ctes(self) -> None:
        vq = "SELECT * FROM __t2"
        ctx = get_test_ctx_col_format()
        got = expand_all_logical_tables_as_ctes(vq, ctx)
        want = """WITH __t1 AS (SELECT
    d1_expr AS d1,
    d2_expr AS d2
  FROM db.sc.t1
), __t2 AS (
  SELECT
    td1_expr AS td1,
    m1_expr AS m1,
    m1_expr AS m2,
    m3_expr
  FROM db.sc.t2
)
SELECT
  *
FROM __t2"""
        assert sqlglot.parse_one(want, "snowflake") == sqlglot.parse_one(
            got, "snowflake"
        )

    def test_expand_all_logical_tables_as_ctes_with_column_renaming(self) -> None:
        ctx = semantic_model_pb2.SemanticModel(
            name="model", tables=[get_test_table_col_format_agg_and_renaming()]
        )
        logical_query = "SELECT * FROM __t1"
        got = expand_all_logical_tables_as_ctes(logical_query, ctx)
        want = """WITH __t1 AS (
  SELECT
    cst AS cost,
    clcks AS clicks,
    clcks,
    cst
  FROM db.sc.t1
)
SELECT
  *
FROM __t1
        """
        assert sqlglot.parse_one(want, "snowflake") == sqlglot.parse_one(
            got, "snowflake"
        )
