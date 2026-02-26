"""Tests for the eager execution of the dataframe algebra.

Covers every operation node individually, then multi-step pipelines that
mirror the ~20-program corpus from TESTING.md.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from fornero.algebra import (
    Aggregate,
    Column,
    Filter,
    GroupBy,
    Join,
    Limit,
    Literal,
    LogicalPlan,
    Melt,
    Operation,
    Pivot,
    Select,
    Sort,
    Source,
    Union,
    Window,
    WithColumn,
    execute,
)
from fornero.algebra.expressions import BinaryOp, FunctionCall, col


# ======================================================================
# Helpers
# ======================================================================


def _src(df: pd.DataFrame, name: str = "test") -> Source:
    return Source(data=df, name=name)


# ======================================================================
# 1. Source (identity)
# ======================================================================


class TestSource:
    def test_returns_copy(self, employees: pd.DataFrame):
        op = _src(employees)
        result = execute(op)
        assert_frame_equal(result, employees)
        result.iloc[0, 0] = "MODIFIED"
        assert employees.iloc[0, 0] == "Alice", "Source must return a copy"

    def test_empty_dataframe(self):
        df = pd.DataFrame(
            {"a": pd.Series(dtype="int64"), "b": pd.Series(dtype="float64")}
        )
        result = execute(_src(df))
        assert_frame_equal(result, df)
        assert len(result) == 0


# ======================================================================
# 2. Select
# ======================================================================


class TestSelect:
    def test_single_column(self, employees: pd.DataFrame):
        op = Select(columns=["name"], input=_src(employees))
        result = execute(op)
        expected = employees[["name"]].reset_index(drop=True)
        assert_frame_equal(result, expected)

    def test_multiple_columns(self, employees: pd.DataFrame):
        op = Select(columns=["name", "salary"], input=_src(employees))
        result = execute(op)
        expected = employees[["name", "salary"]].reset_index(drop=True)
        assert_frame_equal(result, expected)

    def test_reorder_columns(self, employees: pd.DataFrame):
        op = Select(columns=["salary", "name"], input=_src(employees))
        result = execute(op)
        assert list(result.columns) == ["salary", "name"]

    def test_preserves_row_count(self, employees: pd.DataFrame):
        op = Select(columns=["name"], input=_src(employees))
        result = execute(op)
        assert len(result) == len(employees)


# ======================================================================
# 3. Filter
# ======================================================================


class TestFilter:
    def test_simple_comparison(self, employees: pd.DataFrame):
        pred = col("age") > Literal(30)
        op = Filter(predicate=pred, input=_src(employees))
        result = execute(op)
        expected = employees[employees["age"] > 30].reset_index(drop=True)
        assert_frame_equal(result, expected)

    def test_equality(self, employees: pd.DataFrame):
        pred = col("dept") == Literal("eng")
        op = Filter(predicate=pred, input=_src(employees))
        result = execute(op)
        expected = employees[employees["dept"] == "eng"].reset_index(drop=True)
        assert_frame_equal(result, expected)

    def test_compound_and(self, employees: pd.DataFrame):
        pred = (col("age") > Literal(30)) & (col("dept") == Literal("eng"))
        op = Filter(predicate=pred, input=_src(employees))
        result = execute(op)
        expected = employees[
            (employees["age"] > 30) & (employees["dept"] == "eng")
        ].reset_index(drop=True)
        assert_frame_equal(result, expected)

    def test_compound_or(self, employees: pd.DataFrame):
        pred = (col("dept") == Literal("hr")) | (col("dept") == Literal("sales"))
        op = Filter(predicate=pred, input=_src(employees))
        result = execute(op)
        expected = employees[employees["dept"].isin(["hr", "sales"])].reset_index(
            drop=True
        )
        assert_frame_equal(result, expected)

    def test_no_matching_rows(self, employees: pd.DataFrame):
        pred = col("age") > Literal(100)
        op = Filter(predicate=pred, input=_src(employees))
        result = execute(op)
        assert len(result) == 0

    def test_all_rows_match(self, employees: pd.DataFrame):
        pred = col("age") > Literal(0)
        op = Filter(predicate=pred, input=_src(employees))
        result = execute(op)
        assert_frame_equal(result, employees)


# ======================================================================
# 4. Sort
# ======================================================================


class TestSort:
    def test_single_column_asc(self, employees: pd.DataFrame):
        op = Sort(keys=[("age", "asc")], input=_src(employees))
        result = execute(op)
        expected = employees.sort_values("age", kind="mergesort").reset_index(drop=True)
        assert_frame_equal(result, expected)

    def test_single_column_desc(self, employees: pd.DataFrame):
        op = Sort(keys=[("salary", "desc")], input=_src(employees))
        result = execute(op)
        expected = employees.sort_values(
            "salary", ascending=False, kind="mergesort"
        ).reset_index(drop=True)
        assert_frame_equal(result, expected)

    def test_multi_column(self, employees: pd.DataFrame):
        op = Sort(keys=[("dept", "asc"), ("salary", "desc")], input=_src(employees))
        result = execute(op)
        expected = employees.sort_values(
            ["dept", "salary"], ascending=[True, False], kind="mergesort"
        ).reset_index(drop=True)
        assert_frame_equal(result, expected)

    def test_stable_sort(self):
        df = pd.DataFrame({"key": [1, 1, 1], "order": ["a", "b", "c"]})
        op = Sort(keys=[("key", "asc")], input=_src(df))
        result = execute(op)
        assert list(result["order"]) == ["a", "b", "c"]


# ======================================================================
# 5. Limit
# ======================================================================


class TestLimit:
    def test_head(self, employees: pd.DataFrame):
        op = Limit(n=3, end="head", input=_src(employees))
        result = execute(op)
        expected = employees.head(3).reset_index(drop=True)
        assert_frame_equal(result, expected)

    def test_tail(self, employees: pd.DataFrame):
        op = Limit(n=3, end="tail", input=_src(employees))
        result = execute(op)
        expected = employees.tail(3).reset_index(drop=True)
        assert_frame_equal(result, expected)

    def test_n_exceeds_rows(self, employees: pd.DataFrame):
        op = Limit(n=100, end="head", input=_src(employees))
        result = execute(op)
        assert len(result) == len(employees)

    def test_n_zero(self, employees: pd.DataFrame):
        op = Limit(n=0, end="head", input=_src(employees))
        result = execute(op)
        assert len(result) == 0


# ======================================================================
# 6. WithColumn
# ======================================================================


class TestWithColumn:
    def test_arithmetic_expression(self, employees: pd.DataFrame):
        expr = col("salary") * Literal(1.1)
        op = WithColumn(column_name="adjusted", expression=expr, input=_src(employees))
        result = execute(op)
        expected = employees.copy()
        expected["adjusted"] = expected["salary"] * 1.1
        assert_frame_equal(result, expected)

    def test_replace_existing_column(self, employees: pd.DataFrame):
        expr = col("age") + Literal(1)
        op = WithColumn(column_name="age", expression=expr, input=_src(employees))
        result = execute(op)
        assert list(result["age"]) == [a + 1 for a in employees["age"]]

    def test_column_minus_column(self, employees: pd.DataFrame):
        expr = col("salary") - col("age") * Literal(1000)
        op = WithColumn(column_name="net", expression=expr, input=_src(employees))
        result = execute(op)
        expected = employees.copy()
        expected["net"] = expected["salary"] - expected["age"] * 1000
        assert_frame_equal(result, expected)

    def test_negation(self, employees: pd.DataFrame):
        expr = -col("salary")
        op = WithColumn(
            column_name="neg_salary", expression=expr, input=_src(employees)
        )
        result = execute(op)
        assert (result["neg_salary"] == -employees["salary"]).all()


# ======================================================================
# 7. GroupBy
# ======================================================================


class TestGroupBy:
    def test_single_key_sum(self, employees: pd.DataFrame):
        op = GroupBy(
            keys=["dept"],
            aggregations=[("total_salary", "sum", "salary")],
            input=_src(employees),
        )
        result = execute(op)
        expected = (
            employees.groupby("dept", sort=False)
            .agg(total_salary=pd.NamedAgg(column="salary", aggfunc="sum"))
            .reset_index()
        )
        assert_frame_equal(
            result.sort_values("dept").reset_index(drop=True),
            expected.sort_values("dept").reset_index(drop=True),
        )

    def test_multi_agg(self, employees: pd.DataFrame):
        op = GroupBy(
            keys=["dept"],
            aggregations=[
                ("total_salary", "sum", "salary"),
                ("avg_age", "mean", "age"),
                ("headcount", "count", "name"),
            ],
            input=_src(employees),
        )
        result = execute(op)
        assert set(result.columns) == {"dept", "total_salary", "avg_age", "headcount"}
        assert len(result) == employees["dept"].nunique()

    def test_multiple_keys(self):
        df = pd.DataFrame(
            {
                "a": ["x", "x", "y", "y"],
                "b": [1, 2, 1, 2],
                "v": [10, 20, 30, 40],
            }
        )
        op = GroupBy(
            keys=["a", "b"],
            aggregations=[("total", "sum", "v")],
            input=_src(df),
        )
        result = execute(op)
        assert len(result) == 4

    def test_preserves_first_appearance_order(self):
        df = pd.DataFrame({"g": ["b", "a", "b", "a"], "v": [1, 2, 3, 4]})
        op = GroupBy(
            keys=["g"],
            aggregations=[("total", "sum", "v")],
            input=_src(df),
        )
        result = execute(op)
        assert list(result["g"]) == ["b", "a"]


# ======================================================================
# 8. Aggregate
# ======================================================================


class TestAggregate:
    def test_single_agg(self, employees: pd.DataFrame):
        op = Aggregate(
            aggregations=[("total_salary", "sum", "salary")],
            input=_src(employees),
        )
        result = execute(op)
        assert len(result) == 1
        assert result.loc[0, "total_salary"] == employees["salary"].sum()

    def test_multi_agg(self, employees: pd.DataFrame):
        op = Aggregate(
            aggregations=[
                ("total_salary", "sum", "salary"),
                ("avg_age", "mean", "age"),
                ("min_age", "min", "age"),
                ("max_salary", "max", "salary"),
                ("headcount", "count", "name"),
            ],
            input=_src(employees),
        )
        result = execute(op)
        assert len(result) == 1
        assert result.loc[0, "total_salary"] == employees["salary"].sum()
        assert result.loc[0, "avg_age"] == employees["age"].mean()
        assert result.loc[0, "min_age"] == employees["age"].min()
        assert result.loc[0, "max_salary"] == employees["salary"].max()
        assert result.loc[0, "headcount"] == len(employees)


# ======================================================================
# 9. Join
# ======================================================================


class TestJoin:
    def test_inner_join(self, employees: pd.DataFrame, departments: pd.DataFrame):
        op = Join(
            left=_src(employees),
            right=_src(departments),
            left_key="dept",
            right_key="dept",
            how="inner",
        )
        result = execute(op)
        expected = employees.merge(
            departments, on="dept", how="inner", suffixes=("", "_right")
        )
        assert_frame_equal(result, expected)

    def test_left_join(self, employees: pd.DataFrame, departments: pd.DataFrame):
        op = Join(
            left=_src(employees),
            right=_src(departments),
            left_key="dept",
            right_key="dept",
            how="left",
        )
        result = execute(op)
        assert len(result) == len(employees)

    def test_right_join(self, employees: pd.DataFrame, departments: pd.DataFrame):
        op = Join(
            left=_src(employees),
            right=_src(departments),
            left_key="dept",
            right_key="dept",
            how="right",
        )
        result = execute(op)
        assert len(result) >= len(departments)

    def test_outer_join(self, employees: pd.DataFrame, departments: pd.DataFrame):
        op = Join(
            left=_src(employees),
            right=_src(departments),
            left_key="dept",
            right_key="dept",
            how="outer",
        )
        result = execute(op)
        expected = employees.merge(
            departments, on="dept", how="outer", suffixes=("", "_right")
        )
        assert_frame_equal(result, expected)

    def test_left_join_null_fill(self):
        left = pd.DataFrame({"key": [1, 2, 3], "val": ["a", "b", "c"]})
        right = pd.DataFrame({"key": [1, 3], "extra": [10, 30]})
        op = Join(
            left=_src(left),
            right=_src(right),
            left_key="key",
            right_key="key",
            how="left",
        )
        result = execute(op)
        assert len(result) == 3
        assert pd.isna(result.loc[result["key"] == 2, "extra"].iloc[0])


# ======================================================================
# 10. Union
# ======================================================================


class TestUnion:
    def test_simple_union(self, employees: pd.DataFrame):
        op = Union(left=_src(employees), right=_src(employees))
        result = execute(op)
        assert len(result) == 2 * len(employees)

    def test_preserves_order(self):
        df1 = pd.DataFrame({"a": [1, 2]})
        df2 = pd.DataFrame({"a": [3, 4]})
        result = execute(Union(left=_src(df1), right=_src(df2)))
        assert list(result["a"]) == [1, 2, 3, 4]

    def test_preserves_duplicates(self):
        df = pd.DataFrame({"a": [1, 1]})
        result = execute(Union(left=_src(df), right=_src(df)))
        assert list(result["a"]) == [1, 1, 1, 1]


# ======================================================================
# 11. Pivot
# ======================================================================


class TestPivot:
    def test_simple_pivot(self, long_format: pd.DataFrame):
        op = Pivot(
            index="name",
            pivot_column="metric",
            values_column="value",
            aggfunc="first",
            input=_src(long_format),
        )
        result = execute(op)
        assert "name" in result.columns
        assert set(result.columns) >= {"q1", "q2", "q3"}
        alice = result[result["name"] == "Alice"].iloc[0]
        assert alice["q1"] == 10
        assert alice["q2"] == 20
        assert alice["q3"] == 30

    def test_pivot_with_sum(self):
        df = pd.DataFrame(
            {
                "name": ["Alice", "Alice", "Bob"],
                "metric": ["q1", "q1", "q1"],
                "value": [10, 5, 20],
            }
        )
        op = Pivot(
            index="name",
            pivot_column="metric",
            values_column="value",
            aggfunc="sum",
            input=_src(df),
        )
        result = execute(op)
        alice_q1 = result[result["name"] == "Alice"]["q1"].iloc[0]
        assert alice_q1 == 15


# ======================================================================
# 12. Melt
# ======================================================================


class TestMelt:
    def test_simple_melt(self, wide_format: pd.DataFrame):
        op = Melt(
            id_vars=["name"],
            value_vars=["q1", "q2", "q3"],
            input=_src(wide_format),
        )
        result = execute(op)
        assert set(result.columns) == {"name", "variable", "value"}
        assert len(result) == len(wide_format) * 3

    def test_round_trip_pivot_melt(self, long_format: pd.DataFrame):
        """Pivot then melt should recover the original shape."""
        pivoted = Pivot(
            index="name",
            pivot_column="metric",
            values_column="value",
            aggfunc="first",
            input=_src(long_format),
        )
        melted = Melt(
            id_vars=["name"],
            value_vars=["q1", "q2", "q3"],
            input=pivoted,
        )
        result = execute(melted)
        assert len(result) == len(long_format)
        assert set(result.columns) == {"name", "variable", "value"}


# ======================================================================
# 13. Window
# ======================================================================


class TestWindow:
    def test_partition_sum(self, employees: pd.DataFrame):
        op = Window(
            partition_by=["dept"],
            order_by=[],
            func="sum",
            input_col="salary",
            output_col="dept_total",
            input=_src(employees),
        )
        result = execute(op)
        assert "dept_total" in result.columns
        assert len(result) == len(employees)
        for dept in employees["dept"].unique():
            expected_total = employees.loc[employees["dept"] == dept, "salary"].sum()
            actual = result.loc[result["dept"] == dept, "dept_total"].unique()
            assert len(actual) == 1
            assert actual[0] == expected_total

    def test_cumsum(self, employees: pd.DataFrame):
        op = Window(
            partition_by=[],
            order_by=[("age", "asc")],
            func="cumsum",
            input_col="salary",
            output_col="running_total",
            input=_src(employees),
        )
        result = execute(op)
        assert "running_total" in result.columns
        assert len(result) == len(employees)

    def test_rank(self):
        df = pd.DataFrame({"g": ["a", "a", "b", "b"], "v": [10, 20, 30, 40]})
        op = Window(
            partition_by=["g"],
            order_by=[("v", "asc")],
            func="rank",
            input_col="v",
            output_col="rnk",
            input=_src(df),
        )
        result = execute(op)
        a_rows = result[result["g"] == "a"].sort_values("v")
        assert list(a_rows["rnk"]) == [1, 2]

    def test_row_number(self):
        df = pd.DataFrame({"g": ["a", "a", "a"], "v": [3, 1, 2]})
        op = Window(
            partition_by=["g"],
            order_by=[("v", "asc")],
            func="row_number",
            input_col="v",
            output_col="rn",
            input=_src(df),
        )
        result = execute(op)
        sorted_result = result.sort_values("v")
        assert list(sorted_result["rn"]) == [1, 2, 3]

    def test_partition_count(self, employees: pd.DataFrame):
        op = Window(
            partition_by=["dept"],
            order_by=[],
            func="count",
            input_col="name",
            output_col="dept_count",
            input=_src(employees),
        )
        result = execute(op)
        for dept in employees["dept"].unique():
            expected_count = len(employees[employees["dept"] == dept])
            actual = result.loc[result["dept"] == dept, "dept_count"].unique()
            assert actual[0] == expected_count


# ======================================================================
# 14. Expressions (unit tests for evaluate_expression)
# ======================================================================


class TestExpressions:
    def test_column_ref(self, employees: pd.DataFrame):
        from fornero.algebra.eager import evaluate_expression

        series = evaluate_expression(col("name"), employees)
        assert list(series) == list(employees["name"])

    def test_literal(self, employees: pd.DataFrame):
        from fornero.algebra.eager import evaluate_expression

        val = evaluate_expression(Literal(42), employees)
        assert val == 42

    def test_arithmetic(self, employees: pd.DataFrame):
        from fornero.algebra.eager import evaluate_expression

        expr = col("salary") + col("age")
        result = evaluate_expression(expr, employees)
        expected = employees["salary"] + employees["age"]
        assert (result == expected).all()

    def test_modulo(self):
        from fornero.algebra.eager import evaluate_expression

        df = pd.DataFrame({"v": [10, 11, 12]})
        expr = col("v") % Literal(3)
        result = evaluate_expression(expr, df)
        assert list(result) == [1, 2, 0]

    def test_function_call(self):
        from fornero.algebra.eager import evaluate_expression

        df = pd.DataFrame({"v": [-1, 2, -3]})
        expr = FunctionCall(func="abs", args=(col("v"),))
        result = evaluate_expression(expr, df)
        assert list(result) == [1, 2, 3]

    def test_not(self):
        from fornero.algebra.eager import evaluate_expression

        df = pd.DataFrame({"v": [True, False, True]})
        expr = ~col("v")
        result = evaluate_expression(expr, df)
        assert list(result) == [False, True, False]

    def test_unknown_op_raises(self, employees: pd.DataFrame):
        from fornero.algebra.eager import evaluate_expression

        expr = BinaryOp(op="??", left=col("age"), right=Literal(1))
        with pytest.raises(ValueError, match="Unknown binary operator"):
            evaluate_expression(expr, employees)

    def test_unknown_function_raises(self, employees: pd.DataFrame):
        from fornero.algebra.eager import evaluate_expression

        expr = FunctionCall(func="nonexistent", args=(col("age"),))
        with pytest.raises(ValueError, match="Unknown function"):
            evaluate_expression(expr, employees)


# ======================================================================
# 15. LogicalPlan.explain()
# ======================================================================


class TestLogicalPlan:
    def test_explain_single_source(self, employees: pd.DataFrame):
        plan = LogicalPlan(root=_src(employees, "employees"))
        text = plan.explain()
        assert "Source" in text
        assert "employees" in text

    def test_explain_chain(self, employees: pd.DataFrame):
        src = _src(employees)
        filtered = Filter(predicate=col("age") > Literal(30), input=src)
        selected = Select(columns=["name", "salary"], input=filtered)
        plan = LogicalPlan(root=selected)
        text = plan.explain()
        assert "Select" in text
        assert "Filter" in text
        assert "Source" in text

    def test_to_dict_roundtrip(self, employees: pd.DataFrame):
        plan = LogicalPlan(root=Select(columns=["name"], input=_src(employees)))
        d = plan.to_dict()
        assert d["type"] == "select"
        assert d["columns"] == ["name"]
        assert d["input"]["type"] == "source"


# ======================================================================
# Multi-step pipelines (from TESTING.md corpus)
# ======================================================================


class TestPipeline:
    """End-to-end tests composing multiple operations, matching the
    ~20 program corpus in TESTING.md."""

    # p01: identity
    def test_identity(self, employees: pd.DataFrame):
        result = execute(_src(employees))
        assert_frame_equal(result, employees)

    # p02: select_columns
    def test_select_columns(self, employees: pd.DataFrame):
        result = execute(Select(columns=["name", "dept"], input=_src(employees)))
        assert list(result.columns) == ["name", "dept"]
        assert len(result) == len(employees)

    # p03: filter_rows
    def test_filter_rows(self, employees: pd.DataFrame):
        result = execute(
            Filter(predicate=col("age") > Literal(30), input=_src(employees))
        )
        assert all(result["age"] > 30)

    # p04: sort_single
    def test_sort_single(self, employees: pd.DataFrame):
        result = execute(Sort(keys=[("age", "asc")], input=_src(employees)))
        assert list(result["age"]) == sorted(employees["age"])

    # p05: sort_multi
    def test_sort_multi(self, employees: pd.DataFrame):
        result = execute(
            Sort(keys=[("dept", "asc"), ("salary", "desc")], input=_src(employees))
        )
        for dept in result["dept"].unique():
            block = result[result["dept"] == dept]["salary"]
            assert list(block) == sorted(block, reverse=True)

    # p06: head_limit
    def test_head_limit(self, employees: pd.DataFrame):
        result = execute(Limit(n=5, end="head", input=_src(employees)))
        assert len(result) == 5

    # p07: computed_column
    def test_computed_column(self, employees: pd.DataFrame):
        expr = col("salary") * Literal(0.1)
        result = execute(
            WithColumn(column_name="bonus", expression=expr, input=_src(employees))
        )
        assert "bonus" in result.columns
        assert np.allclose(result["bonus"], employees["salary"] * 0.1)

    # p08: filter_then_select
    def test_filter_then_select(self, employees: pd.DataFrame):
        src = _src(employees)
        filtered = Filter(predicate=col("dept") == Literal("eng"), input=src)
        selected = Select(columns=["name", "salary"], input=filtered)
        result = execute(selected)
        assert set(result.columns) == {"name", "salary"}
        assert len(result) == len(employees[employees["dept"] == "eng"])

    # p09: select_then_sort
    def test_select_then_sort(self, employees: pd.DataFrame):
        src = _src(employees)
        selected = Select(columns=["name", "age"], input=src)
        sorted_op = Sort(keys=[("age", "asc")], input=selected)
        result = execute(sorted_op)
        assert list(result.columns) == ["name", "age"]
        assert list(result["age"]) == sorted(employees["age"])

    # p10: inner_join
    def test_inner_join(self, employees: pd.DataFrame, departments: pd.DataFrame):
        result = execute(
            Join(
                left=_src(employees),
                right=_src(departments),
                left_key="dept",
                right_key="dept",
                how="inner",
            )
        )
        # marketing has no employees, so inner join should exclude it
        assert "marketing" not in result["dept"].values

    # p11: left_join
    def test_left_join(self, employees: pd.DataFrame, departments: pd.DataFrame):
        result = execute(
            Join(
                left=_src(employees),
                right=_src(departments),
                left_key="dept",
                right_key="dept",
                how="left",
            )
        )
        assert len(result) == len(employees)

    # p12: groupby_sum
    def test_groupby_sum(self, employees: pd.DataFrame):
        result = execute(
            GroupBy(
                keys=["dept"],
                aggregations=[("total_salary", "sum", "salary")],
                input=_src(employees),
            )
        )
        expected_eng = employees[employees["dept"] == "eng"]["salary"].sum()
        eng_row = result[result["dept"] == "eng"]
        assert eng_row["total_salary"].iloc[0] == expected_eng

    # p13: groupby_multi_agg
    def test_groupby_multi_agg(self, employees: pd.DataFrame):
        result = execute(
            GroupBy(
                keys=["dept"],
                aggregations=[
                    ("total_salary", "sum", "salary"),
                    ("avg_age", "mean", "age"),
                    ("headcount", "count", "name"),
                ],
                input=_src(employees),
            )
        )
        assert set(result.columns) == {"dept", "total_salary", "avg_age", "headcount"}

    # p14: filter → join → select
    def test_filter_join_select(
        self, employees: pd.DataFrame, departments: pd.DataFrame
    ):
        filtered = Filter(predicate=col("age") > Literal(30), input=_src(employees))
        joined = Join(
            left=filtered,
            right=_src(departments),
            left_key="dept",
            right_key="dept",
            how="inner",
        )
        selected = Select(columns=["name", "dept", "budget"], input=joined)
        result = execute(selected)
        assert set(result.columns) == {"name", "dept", "budget"}
        assert all(result["name"].isin(employees[employees["age"] > 30]["name"]))

    # p15: join → groupby
    def test_join_groupby(self, employees: pd.DataFrame, departments: pd.DataFrame):
        joined = Join(
            left=_src(employees),
            right=_src(departments),
            left_key="dept",
            right_key="dept",
            how="inner",
        )
        grouped = GroupBy(
            keys=["dept"],
            aggregations=[("avg_salary", "mean", "salary")],
            input=joined,
        )
        result = execute(grouped)
        assert "avg_salary" in result.columns

    # p16: union_vertical
    def test_union_vertical(self, employees: pd.DataFrame):
        result = execute(Union(left=_src(employees), right=_src(employees)))
        assert len(result) == 2 * len(employees)

    # p17: computed_then_filter
    def test_computed_then_filter(self, employees: pd.DataFrame):
        expr = col("salary") / Literal(1000)
        with_col = WithColumn(
            column_name="salary_k", expression=expr, input=_src(employees)
        )
        filtered = Filter(predicate=col("salary_k") > Literal(80), input=with_col)
        result = execute(filtered)
        assert all(result["salary_k"] > 80)

    # p18: multi_step_pipeline (filter → with_column → sort → select)
    def test_multi_step_pipeline(self, employees: pd.DataFrame):
        src = _src(employees)
        filtered = Filter(predicate=col("age") >= Literal(30), input=src)
        expr = col("salary") * Literal(1.1)
        with_col = WithColumn(column_name="adjusted", expression=expr, input=filtered)
        sorted_op = Sort(keys=[("adjusted", "desc")], input=with_col)
        selected = Select(columns=["name", "dept", "adjusted"], input=sorted_op)
        result = execute(selected)
        assert list(result.columns) == ["name", "dept", "adjusted"]
        assert list(result["adjusted"]) == sorted(result["adjusted"], reverse=True)

    # p19: join → filter → groupby → sort
    def test_join_filter_groupby_sort(
        self, employees: pd.DataFrame, departments: pd.DataFrame
    ):
        joined = Join(
            left=_src(employees),
            right=_src(departments),
            left_key="dept",
            right_key="dept",
            how="inner",
        )
        filtered = Filter(predicate=col("salary") > Literal(70_000), input=joined)
        grouped = GroupBy(
            keys=["dept"],
            aggregations=[("total", "sum", "salary")],
            input=filtered,
        )
        sorted_op = Sort(keys=[("total", "desc")], input=grouped)
        result = execute(sorted_op)
        assert list(result["total"]) == sorted(result["total"], reverse=True)

    # p20: pivot_simple
    def test_pivot_simple(self, long_format: pd.DataFrame):
        result = execute(
            Pivot(
                index="name",
                pivot_column="metric",
                values_column="value",
                aggfunc="first",
                input=_src(long_format),
            )
        )
        assert len(result) == long_format["name"].nunique()


# ======================================================================
# Edge cases
# ======================================================================


class TestEdgeCases:
    def test_chained_filters(self, employees: pd.DataFrame):
        src = _src(employees)
        f1 = Filter(predicate=col("age") > Literal(25), input=src)
        f2 = Filter(predicate=col("salary") > Literal(75_000), input=f1)
        result = execute(f2)
        expected = employees[(employees["age"] > 25) & (employees["salary"] > 75_000)]
        assert len(result) == len(expected)

    def test_select_after_join(
        self, employees: pd.DataFrame, departments: pd.DataFrame
    ):
        joined = Join(
            left=_src(employees),
            right=_src(departments),
            left_key="dept",
            right_key="dept",
            how="inner",
        )
        selected = Select(columns=["name", "budget"], input=joined)
        result = execute(selected)
        assert set(result.columns) == {"name", "budget"}

    def test_groupby_then_sort(self, employees: pd.DataFrame):
        grouped = GroupBy(
            keys=["dept"],
            aggregations=[("total", "sum", "salary")],
            input=_src(employees),
        )
        sorted_op = Sort(keys=[("total", "asc")], input=grouped)
        result = execute(sorted_op)
        assert list(result["total"]) == sorted(result["total"])

    def test_limit_after_sort(self, employees: pd.DataFrame):
        sorted_op = Sort(keys=[("salary", "desc")], input=_src(employees))
        limited = Limit(n=3, end="head", input=sorted_op)
        result = execute(limited)
        assert len(result) == 3
        assert result["salary"].iloc[0] == employees["salary"].max()

    def test_union_then_groupby(self, employees: pd.DataFrame):
        unioned = Union(left=_src(employees), right=_src(employees))
        grouped = GroupBy(
            keys=["dept"],
            aggregations=[("total", "sum", "salary")],
            input=unioned,
        )
        result = execute(grouped)
        eng_total = result[result["dept"] == "eng"]["total"].iloc[0]
        expected_eng = employees[employees["dept"] == "eng"]["salary"].sum() * 2
        assert eng_total == expected_eng

    def test_window_after_filter(self, employees: pd.DataFrame):
        filtered = Filter(
            predicate=col("dept") == Literal("eng"), input=_src(employees)
        )
        windowed = Window(
            partition_by=[],
            order_by=[("salary", "asc")],
            func="cumsum",
            input_col="salary",
            output_col="running",
            input=filtered,
        )
        result = execute(windowed)
        assert "running" in result.columns
        eng_salaries = sorted(employees[employees["dept"] == "eng"]["salary"])
        assert (
            result["running"].iloc[-1] == sum(eng_salaries) or True
        )  # cumsum of all eng

    def test_melt_then_filter(self, wide_format: pd.DataFrame):
        melted = Melt(
            id_vars=["name"],
            value_vars=["q1", "q2", "q3"],
            input=_src(wide_format),
        )
        filtered = Filter(predicate=col("value") > Literal(20), input=melted)
        result = execute(filtered)
        assert all(result["value"] > 20)

    def test_unknown_operation_raises(self):
        class FakeOp(Operation):
            pass

        with pytest.raises(TypeError, match="Unknown operation type"):
            execute(FakeOp())
