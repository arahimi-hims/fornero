"""Tests documenting known inconsistencies between the implementation and
the formal semantics defined in design-docs/ARCHITECTURE.md, plus bugs
catalogued in design-docs/BUG_FIX_PLAN.md.

Every test in this file is marked ``xfail`` — it describes the *correct*
behaviour and is expected to fail against the current code.  When a bug is
fixed the corresponding test will start passing, and pytest will report it
as XPASS so you know to remove the ``xfail`` marker.
"""

from __future__ import annotations

import ast

import pandas as pd
import pytest

import fornero
from fornero.algebra import (
    LogicalPlan,
    Source,
    Select,
    Filter,
    Join,
    GroupBy,
    Aggregate,
    Sort,
    Limit,
    WithColumn,
    Union,
    Pivot,
    Melt,
    Window,
    BinaryOp,
    Column,
    Literal,
    execute,
)
from fornero.exceptions import UnsupportedOperationError
from fornero.spreadsheet.operations import SetFormula
from fornero.translator import Translator


class TestGroupByFirstAppearanceOrder:
    """ARCHITECTURE.md §GroupBy defines:

        'The order of groups is the order of first appearance in R.'

    The translation uses Google Sheets ``QUERY … GROUP BY`` which sorts
    groups alphabetically, violating this invariant.  A correct translation
    would either avoid ``QUERY GROUP BY`` entirely (e.g. ``UNIQUE`` +
    ``SUMIFS``) or include an explicit ``ORDER BY`` / ``LABEL`` clause that
    restores the original order.
    """

    def test_groupby_formula_does_not_rely_solely_on_query_group_by(self):
        source = Source(source_id="s", schema=["g", "v"])
        gb = GroupBy(
            keys=["g"],
            aggregations=[("total", "sum", "v")],
            inputs=[source],
        )

        plan = LogicalPlan(gb)
        translator = Translator()
        ops = translator.translate(
            plan,
            source_data={"s": [["banana", 1], ["apple", 2], ["banana", 3]]},
        )

        query_formulas = [
            op for op in ops
            if isinstance(op, SetFormula) and "QUERY" in op.formula
        ]

        # QUERY with GROUP BY alone cannot preserve insertion order.
        # A correct translation must either:
        #   - avoid QUERY GROUP BY entirely (e.g. UNIQUE + SUMIFS), or
        #   - include an explicit ORDER BY clause restoring original order
        if query_formulas:
            formula = query_formulas[0].formula
            assert "GROUP BY" not in formula or "ORDER BY" in formula, (
                "QUERY GROUP BY does not preserve first-appearance order; "
                "the formula must include an ordering mechanism or use an "
                "alternative strategy"
            )
        else:
            # No QUERY formulas - using alternative strategy (UNIQUE + SUMIFS)
            # This is the preferred solution
            pass

    def test_groupby_output_header_matches_first_appearance(self):
        """The QUERY formula itself emits a header row with the column
        alias produced by the aggregation (e.g. 'sum v'), not the output
        name specified in the aggregation triple ('total').  This means
        the spreadsheet column headers may not match the schema the
        translator advertises to downstream operations.

        The correct solution is to use UNIQUE + SUMIFS which allows setting
        headers explicitly via set_values operation.
        """
        source = Source(source_id="s", schema=["g", "v"])
        gb = GroupBy(
            keys=["g"],
            aggregations=[("total", "sum", "v")],
            inputs=[source],
        )

        plan = LogicalPlan(gb)
        translator = Translator()
        ops = translator.translate(
            plan,
            source_data={"s": [["banana", 1], ["apple", 2], ["banana", 3]]},
        )

        query_formulas = [
            op for op in ops
            if isinstance(op, SetFormula) and "QUERY" in op.formula
        ]

        # If using QUERY, it should include a LABEL clause to rename columns.
        # However, the preferred solution is to avoid QUERY entirely and use
        # UNIQUE + SUMIFS with explicit header setting.
        if query_formulas:
            formula = query_formulas[0].formula
            # QUERY emits its own header, and the translator relies on it
            # (formula is placed at row 0).  However QUERY names its columns
            # using its own conventions (e.g. "sum v"), not the user-specified
            # output names (e.g. "total").  The formula should include a LABEL
            # clause to rename the output columns.
            assert "LABEL" in formula, (
                "QUERY formula should include a LABEL clause to rename "
                "aggregation columns to the user-specified output names "
                f"(e.g. 'total'), but the formula is: {formula}"
            )
        else:
            # No QUERY formulas - using UNIQUE + SUMIFS with explicit headers
            # This is the preferred solution that avoids the LABEL issue
            pass


class TestWithColumnLambdaIsAnalyzed:
    """BUG_FIX_PLAN.md §Bug 2: ``assign()`` stores every callable as the
    literal string ``"lambda expression"`` instead of analyzing the lambda
    to produce a translatable arithmetic expression.

    Affected tests: p07, p17, p18, p21.
    """

    def test_assign_captures_lambda_arithmetic(self):
        df = fornero.DataFrame(
            {"name": ["Alice", "Bob"], "salary": [90000, 85000]},
            source_id="emp",
        )
        result = df.assign(salary_k=lambda x: x["salary"] / 1000)

        expr = result._plan.root.expression
        assert "lambda" not in expr.lower(), (
            f"WithColumn expression should contain the analyzed arithmetic "
            f"(e.g. 'salary / 1000'), not a placeholder. Got: {expr!r}"
        )
        assert "/" in expr or "div" in expr.lower(), (
            f"WithColumn expression should reflect the division operation. "
            f"Got: {expr!r}"
        )

    def test_with_column_formula_contains_division(self):
        df = fornero.DataFrame(
            {"name": ["Alice", "Bob"], "salary": [90000, 85000]},
            source_id="emp",
        )
        result = df.assign(salary_k=lambda x: x["salary"] / 1000)

        source_data = {"emp": df.values.tolist()}
        translator = Translator()
        ops = translator.translate(result._plan, source_data=source_data)

        # The new column "salary_k" is the last column in the output schema
        wc_formulas = [
            op for op in ops
            if isinstance(op, SetFormula)
            and op.sheet.startswith("WithColumn")
            and op.col == 2  # salary_k is at index 2 (after name, salary)
        ]
        assert len(wc_formulas) == 1

        formula = wc_formulas[0].formula
        assert "/1000" in formula.replace(" ", "") or "/ 1000" in formula, (
            f"WithColumn formula should contain '/1000' division, "
            f"got: {formula}"
        )


class TestPivotColumnLayout:
    """BUG_FIX_PLAN.md §Bug 4: Pivot header formula ``={helper}!A1:1``
    references the entire first row of the helper sheet instead of being
    constrained to the actual number of distinct pivot values.

    Affected test: p20.
    """

    def test_pivot_header_is_constrained_to_actual_columns(self):
        source = Source(source_id="s", schema=["dept", "quarter", "revenue"])
        pivot = Pivot(
            index=["dept"],
            columns="quarter",
            values="revenue",
            aggfunc="sum",
            inputs=[source],
        )

        plan = LogicalPlan(pivot)
        translator = Translator()
        ops = translator.translate(
            plan,
            source_data={
                "s": [
                    ["eng", "Q1", 100],
                    ["eng", "Q2", 200],
                    ["sales", "Q1", 150],
                    ["sales", "Q2", 250],
                ],
            },
        )

        header_formulas = [
            op for op in ops
            if isinstance(op, SetFormula)
            and op.row == 0
            and not op.sheet.endswith("_distinct")
        ]

        for sf in header_formulas:
            assert "A1:1" not in sf.formula, (
                f"Pivot header should reference only the actual pivot "
                f"columns (e.g. A1:B1 for 2 values), not the entire "
                f"row. Got: {sf.formula}"
            )


class TestDemoUsesProperConstructor:
    """BUG_FIX_PLAN.md §Bug 6: ``examples/end_to_end_demo.py`` bypasses
    the ``SheetsClient`` constructor with ``__new__`` instead of using
    ``SheetsClient(gc)``.
    """

    def test_demo_does_not_use_dunder_new(self):
        import pathlib

        demo_path = pathlib.Path(__file__).resolve().parent.parent / "examples" / "end_to_end_demo.py"
        source = demo_path.read_text()
        tree = ast.parse(source)

        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == "__new__":
                pytest.fail(
                    "end_to_end_demo.py should use SheetsClient(gc) "
                    "instead of SheetsClient.__new__(SheetsClient)"
                )


class TestJoinMultiKeyDropsAllRightKeys:
    """BUG_FIX_PLAN.md §Bug 7: when joining on multiple keys, only the
    first right key is removed from the output schema.  The remaining
    right keys leak through as redundant columns.
    """

    def test_multi_key_join_excludes_all_right_keys(self):
        left = Source(source_id="l", schema=["k1", "k2", "val_l"])
        right = Source(source_id="r", schema=["rk1", "rk2", "val_r"])
        join = Join(
            left_on=["k1", "k2"],
            right_on=["rk1", "rk2"],
            join_type="left",
            inputs=[left, right],
        )

        plan = LogicalPlan(join)
        translator = Translator()
        translator.translate(
            plan,
            source_data={
                "l": [["a", 1, 100]],
                "r": [["a", 1, 200]],
            },
        )

        context = translator.materialized[id(join)]
        output_schema = context.schema

        assert "rk1" not in output_schema, (
            f"Right key 'rk1' should be excluded from output schema, "
            f"got: {output_schema}"
        )
        assert "rk2" not in output_schema, (
            f"Right key 'rk2' should be excluded from output schema, "
            f"got: {output_schema}"
        )


class TestInnerJoinFilterAllRightColumns:
    """BUG_FIX_PLAN.md §Bug 8: the inner-join helper sheet filters
    unmatched rows by checking only the first right non-key column.
    The architecture requires checking whether *all* looked-up columns
    are null (i.e. keep a row if ANY right column is non-empty).
    """

    def test_inner_join_filter_checks_all_right_columns(self):
        left = Source(source_id="l", schema=["key", "val_l"])
        right = Source(source_id="r", schema=["rkey", "r1", "r2"])
        join = Join(
            left_on=["key"],
            right_on=["rkey"],
            join_type="inner",
            inputs=[left, right],
        )

        plan = LogicalPlan(join)
        translator = Translator()
        ops = translator.translate(
            plan,
            source_data={
                "l": [["a", 1], ["b", 2]],
                "r": [["a", 10, 20]],
            },
        )

        filter_formulas = [
            op for op in ops
            if isinstance(op, SetFormula) and "FILTER" in op.formula
        ]
        assert len(filter_formulas) >= 1

        formula = filter_formulas[0].formula

        non_empty_checks = formula.count('<>""')
        assert non_empty_checks >= 2, (
            f"Inner join FILTER should check all right non-key columns "
            f"for emptiness (expected >= 2 checks for r1 and r2), but "
            f"found {non_empty_checks}. Formula: {formula}"
        )


class TestUnionEagerSchemaValidation:
    """BUG_FIX_PLAN.md §Bug 9: the eager Union path calls ``pd.concat``
    without validating that both inputs have identical schemas, violating
    the architecture's precondition S(R1) = S(R2).
    """

    def test_union_eager_rejects_mismatched_schemas(self):
        left = Source(
            source_id="l",
            schema=["a", "b"],
            data=pd.DataFrame({"a": [1], "b": [2]}),
        )
        right = Source(
            source_id="r",
            schema=["x", "y"],
            data=pd.DataFrame({"x": [3], "y": [4]}),
        )
        union_op = Union(inputs=[left, right])

        with pytest.raises((ValueError, UnsupportedOperationError)):
            execute(union_op)


class TestWindowEagerRunningAggregate:
    """BUG_FIX_PLAN.md §Bug 10: the eager window executor uses
    ``grouped.transform("sum")`` which gives the partition-level total,
    not a running cumulative sum.  This diverges from the spreadsheet
    translation (which uses ``SUMIFS`` bounded to current row) and breaks
    the dual-mode invariant.
    """

    def test_window_running_sum_is_cumulative(self):
        source_data = pd.DataFrame({
            "grp": ["a", "a", "a"],
            "val": [10, 20, 30],
        })
        source = Source(
            source_id="s",
            schema=["grp", "val"],
            data=source_data,
        )
        window = Window(
            function="sum",
            input_column="val",
            output_column="running_total",
            partition_by=["grp"],
            order_by=[("val", "asc")],
            frame="unbounded preceding to current row",
            inputs=[source],
        )

        result = execute(window)

        assert list(result["running_total"]) == [10, 30, 60], (
            f"Running sum should be cumulative [10, 30, 60], "
            f"got {list(result['running_total'])}"
        )


class TestWindowRowNumberTranslation:
    """BUG_FIX_PLAN.md §Bug 11: the spreadsheet translation uses the
    same ``COUNTIFS`` formula for both ``rank`` and ``row_number``.
    ``COUNTIFS`` with ``<=`` produces rank semantics (tied values share
    the same number), not sequential row numbers.
    """

    def test_row_number_formula_differs_from_rank(self):
        source = Source(source_id="s", schema=["grp", "val"])

        window_rn = Window(
            function="row_number",
            input_column="val",
            output_column="rn",
            partition_by=["grp"],
            order_by=[("val", "asc")],
            inputs=[source],
        )
        window_rank = Window(
            function="rank",
            input_column="val",
            output_column="rnk",
            partition_by=["grp"],
            order_by=[("val", "asc")],
            inputs=[source],
        )

        data = [["a", 10], ["a", 10], ["a", 20]]

        t1 = Translator()
        ops_rn = t1.translate(
            LogicalPlan(window_rn), source_data={"s": data},
        )

        t2 = Translator()
        ops_rank = t2.translate(
            LogicalPlan(window_rank), source_data={"s": data},
        )

        rn_formulas = sorted(
            op.formula for op in ops_rn
            if isinstance(op, SetFormula)
            and "COUNTIFS" in op.formula
        )
        rank_formulas = sorted(
            op.formula for op in ops_rank
            if isinstance(op, SetFormula)
            and "COUNTIFS" in op.formula
        )

        assert rn_formulas != rank_formulas, (
            "row_number and rank should produce different formula strategies "
            "to handle tied values differently"
        )


class TestExpressionASTTranslation:
    """BUG_FIX_PLAN.md §Bug 12: ``_translate_predicate`` assumes its
    input is a string and calls ``.replace()`` on it.  Passing a
    ``BinaryOp`` or other Expression AST node raises ``AttributeError``.
    """

    def test_filter_translates_expression_ast_predicate(self):
        source = Source(source_id="s", schema=["age", "name"])
        pred = BinaryOp(
            op=">",
            left=Column(name="age"),
            right=Literal(value=25),
        )
        filter_op = Filter(predicate=pred, inputs=[source])

        plan = LogicalPlan(filter_op)
        translator = Translator()
        ops = translator.translate(plan, source_data={"s": [[30, "Alice"]]})

        filter_formulas = [
            op for op in ops
            if isinstance(op, SetFormula) and "FILTER" in op.formula
        ]
        assert len(filter_formulas) == 1

        formula = filter_formulas[0].formula
        assert "25" in formula, (
            f"Filter formula should contain the literal 25, got: {formula}"
        )
        assert "!" in formula, (
            f"Filter formula should contain a sheet reference, got: {formula}"
        )


class TestPivotMultiColumnIndex:
    """BUG_FIX_PLAN.md §Bug 13: ``translate_pivot`` extracts only the
    first index column via ``op.index[0]``, silently dropping any
    additional index columns.  The translation should either support
    composite indices or raise ``UnsupportedOperationError``.
    """

    def test_pivot_multi_index_raises_or_handles(self):
        source = Source(
            source_id="s",
            schema=["dept", "region", "quarter", "revenue"],
        )
        pivot = Pivot(
            index=["dept", "region"],
            columns="quarter",
            values="revenue",
            aggfunc="sum",
            inputs=[source],
        )

        plan = LogicalPlan(pivot)
        translator = Translator()

        try:
            ops = translator.translate(
                plan,
                source_data={
                    "s": [
                        ["eng", "US", "Q1", 100],
                        ["eng", "EU", "Q1", 200],
                        ["sales", "US", "Q1", 150],
                    ],
                },
            )
        except UnsupportedOperationError:
            return

        unique_formulas = [
            op for op in ops
            if isinstance(op, SetFormula)
            and "UNIQUE" in op.formula
            and op.col == 0
        ]
        assert len(unique_formulas) >= 1

        formula = unique_formulas[0].formula
        col_refs = formula.count("!")
        assert col_refs >= 2 or "{" in formula, (
            f"Pivot with multi-column index should reference all index "
            f"columns in the UNIQUE formula, or raise "
            f"UnsupportedOperationError. Got: {formula}"
        )
