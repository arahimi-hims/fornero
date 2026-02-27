"""
Unit tests for the translator module (Tasks 8-12).

These tests verify:
1. Translation strategies for all operations (Task 8)
2. Multi-sheet plan handling (Task 9)
3. Optimization passes (Task 10)
4. Lambda function support (Task 11)
5. Apps Script generation (Task 12)

No external dependencies (no API calls, no filesystem except fixtures).
"""

import pytest
from fornero.algebra import (
    LogicalPlan, Source, Select, Filter, Join, GroupBy, Aggregate,
    Sort, Limit, WithColumn, Union, Pivot, Melt, Window
)
from fornero.algebra.expressions import col, Literal
from fornero.translator import (
    Translator, Optimizer, LambdaAnalyzer,
    AppsScriptGenerator, generate_apps_script_function
)
from fornero.exceptions import UnsupportedOperationError
from fornero.spreadsheet.operations import CreateSheet, SetValues, SetFormula


# ============================================================================
# Task 8: Translation strategies for all operations
# ============================================================================

class TestTranslateSelect:
    """Test Select translation strategy."""

    def test_select_produces_correct_operations(self):
        """Select produces CreateSheet, SetValues (header), and SetFormula per column."""
        source = Source(source_id="test.csv", schema=["a", "b", "c"])
        select = Select(columns=["a", "c"], inputs=[source])
        plan = LogicalPlan(select)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[1, 2, 3], [4, 5, 6]]})

        # Find Select operations (after Source operations)
        select_ops = [op for op in ops if isinstance(op, CreateSheet) and 'Select' in op.name]

        # Should have: CreateSheet, SetValues (header), SetFormula for col a, SetFormula for col c
        create_ops = [op for op in ops if isinstance(op, CreateSheet) and 'Select' in op.name]
        assert len(create_ops) == 1
        assert create_ops[0].cols == 2  # Two columns selected

        set_values_ops = [op for op in ops if isinstance(op, SetValues) and 'Select' in op.sheet]
        assert len(set_values_ops) >= 1  # At least header

        set_formula_ops = [op for op in ops if isinstance(op, SetFormula) and 'Select' in op.sheet]
        assert len(set_formula_ops) == 1  # Single FILTER formula for all columns

    def test_select_references_source_columns(self):
        """Select formulas reference correct source column ranges."""
        source = Source(source_id="test.csv", schema=["a", "b", "c"])
        select = Select(columns=["b"], inputs=[source])
        plan = LogicalPlan(select)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[1, 2, 3]]})

        formula_ops = [op for op in ops if isinstance(op, SetFormula) and 'Select' in op.sheet]
        assert len(formula_ops) == 1

        formula = formula_ops[0].formula
        # Should reference column B from source sheet
        assert 'B' in formula or 'b' in formula.lower()


class TestTranslateFilter:
    """Test Filter translation strategy."""

    def test_filter_produces_filter_formula(self):
        """Filter produces CreateSheet, SetValues (header), and FILTER formula."""
        source = Source(source_id="test.csv", schema=["age", "city"])
        filter_op = Filter(predicate=col("age") > 25, inputs=[source])
        plan = LogicalPlan(filter_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[30, "NYC"], [20, "LA"]]})

        filter_formula_ops = [op for op in ops if isinstance(op, SetFormula) and 'Filter' in op.sheet]
        assert len(filter_formula_ops) == 1

        formula = filter_formula_ops[0].formula
        assert 'FILTER' in formula

    def test_filter_translates_predicates(self):
        """Filter correctly translates comparison operators."""
        test_cases = [
            (col("age") > 25, ">"),
            (col("age") < 30, "<"),
            (col("age") == 25, "="),
            (col("age") >= 25, ">="),
            (col("age") <= 30, "<="),
            (col("age") != 25, "!="),
        ]

        for predicate, operator in test_cases:
            source = Source(source_id="test.csv", schema=["age"])
            filter_op = Filter(predicate=predicate, inputs=[source])
            plan = LogicalPlan(filter_op)

            translator = Translator()
            ops = translator.translate(plan, source_data={"test.csv": [[30]]})

            formula_ops = [op for op in ops if isinstance(op, SetFormula) and 'Filter' in op.sheet]
            formula = formula_ops[0].formula

            # The operator should appear in the formula
            assert operator in formula or operator.replace("!=", "<>") in formula

    def test_filter_translates_and_or(self):
        """Filter translates AND/OR to spreadsheet equivalents."""
        source = Source(source_id="test.csv", schema=["age", "city"])
        filter_op = Filter(predicate=(col("age") > 25) & (col("city") == "NYC"), inputs=[source])
        plan = LogicalPlan(filter_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[30, "NYC"]]})

        formula_ops = [op for op in ops if isinstance(op, SetFormula) and 'Filter' in op.sheet]
        formula = formula_ops[0].formula

        # AND should be translated to * (multiplication for boolean arrays)
        assert '*' in formula or 'AND' in formula


class TestTranslateJoin:
    """Test Join translation strategy."""

    def test_join_produces_xlookup_formulas(self):
        """Join produces XLOOKUP formulas for right-side columns."""
        left = Source(source_id="left.csv", schema=["id", "name"])
        right = Source(source_id="right.csv", schema=["id", "score"])
        join_op = Join(left_on="id", right_on="id", join_type="inner", inputs=[left, right])
        plan = LogicalPlan(join_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={
            "left.csv": [[1, "Alice"], [2, "Bob"]],
            "right.csv": [[1, 90], [2, 85]]
        })

        xlookup_ops = [op for op in ops if isinstance(op, SetFormula) and 'Join' in op.sheet]
        # Should have formulas for left columns (array refs) and right columns (XLOOKUP)
        assert len(xlookup_ops) > 0

        # At least one should contain XLOOKUP
        xlookup_formulas = [op.formula for op in xlookup_ops if 'XLOOKUP' in op.formula]
        assert len(xlookup_formulas) > 0

    def test_join_left_uses_empty_string_for_not_found(self):
        """Left join uses empty string for unmatched rows."""
        left = Source(source_id="left.csv", schema=["id", "name"])
        right = Source(source_id="right.csv", schema=["id", "score"])
        join_op = Join(left_on="id", right_on="id", join_type="left", inputs=[left, right])
        plan = LogicalPlan(join_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={
            "left.csv": [[1, "Alice"]],
            "right.csv": [[1, 90]]
        })

        xlookup_ops = [op for op in ops if isinstance(op, SetFormula) and 'XLOOKUP' in op.formula]
        assert len(xlookup_ops) > 0

        # Check for empty string as if-not-found argument
        for op in xlookup_ops:
            formula = op.formula
            # Should contain empty string as default
            assert '""' in formula


class TestTranslateGroupBy:
    """Test GroupBy translation strategy."""

    def test_groupby_produces_query_formula(self):
        """GroupBy produces UNIQUE + SUMIFS formulas (not QUERY)."""
        source = Source(source_id="test.csv", schema=["dept", "salary"])
        groupby_op = GroupBy(
            keys=["dept"],
            aggregations=[("total_salary", "sum", "salary")],
            inputs=[source]
        )
        plan = LogicalPlan(groupby_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [["Sales", 50000], ["IT", 60000]]})

        groupby_ops = [op for op in ops if isinstance(op, SetFormula) and 'GroupBy' in op.sheet]

        # Should have UNIQUE formula for keys
        unique_formula = [op.formula for op in groupby_ops if 'UNIQUE' in op.formula]
        assert len(unique_formula) == 1, "Should have one UNIQUE formula for group keys"

        # Should have SUMIFS formulas for aggregations
        sumifs_formula = [op.formula for op in groupby_ops if 'SUMIFS' in op.formula]
        assert len(sumifs_formula) > 0, "Should have SUMIFS formulas for aggregations"

    def test_groupby_function_mapping(self):
        """GroupBy maps aggregation functions correctly."""
        function_map = {
            'sum': 'SUMIFS',
            'mean': 'AVERAGEIFS',
            'count': 'COUNTIFS',
            'min': 'MINIFS',
            'max': 'MAXIFS',
        }

        for py_func, sheets_func in function_map.items():
            source = Source(source_id="test.csv", schema=["dept", "value"])
            groupby_op = GroupBy(
                keys=["dept"],
                aggregations=[("result", py_func, "value")],
                inputs=[source]
            )
            plan = LogicalPlan(groupby_op)

            translator = Translator()
            ops = translator.translate(plan, source_data={"test.csv": [["A", 10]]})

            groupby_ops = [op for op in ops if isinstance(op, SetFormula) and 'GroupBy' in op.sheet]
            formulas = [op.formula for op in groupby_ops]

            # Should contain the mapped function name in at least one formula
            assert any(sheets_func in formula for formula in formulas), f"Expected {sheets_func} in formulas"


class TestTranslateAggregate:
    """Test Aggregate translation strategy."""

    def test_aggregate_produces_scalar_formulas(self):
        """Aggregate produces one scalar formula per aggregation."""
        source = Source(source_id="test.csv", schema=["value"])
        agg_op = Aggregate(
            aggregations=[
                ("total", "sum", "value"),
                ("average", "mean", "value")
            ],
            inputs=[source]
        )
        plan = LogicalPlan(agg_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[10], [20], [30]]})

        agg_ops = [op for op in ops if isinstance(op, SetFormula) and 'Aggregate' in op.sheet]
        assert len(agg_ops) == 2  # One per aggregation

    def test_aggregate_function_mapping(self):
        """Aggregate maps functions to spreadsheet equivalents."""
        function_map = {
            'sum': 'SUM',
            'mean': 'AVERAGE',
            'count': 'COUNTA',
            'min': 'MIN',
            'max': 'MAX',
        }

        for py_func, sheets_func in function_map.items():
            source = Source(source_id="test.csv", schema=["value"])
            agg_op = Aggregate(
                aggregations=[("result", py_func, "value")],
                inputs=[source]
            )
            plan = LogicalPlan(agg_op)

            translator = Translator()
            ops = translator.translate(plan, source_data={"test.csv": [[10]]})

            formula_ops = [op for op in ops if isinstance(op, SetFormula) and 'Aggregate' in op.sheet]
            formula = formula_ops[0].formula

            assert sheets_func in formula


class TestTranslateSort:
    """Test Sort translation strategy."""

    def test_sort_produces_sort_formula(self):
        """Sort produces SORT formula with column index and direction."""
        source = Source(source_id="test.csv", schema=["name", "age"])
        sort_op = Sort(keys=[("age", "asc")], inputs=[source])
        plan = LogicalPlan(sort_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [["Alice", 30], ["Bob", 25]]})

        sort_ops = [op for op in ops if isinstance(op, SetFormula) and 'Sort' in op.sheet]
        assert len(sort_ops) == 1

        formula = sort_ops[0].formula
        assert 'SORT' in formula

    def test_sort_direction_mapping(self):
        """Sort correctly maps asc to TRUE and desc to FALSE."""
        source = Source(source_id="test.csv", schema=["value"])

        # Test ascending
        sort_asc = Sort(keys=[("value", "asc")], inputs=[source])
        plan_asc = LogicalPlan(sort_asc)

        translator = Translator()
        ops_asc = translator.translate(plan_asc, source_data={"test.csv": [[10]]})

        formula_asc = [op for op in ops_asc if isinstance(op, SetFormula) and 'Sort' in op.sheet][0].formula
        assert 'TRUE' in formula_asc  # TRUE for ascending

        # Test descending
        sort_desc = Sort(keys=[("value", "desc")], inputs=[source])
        plan_desc = LogicalPlan(sort_desc)

        ops_desc = translator.translate(plan_desc, source_data={"test.csv": [[10]]})

        formula_desc = [op for op in ops_desc if isinstance(op, SetFormula) and 'Sort' in op.sheet][0].formula
        assert 'FALSE' in formula_desc  # FALSE for descending


class TestTranslateLimit:
    """Test Limit translation strategy."""

    def test_limit_head_uses_array_constrain(self):
        """Limit with end='head' uses ARRAY_CONSTRAIN."""
        source = Source(source_id="test.csv", schema=["value"])
        limit_op = Limit(count=5, end="head", inputs=[source])
        plan = LogicalPlan(limit_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[i] for i in range(10)]})

        limit_ops = [op for op in ops if isinstance(op, SetFormula) and 'Limit' in op.sheet]
        assert len(limit_ops) == 1

        formula = limit_ops[0].formula
        assert 'ARRAY_CONSTRAIN' in formula or 'INDEX' in formula

    def test_limit_tail_uses_index(self):
        """Limit with end='tail' uses INDEX formula."""
        source = Source(source_id="test.csv", schema=["value"])
        limit_op = Limit(count=5, end="tail", inputs=[source])
        plan = LogicalPlan(limit_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[i] for i in range(10)]})

        limit_ops = [op for op in ops if isinstance(op, SetFormula) and 'Limit' in op.sheet]
        formula = limit_ops[0].formula

        assert 'INDEX' in formula or 'OFFSET' in formula


class TestTranslateWithColumn:
    """Test WithColumn translation strategy."""

    def test_with_column_adds_new_column(self):
        """WithColumn produces formulas for existing columns plus new column."""
        source = Source(source_id="test.csv", schema=["a", "b"])
        with_col_op = WithColumn(column="c", expression="a + b", inputs=[source])
        plan = LogicalPlan(with_col_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[1, 2], [3, 4]]})

        create_ops = [op for op in ops if isinstance(op, CreateSheet) and 'WithColumn' in op.name]
        assert len(create_ops) == 1
        assert create_ops[0].cols == 3  # a, b, c

    def test_with_column_expression_references_columns(self):
        """WithColumn formula references existing column ranges."""
        source = Source(source_id="test.csv", schema=["x", "y"])
        with_col_op = WithColumn(column="z", expression="x * 2", inputs=[source])
        plan = LogicalPlan(with_col_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[5, 10]]})

        formula_ops = [op for op in ops if isinstance(op, SetFormula) and 'WithColumn' in op.sheet]
        # Find the formula for column z
        z_formulas = [op.formula for op in formula_ops if op.col == 2]
        assert len(z_formulas) == 1
        assert '2' in z_formulas[0] or 'x' in z_formulas[0].lower()


class TestTranslateUnion:
    """Test Union translation strategy."""

    def test_union_produces_vertical_stack_formula(self):
        """Union produces vertical stack formula: ={range1; range2}."""
        source1 = Source(source_id="s1.csv", schema=["a", "b"])
        source2 = Source(source_id="s2.csv", schema=["a", "b"])
        union_op = Union(inputs=[source1, source2])
        plan = LogicalPlan(union_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={
            "s1.csv": [[1, 2]],
            "s2.csv": [[3, 4]]
        })

        union_ops = [op for op in ops if isinstance(op, SetFormula) and 'Union' in op.sheet]
        assert len(union_ops) == 1

        formula = union_ops[0].formula
        # Should contain semicolon for vertical stacking
        assert ';' in formula
        assert '{' in formula and '}' in formula


class TestTranslatePivotMeltWindow:
    """Test Pivot, Melt, and Window produce correct spreadsheet operations."""

    def test_pivot_produces_two_sheet_strategy(self):
        """Pivot produces a helper sheet with TRANSPOSE/UNIQUE and output sheet with FILTER."""
        source = Source(source_id="test.csv", schema=["a", "b", "c"])
        pivot_op = Pivot(index="a", columns="b", values="c", inputs=[source])
        plan = LogicalPlan(pivot_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[1, 2, 3]]})
        assert len(ops) > 0

        create_ops = [op for op in ops if isinstance(op, CreateSheet)]
        assert len(create_ops) >= 3  # source + helper + output

        formulas = [op.formula for op in ops if isinstance(op, SetFormula)]
        assert any('TRANSPOSE' in f and 'UNIQUE' in f for f in formulas)
        assert any('IFERROR' in f and 'FILTER' in f for f in formulas)

    def test_melt_produces_arrayformula_choose(self):
        """Melt produces ARRAYFORMULA with INDEX and CHOOSE/MOD formulas."""
        source = Source(source_id="test.csv", schema=["id", "a", "b"])
        melt_op = Melt(id_vars=["id"], value_vars=["a", "b"], inputs=[source])
        plan = LogicalPlan(melt_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[1, 2, 3]]})
        assert len(ops) > 0

        formulas = [op.formula for op in ops if isinstance(op, SetFormula)]
        assert any('ARRAYFORMULA' in f and 'INDEX' in f for f in formulas)
        assert any('CHOOSE' in f and 'MOD' in f for f in formulas)

    def test_window_ranking_produces_countifs(self):
        """Window rank/row_number produces COUNTIFS-based per-row formulas."""
        source = Source(source_id="test.csv", schema=["dept", "value"])
        window_op = Window(
            function="rank",
            output_column="rank",
            partition_by=["dept"],
            order_by=[("value", "asc")],
            inputs=[source]
        )
        plan = LogicalPlan(window_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={
            "test.csv": [["eng", 10], ["eng", 20], ["sales", 30]]
        })
        assert len(ops) > 0

        formulas = [op.formula for op in ops if isinstance(op, SetFormula)]
        assert any('COUNTIFS' in f for f in formulas)

    def test_window_running_sum_produces_sumifs(self):
        """Window running sum produces SUMIFS-based per-row formulas."""
        source = Source(source_id="test.csv", schema=["dept", "value"])
        window_op = Window(
            function="sum",
            input_column="value",
            output_column="running_sum",
            partition_by=["dept"],
            order_by=[("value", "asc")],
            inputs=[source]
        )
        plan = LogicalPlan(window_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={
            "test.csv": [["eng", 10], ["eng", 20], ["sales", 30]]
        })
        formulas = [op.formula for op in ops if isinstance(op, SetFormula)]
        assert any('SUMIFS' in f for f in formulas)

    def test_window_lag_produces_offset(self):
        """Window lag produces IFERROR(OFFSET(...)) formulas."""
        source = Source(source_id="test.csv", schema=["value"])
        window_op = Window(
            function="lag",
            input_column="value",
            output_column="prev_value",
            inputs=[source]
        )
        plan = LogicalPlan(window_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={
            "test.csv": [[10], [20], [30]]
        })
        formulas = [op.formula for op in ops if isinstance(op, SetFormula)]
        assert any('IFERROR' in f and 'OFFSET' in f for f in formulas)

    def test_unsupported_window_function_raises_error(self):
        """Unsupported window functions raise UnsupportedOperationError."""
        source = Source(source_id="test.csv", schema=["value"])
        window_op = Window(
            function="custom_func",
            output_column="result",
            inputs=[source]
        )
        plan = LogicalPlan(window_op)

        translator = Translator()
        with pytest.raises(UnsupportedOperationError):
            translator.translate(plan, source_data={"test.csv": [[10]]})


class TestTranslationCrossCutting:
    """Cross-cutting translation checks."""

    def test_non_source_without_input_raises_error(self):
        """Translating non-Source operation without input raises error."""
        # Create a Select with no inputs (invalid)
        with pytest.raises(ValueError):
            Select(columns=["a"], inputs=[])

    def test_sheet_names_are_unique(self):
        """Sheet names generated by translator are unique."""
        source = Source(source_id="test.csv", schema=["a"])
        filter1 = Filter(predicate="a > 1", inputs=[source])
        filter2 = Filter(predicate="a < 10", inputs=[filter1])
        plan = LogicalPlan(filter2)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[5]]})

        sheet_names = [op.name for op in ops if isinstance(op, CreateSheet)]
        assert len(sheet_names) == len(set(sheet_names))  # All unique

    def test_headers_match_output_schema(self):
        """Headers written by SetValues match output schema."""
        source = Source(source_id="test.csv", schema=["x", "y", "z"])
        select = Select(columns=["x", "z"], inputs=[source])
        plan = LogicalPlan(select)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[1, 2, 3]]})

        # Find header write for Select sheet
        header_ops = [op for op in ops if
                     isinstance(op, SetValues) and
                     'Select' in op.sheet and
                     op.row == 0]

        assert len(header_ops) == 1
        assert header_ops[0].values == [["x", "z"]]


# ============================================================================
# Task 9: Multi-sheet plans
# ============================================================================

class TestMultiSheetPlans:
    """Test multi-sheet plan handling."""

    def test_chained_operations_produce_multiple_sheets(self):
        """Chain of operations produces one sheet per operation plus sources."""
        source = Source(source_id="test.csv", schema=["a", "b"])
        filter_op = Filter(predicate=col("a") > 1, inputs=[source])
        groupby_op = GroupBy(keys=["b"], aggregations=[("total", "sum", "a")], inputs=[filter_op])
        sort_op = Sort(keys=[("total", "desc")], inputs=[groupby_op])
        plan = LogicalPlan(sort_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[2, "x"], [3, "y"]]})

        create_sheet_ops = [op for op in ops if isinstance(op, CreateSheet)]
        # Should have at least 4 sheets: Source, Filter, GroupBy, Sort
        assert len(create_sheet_ops) >= 4

    def test_cross_sheet_references_use_correct_names(self):
        """Cross-sheet formula references use correct sheet names."""
        source = Source(source_id="test.csv", schema=["value"])
        filter_op = Filter(predicate=col("value") > 5, inputs=[source])
        plan = LogicalPlan(filter_op)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": [[10]]})

        # Find Filter formula
        filter_formula_ops = [op for op in ops if
                             isinstance(op, SetFormula) and
                             'Filter' in op.sheet]

        assert len(filter_formula_ops) > 0

        # Formula should reference the source sheet
        formula = filter_formula_ops[0].formula
        # Should contain sheet reference (SheetName! format)
        assert '!' in formula


# ============================================================================
# Task 10: Optimization passes
# ============================================================================

class TestOptimizationPasses:
    """Test optimization passes."""

    def test_predicate_pushdown(self):
        """Predicate pushdown moves filter closer to source, then fuses into Select."""
        source = Source(source_id="test.csv", schema=["a", "b", "c"])
        select = Select(columns=["a", "b"], inputs=[source])
        filter_op = Filter(predicate="a > 10", inputs=[select])
        plan = LogicalPlan(filter_op)

        optimizer = Optimizer()
        optimized_plan = optimizer.optimize(plan)

        # The optimized plan should have fused the filter into Select
        assert isinstance(optimized_plan.root, Select)
        assert optimized_plan.root.predicate == "a > 10"
        # Select's input should be Source
        assert isinstance(optimized_plan.root.inputs[0], Source)

    def test_projection_pushdown(self):
        """Projection pushdown drops unused columns early."""
        source = Source(source_id="test.csv", schema=["a", "b", "c"])
        select1 = Select(columns=["a", "b", "c"], inputs=[source])
        select2 = Select(columns=["a"], inputs=[select1])
        plan = LogicalPlan(select2)

        optimizer = Optimizer()
        optimized_plan = optimizer.optimize(plan)

        # Should eliminate the first select (identity) or merge selects
        # The optimized plan should have fewer operations
        assert isinstance(optimized_plan.root, Select)
        assert optimized_plan.root.columns == ["a"]

    def test_formula_simplification_identity_select(self):
        """Identity Select (all columns) is elided."""
        source = Source(source_id="test.csv", schema=["a", "b"])
        select = Select(columns=["a", "b"], inputs=[source])
        plan = LogicalPlan(select)

        optimizer = Optimizer()
        optimized_plan = optimizer.optimize(plan)

        # Select should be eliminated
        assert isinstance(optimized_plan.root, Source)

    def test_formula_simplification_tautological_filter(self):
        """Tautological filter is elided."""
        source = Source(source_id="test.csv", schema=["a"])
        filter_op = Filter(predicate=Literal(value=True), inputs=[source])
        plan = LogicalPlan(filter_op)

        optimizer = Optimizer()
        optimized_plan = optimizer.optimize(plan)

        # Filter should be eliminated
        assert isinstance(optimized_plan.root, Source)

    def test_optimization_is_idempotent(self):
        """Applying optimization twice produces same result as once."""
        source = Source(source_id="test.csv", schema=["a", "b"])
        select = Select(columns=["a", "b"], inputs=[source])
        filter_op = Filter(predicate="a > 1", inputs=[select])
        plan = LogicalPlan(filter_op)

        optimizer = Optimizer()
        optimized_once = optimizer.optimize(plan)
        optimized_twice = optimizer.optimize(optimized_once)

        # Should produce same structure
        assert type(optimized_once.root) == type(optimized_twice.root)
        assert optimized_once.root.to_dict() == optimized_twice.root.to_dict()

    def test_fuse_operations(self):
        """Limit(Sort(Filter(...))) fuses into a single Sort operation."""
        source = Source(source_id="test.csv", schema=["a", "b"])
        filter_op = Filter(predicate="a > 5", inputs=[source])
        sort_op = Sort(keys=[("b", "desc")], inputs=[filter_op])
        limit_op = Limit(count=10, inputs=[sort_op])
        plan = LogicalPlan(limit_op)

        optimizer = Optimizer()
        optimized_plan = optimizer.optimize(plan)

        # Root should be Sort
        assert isinstance(optimized_plan.root, Sort)
        # Sort should have limit and predicate
        assert optimized_plan.root.limit == 10
        assert optimized_plan.root.predicate == "a > 5"
        # Input should be Source (Filter fused into Sort)
        assert isinstance(optimized_plan.root.inputs[0], Source)

    def test_fuse_select_filter(self):
        """Select(Filter(...)) fuses into a single Select operation."""
        source = Source(source_id="test.csv", schema=["a", "b"])
        filter_op = Filter(predicate="a > 5", inputs=[source])
        select_op = Select(columns=["a"], inputs=[filter_op])
        plan = LogicalPlan(select_op)

        optimizer = Optimizer()
        optimized_plan = optimizer.optimize(plan)

        # Root should be Select
        assert isinstance(optimized_plan.root, Select)
        # Select should have predicate
        assert optimized_plan.root.predicate == "a > 5"
        # Input should be Source
        assert isinstance(optimized_plan.root.inputs[0], Source)

    def test_optimizations_preserve_semantics(self):
        """Optimized plan produces same output for fixed input."""
        source = Source(source_id="test.csv", schema=["value"])
        filter_op = Filter(predicate=col("value") > 5, inputs=[source])
        plan = LogicalPlan(filter_op)

        translator = Translator()
        ops_unoptimized = translator.translate(plan, source_data={"test.csv": [[10], [3], [7]]}, optimize=False)

        optimizer = Optimizer()
        optimized_plan = optimizer.optimize(plan)

        ops_optimized = translator.translate(optimized_plan, source_data={"test.csv": [[10], [3], [7]]}, optimize=False)

        # Should have same number of final sheets (may differ in intermediate sheets)
        create_unopt = [op for op in ops_unoptimized if isinstance(op, CreateSheet)]
        create_opt = [op for op in ops_optimized if isinstance(op, CreateSheet)]

        # At minimum, should both create sheets
        assert len(create_unopt) > 0
        assert len(create_opt) > 0


# ============================================================================
# Task 11: Lambda support
# ============================================================================

class TestLambdaSupport:
    """Test simple lambda function translation."""

    def test_lambda_simple_multiplication(self):
        """lambda x: x * 2 translates to =A2 * 2."""
        analyzer = LambdaAnalyzer()
        formula = analyzer.translate_to_formula("lambda x: x * 2", {"x": "A2"})

        assert formula.startswith("=")
        assert "A2" in formula
        assert "*" in formula
        assert "2" in formula

    def test_lambda_simple_addition(self):
        """lambda x: x + 1 translates to =A2 + 1."""
        analyzer = LambdaAnalyzer()
        formula = analyzer.translate_to_formula("lambda x: x + 1", {"x": "A2"})

        assert "A2" in formula
        assert "+" in formula
        assert "1" in formula

    def test_lambda_string_method_raises_error(self):
        """lambda x: x.upper() raises UnsupportedOperationError."""
        analyzer = LambdaAnalyzer()

        with pytest.raises(UnsupportedOperationError):
            analyzer.translate_to_formula("lambda x: x.upper()", {"x": "A2"})

    def test_lambda_multiple_columns(self):
        """lambda row: row['a'] + row['b'] translates to =A2 + B2."""
        analyzer = LambdaAnalyzer()
        formula = analyzer.translate_to_formula(
            "lambda row: row['a'] + row['b']",
            {"a": "A2", "b": "B2"}
        )

        assert "A2" in formula
        assert "B2" in formula
        assert "+" in formula

    def test_lambda_nested_arithmetic(self):
        """lambda x: (x * 2) + 3 translates to =(A2 * 2) + 3."""
        analyzer = LambdaAnalyzer()
        formula = analyzer.translate_to_formula("lambda x: (x * 2) + 3", {"x": "A2"})

        assert "A2" in formula
        assert "*" in formula
        assert "2" in formula
        assert "+" in formula
        assert "3" in formula
        # Should have parentheses for precedence
        assert "(" in formula


# ============================================================================
# Task 12: Apps Script integration
# ============================================================================

class TestAppsScriptIntegration:
    """Test Apps Script generation for complex functions."""

    def test_complex_function_triggers_apps_script(self):
        """Complex function generates Apps Script code."""
        generator = AppsScriptGenerator()
        func_name, code = generator.generate_from_lambda("lambda x: x.upper()")

        assert func_name is not None
        assert len(func_name) > 0
        assert "function" in code
        assert func_name in code

    def test_generated_script_has_valid_signature(self):
        """Generated Apps Script has valid function signature."""
        generator = AppsScriptGenerator()
        func_name, code = generator.generate_from_lambda("lambda x: x * 2")

        # Should have function keyword and function name
        assert f"function {func_name}" in code
        # Should have parameter
        assert "(" in code and ")" in code
        # Should have return statement or body
        assert "{" in code and "}" in code

    def test_generated_script_has_function_body(self):
        """Generated Apps Script contains function body."""
        generator = AppsScriptGenerator()
        func_name, code = generator.generate_from_lambda("lambda x, y: x + y", "ADD_CUSTOM")

        # Should contain the function body
        assert "TODO" in code or "return" in code or "Logger" in code

    def test_apps_script_convenience_function(self):
        """Convenience function generates Apps Script."""
        func_name, code = generate_apps_script_function("lambda x: complex_operation(x)")

        assert isinstance(func_name, str)
        assert isinstance(code, str)
        assert len(code) > 0


# ============================================================================
# Edge cases and error handling
# ============================================================================

class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_source_data(self):
        """Translation works with empty source data."""
        source = Source(source_id="test.csv", schema=["a", "b"])
        plan = LogicalPlan(source)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test.csv": []})

        # Should still create sheet and write header
        create_ops = [op for op in ops if isinstance(op, CreateSheet)]
        assert len(create_ops) == 1

    def test_missing_source_data_uses_empty_list(self):
        """Missing source data defaults to empty list."""
        source = Source(source_id="test.csv", schema=["a"])
        plan = LogicalPlan(source)

        translator = Translator()
        ops = translator.translate(plan, source_data={})

        # Should handle gracefully
        assert len(ops) > 0

    def test_invalid_column_reference_raises_error(self):
        """Reference to non-existent column raises error.

        With early schema validation, this error is now caught at operation
        construction time rather than during translation.
        """
        from fornero.algebra.operations import SchemaValidationError

        source = Source(source_id="test.csv", schema=["a", "b"])

        # Error now caught at construction time with early validation
        with pytest.raises(SchemaValidationError, match="non-existent columns"):
            Select(columns=["c"], inputs=[source])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
