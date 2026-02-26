"""
End-to-end correctness tests for fornero.

Runs every program in tests/programs/, translates the resulting logical plan
to spreadsheet operations, and verifies correctness at three levels:

- Offline structural tests (always run)
- Local executor tests via formualizer (always run)
- Live Google Sheets tests (marked @pytest.mark.slow, skipped by default)

Usage:
    pytest tests/test_correctness.py -v                    # fast tests only
    pytest tests/test_correctness.py -v --run-slow         # all tests
    pytest tests/test_correctness.py -v -m slow            # slow tests only
    pytest tests/test_correctness.py -k "groupby_sum" -v   # single program
"""

import re
import time

import pytest

from fornero.translator import Translator
from fornero.exceptions import UnsupportedOperationError
from fornero.spreadsheet.operations import CreateSheet, SetValues, SetFormula
from fornero.executor.plan import ExecutionPlan

from tests.helpers.comparison import assert_matrix_equal, dataframe_to_matrix
from tests.helpers.mock_executor import MockExecutor
from tests.programs import discover


# ---------------------------------------------------------------------------
# Discovery & parametrisation
# ---------------------------------------------------------------------------

_PROGRAMS = discover()
_PROGRAM_IDS = [name for name, _ in _PROGRAMS]


def _run_program(mod):
    """Execute a program module and return (result_df, source_data, plan)."""
    pr = mod.run()
    plan = pr.result._plan
    return pr.result, pr.source_data, plan


def _expected_output_columns(root_op, result_df):
    """Derive the expected output columns from the plan root.

    For Join operations the translator drops the right key (k₂) per the formal
    semantics (ARCHITECTURE.md §Join: output schema is S(R₁) ∪ S(R₂) \\ {k₂}).
    Pandas keeps both key columns when left_on != right_on, so we reconcile here.
    """
    from fornero.algebra.operations import Join as JoinOp

    if isinstance(root_op, JoinOp):
        right_key = root_op.right_on[0] if isinstance(root_op.right_on, list) else root_op.right_on
        return [c for c in result_df.columns if c != right_key]

    return list(result_df.columns)


def _has_dynamic_headers(root_op):
    """True when the operation emits its own header row (QUERY, Pivot)."""
    from fornero.algebra.operations import GroupBy as GroupByOp, Pivot as PivotOp

    return isinstance(root_op, (GroupByOp, PivotOp))


# ---------------------------------------------------------------------------
# Live-execution helpers
# ---------------------------------------------------------------------------

_FORMULA_SETTLE_SECS = 5
_RATE_LIMIT_DELAY = 2.0


def _a1_cell(row: int, col: int) -> str:
    """Convert 1-indexed (row, col) to A1 notation (e.g. 2,3 -> 'C2')."""
    col_str = ""
    c = col
    while c > 0:
        c -= 1
        col_str = chr(65 + (c % 26)) + col_str
        c //= 26
    return f"{col_str}{row}"


def _execute_on_sheets(gc, operations, title):
    """Execute translator operations on a real Google Sheet.

    Mirrors the three-phase approach of SheetsExecutor (create → values →
    formulas) and works with the translator's SpreadsheetOp output.

    Returns:
        (spreadsheet, main_sheet_name)
    """
    spreadsheet = gc.create(title)
    worksheets = {}
    main_sheet_name = None
    first_sheet = True

    for op in operations:
        if not isinstance(op, CreateSheet):
            continue
        if first_sheet:
            ws = spreadsheet.sheet1
            ws.update_title(op.name)
            ws.resize(rows=op.rows, cols=op.cols)
            first_sheet = False
        else:
            ws = spreadsheet.add_worksheet(
                title=op.name, rows=op.rows, cols=op.cols
            )
        worksheets[op.name] = ws
        main_sheet_name = op.name
        time.sleep(_RATE_LIMIT_DELAY)

    for op in operations:
        if not isinstance(op, SetValues):
            continue
        ws = worksheets[op.sheet]
        if not op.values:
            continue
        r0 = op.row + 1
        c0 = op.col + 1
        r1 = r0 + len(op.values) - 1
        c1 = c0 + len(op.values[0]) - 1
        ws.update(values=op.values, range_name=f"{_a1_cell(r0, c0)}:{_a1_cell(r1, c1)}")
        time.sleep(_RATE_LIMIT_DELAY)

    for op in operations:
        if not isinstance(op, SetFormula):
            continue
        ws = worksheets[op.sheet]
        cell = _a1_cell(op.row + 1, op.col + 1)
        formula = op.formula if op.formula.startswith("=") else f"={op.formula}"
        ws.update(values=[[formula]], range_name=cell, raw=False)
        time.sleep(_RATE_LIMIT_DELAY)

    return spreadsheet, main_sheet_name


def _trim_empty_rows(matrix):
    """Remove trailing all-empty rows produced by over-allocated sheets."""
    while matrix and all(cell == "" for cell in matrix[-1]):
        matrix.pop()
    return matrix


# ---------------------------------------------------------------------------
# Live correctness tests (default — requires Google Sheets credentials)
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestLiveCorrectness:
    """Cell-by-cell verification: spreadsheet values must equal pandas output."""

    @pytest.fixture(scope="class")
    def gc(self):
        """Session-wide authenticated gspread client."""
        import gspread

        try:
            return gspread.service_account()
        except Exception:
            pass
        try:
            return gspread.oauth()
        except Exception as exc:
            pytest.skip(f"No Google Sheets credentials available: {exc}")

    @pytest.mark.parametrize("name, mod", _PROGRAMS, ids=_PROGRAM_IDS)
    def test_cell_values_match_pandas(self, name, mod, gc):
        """Translate, execute on Sheets, read back, and compare cell-by-cell."""
        if getattr(mod, "UNSUPPORTED", False):
            pytest.skip(f"{name} uses an unsupported operation")

        result, source_data, plan = _run_program(mod)
        translator = Translator()
        ops = translator.translate(plan, source_data=source_data)

        title = f"fornero_test_{name}"
        spreadsheet = None
        try:
            try:
                old = gc.open(title)
                gc.del_spreadsheet(old.id)
            except Exception:
                pass

            spreadsheet, main_sheet = _execute_on_sheets(gc, ops, title)

            time.sleep(_FORMULA_SETTLE_SECS)

            ws = spreadsheet.worksheet(main_sheet)
            actual = _trim_empty_rows(ws.get_all_values())

            expected_cols = _expected_output_columns(plan.root, result)
            expected_df = result[expected_cols]

            if _has_dynamic_headers(plan.root):
                # QUERY / Pivot emit their own headers which may differ from
                # the pandas column names.  Compare data rows only, sorted for
                # order-independence.
                expected_data = dataframe_to_matrix(expected_df, include_header=False)
                actual_data = actual[1:] if actual else []
                expected_data.sort(key=lambda r: tuple(str(v) for v in r))
                actual_data.sort(key=lambda r: tuple(str(v) for v in r))
                assert_matrix_equal(expected_data, actual_data)
            else:
                expected_matrix = dataframe_to_matrix(expected_df, include_header=True)
                assert_matrix_equal(expected_matrix, actual)

        finally:
            if spreadsheet:
                try:
                    gc.del_spreadsheet(spreadsheet.id)
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# Offline structural tests (no Google Sheets required)
# ---------------------------------------------------------------------------


class TestOfflineCorrectness:
    """Structural verification of translated spreadsheet operations."""

    @pytest.mark.parametrize("name, mod", _PROGRAMS, ids=_PROGRAM_IDS)
    def test_program_runs_and_produces_plan(self, name, mod):
        """Each program returns a DataFrame with a non-trivial logical plan."""
        result, source_data, plan = _run_program(mod)
        assert plan is not None
        assert plan.root is not None
        assert len(result) > 0, "Program produced an empty DataFrame"

    @pytest.mark.parametrize("name, mod", _PROGRAMS, ids=_PROGRAM_IDS)
    def test_plan_explains_without_error(self, name, mod):
        """plan.explain() should succeed for every program."""
        _, _, plan = _run_program(mod)
        explanation = plan.explain()
        assert isinstance(explanation, str)
        assert len(explanation) > 0

    @pytest.mark.parametrize("name, mod", _PROGRAMS, ids=_PROGRAM_IDS)
    def test_translation_produces_operations(self, name, mod):
        """Translator.translate() should produce a non-empty operation list
        (or raise UnsupportedOperationError for programs marked UNSUPPORTED)."""
        result, source_data, plan = _run_program(mod)

        if getattr(mod, "UNSUPPORTED", False):
            pytest.skip(f"{name} uses an unsupported operation")

        translator = Translator()
        ops = translator.translate(plan, source_data=source_data)
        assert len(ops) > 0, "Translator produced no operations"

    @pytest.mark.parametrize("name, mod", _PROGRAMS, ids=_PROGRAM_IDS)
    def test_operations_contain_create_sheet(self, name, mod):
        """Every translated plan must create at least one sheet."""
        if getattr(mod, "UNSUPPORTED", False):
            pytest.skip(f"{name} uses an unsupported operation")

        _, source_data, plan = _run_program(mod)
        translator = Translator()
        ops = translator.translate(plan, source_data=source_data)

        executor = MockExecutor()
        executor.load(ops)
        assert executor.num_sheets >= 1

    @pytest.mark.parametrize("name, mod", _PROGRAMS, ids=_PROGRAM_IDS)
    def test_source_sheets_contain_static_data(self, name, mod):
        """Source nodes must produce sheets with SetValues (static data)."""
        if getattr(mod, "UNSUPPORTED", False):
            pytest.skip(f"{name} uses an unsupported operation")

        _, source_data, plan = _run_program(mod)
        translator = Translator()
        ops = translator.translate(plan, source_data=source_data)

        executor = MockExecutor()
        executor.load(ops)

        src_sheets = executor.source_sheets()
        assert len(src_sheets) >= 1, "No source sheets found"
        for sheet_name in src_sheets:
            values = executor.values_for(sheet_name)
            assert len(values) >= 2, (
                f"Source sheet '{sheet_name}' should have header + data SetValues"
            )

    @pytest.mark.parametrize("name, mod", _PROGRAMS, ids=_PROGRAM_IDS)
    def test_derived_sheets_use_formulas_not_values(self, name, mod):
        """Non-source sheets must use SetFormula for data, never SetValues
        (except for headers at row 0)."""
        if getattr(mod, "UNSUPPORTED", False):
            pytest.skip(f"{name} uses an unsupported operation")

        _, source_data, plan = _run_program(mod)
        translator = Translator()
        ops = translator.translate(plan, source_data=source_data)

        executor = MockExecutor()
        executor.load(ops)

        for sheet_name in executor.derived_sheets():
            for sv in executor.values_for(sheet_name):
                assert sv.row == 0, (
                    f"Derived sheet '{sheet_name}' has SetValues at row {sv.row} "
                    "(only row-0 headers are permitted; data must use SetFormula)"
                )

    @pytest.mark.parametrize("name, mod", _PROGRAMS, ids=_PROGRAM_IDS)
    def test_headers_match_output_schema(self, name, mod):
        """The last-created sheet's headers should match the result DataFrame columns."""
        if getattr(mod, "UNSUPPORTED", False):
            pytest.skip(f"{name} uses an unsupported operation")

        result, source_data, plan = _run_program(mod)

        if _has_dynamic_headers(plan.root):
            pytest.skip("GroupBy/QUERY and Pivot emit dynamic headers")

        translator = Translator()
        ops = translator.translate(plan, source_data=source_data)

        executor = MockExecutor()
        executor.load(ops)

        output_sheet = executor.last_sheet_name()
        assert output_sheet is not None

        headers = executor.headers_for(output_sheet)
        if headers is not None:
            expected_columns = _expected_output_columns(plan.root, result)
            assert headers == expected_columns, (
                f"Header mismatch on sheet '{output_sheet}': "
                f"expected {expected_columns}, got {headers}"
            )


class TestOfflineFormulaPatterns:
    """Verify that specific operations produce the expected formula functions."""

    def _translate(self, mod):
        result, source_data, plan = _run_program(mod)
        translator = Translator()
        ops = translator.translate(plan, source_data=source_data)
        executor = MockExecutor()
        executor.load(ops)
        return executor, result

    def test_filter_uses_filter_formula(self):
        from tests.programs import p03_filter_rows as mod
        executor, _ = self._translate(mod)
        derived = executor.derived_sheets()
        assert any(
            executor.has_formula_containing(s, "FILTER")
            for s in derived
        ), "Filter program should produce a FILTER() formula"

    def test_sort_uses_sort_formula(self):
        from tests.programs import p04_sort_single as mod
        executor, _ = self._translate(mod)
        derived = executor.derived_sheets()
        assert any(
            executor.has_formula_containing(s, "SORT")
            for s in derived
        ), "Sort program should produce a SORT() formula"

    def test_groupby_uses_query_formula(self):
        from tests.programs import p12_groupby_sum as mod
        executor, _ = self._translate(mod)
        derived = executor.derived_sheets()
        assert any(
            executor.has_formula_containing(s, "QUERY")
            for s in derived
        ), "GroupBy program should produce a QUERY() formula"

    def test_join_uses_xlookup_formula(self):
        from tests.programs import p10_inner_join as mod
        executor, _ = self._translate(mod)
        derived = executor.derived_sheets()
        assert any(
            executor.has_formula_containing(s, "XLOOKUP")
            for s in derived
        ), "Join program should produce XLOOKUP() formulas"

    def test_limit_uses_array_constrain(self):
        from tests.programs import p06_head_limit as mod
        executor, _ = self._translate(mod)
        derived = executor.derived_sheets()
        assert any(
            executor.has_formula_containing(s, "ARRAY_CONSTRAIN")
            for s in derived
        ), "Limit/head program should produce ARRAY_CONSTRAIN() formula"

    def test_union_uses_vertical_stack(self):
        from tests.programs import p16_union_vertical as mod
        executor, _ = self._translate(mod)
        derived = executor.derived_sheets()
        assert any(
            executor.has_formula_containing(s, ";")
            for s in derived
        ), "Union program should produce a vertical-stack formula with ';'"


class TestOfflineMultiStep:
    """Verify multi-operation pipelines produce multiple sheets."""

    def _translate(self, mod):
        result, source_data, plan = _run_program(mod)
        translator = Translator()
        ops = translator.translate(plan, source_data=source_data)
        executor = MockExecutor()
        executor.load(ops)
        return executor

    def test_filter_then_select_has_multiple_sheets(self):
        from tests.programs import p08_filter_then_select as mod
        executor = self._translate(mod)
        assert executor.num_sheets >= 3, (
            "Filter→Select pipeline needs at least source + filter + select sheets"
        )

    def test_multi_step_pipeline_has_many_sheets(self):
        from tests.programs import p18_multi_step_pipeline as mod
        executor = self._translate(mod)
        assert executor.num_sheets >= 4, (
            "Filter→WithColumn→Sort→Select pipeline needs at least 5 sheets"
        )

    def test_join_groupby_has_multiple_sheets(self):
        from tests.programs import p15_join_groupby as mod
        executor = self._translate(mod)
        assert executor.num_sheets >= 4, (
            "Join→GroupBy pipeline needs at least source1 + source2 + join + groupby"
        )

    def test_cross_sheet_references_are_consistent(self):
        """Formulas referencing other sheets must only reference sheets that exist."""
        from tests.programs import p09_select_then_sort as mod
        _, source_data, plan = _run_program(mod)
        translator = Translator()
        ops = translator.translate(plan, source_data=source_data)

        executor = MockExecutor()
        executor.load(ops)
        known_sheets = executor.sheet_names

        for formula_op in executor.all_formulas():
            formula_str = formula_op.formula
            refs = re.findall(r"(\w+_\d+)!", formula_str)
            for ref in refs:
                assert ref in known_sheets, (
                    f"Formula references unknown sheet '{ref}': {formula_str}"
                )


class TestLocalCorrectness:
    """Cell-by-cell verification using the local (formualizer) executor.

    Translates each program, replays it through ``LocalExecutor``, reads back
    the main sheet, and asserts every cell matches the pandas output.  No
    network access is required.
    """

    @pytest.mark.parametrize("name, mod", _PROGRAMS, ids=_PROGRAM_IDS)
    def test_cell_values_match_pandas(self, name, mod):
        from fornero.executor import LocalExecutor
        from fornero.algebra.operations import Pivot

        if getattr(mod, "UNSUPPORTED", False):
            pytest.skip(f"{name} uses an unsupported operation")

        result, source_data, plan = _run_program(mod)

        translator = Translator()
        ops = translator.translate(plan, source_data=source_data)

        executor = LocalExecutor()
        execution_plan = ExecutionPlan.from_operations(ops)
        executor.execute(execution_plan, "Test Spreadsheet")

        main_sheet = [o for o in ops if isinstance(o, CreateSheet)][-1].name
        actual = executor.read_sheet(main_sheet)

        expected_cols = _expected_output_columns(plan.root, result)
        expected_df = result[expected_cols]

        if _has_dynamic_headers(plan.root):
            expected_data = dataframe_to_matrix(expected_df, include_header=False)
            actual_data = actual[1:] if actual else []
            expected_data.sort(key=lambda r: tuple(str(v) for v in r))
            actual_data.sort(key=lambda r: tuple(str(v) for v in r))
            assert_matrix_equal(expected_data, actual_data)
        else:
            expected_matrix = dataframe_to_matrix(expected_df, include_header=True)
            assert_matrix_equal(expected_matrix, actual)


class TestUnsupportedOperations:
    """Programs using unsupported operations should raise UnsupportedOperationError."""

    def test_pivot_produces_two_sheet_strategy(self):
        """Pivot translation should produce a helper sheet and output sheet."""
        from fornero.algebra.operations import Pivot, Source
        source = Source(source_id="test", schema=["dept", "quarter", "revenue"])
        pivot = Pivot(
            index="dept",
            columns="quarter",
            values="revenue",
            inputs=[source],
        )
        from fornero.algebra import LogicalPlan
        plan = LogicalPlan(pivot)

        translator = Translator()
        ops = translator.translate(plan, source_data={"test": [[1, 2, 3]]})
        assert len(ops) > 0
        create_ops = [o for o in ops if isinstance(o, CreateSheet)]
        assert len(create_ops) >= 3
        formula_ops = [o for o in ops if isinstance(o, SetFormula)]
        formulas = [o.formula for o in formula_ops]
        assert any('TRANSPOSE' in f and 'UNIQUE' in f for f in formulas)
        assert any('IFERROR' in f and 'FILTER' in f for f in formulas)
