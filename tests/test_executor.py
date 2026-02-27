"""
Unit tests for executor module (Tasks 13-15).

Task 13: ExecutionPlan tests
Task 14: SheetsClient wrapper tests
Task 15: SheetsExecutor tests

All tests mock gspread - no real API calls are made.
"""

from unittest.mock import Mock
import pytest
import gspread
from gspread.exceptions import APIError

from fornero.executor.sheets_client import SheetsClient
from fornero.executor.sheets_executor import SheetsExecutor
from fornero.executor.plan import ExecutionPlan, StepType
from fornero.spreadsheet.operations import (
    CreateSheet,
    SetValues,
    SetFormula,
    NamedRange,
)
from fornero.exceptions import SheetsAPIError, PlanValidationError


class TestSheetsClient:
    """Test suite for SheetsClient wrapper (Task 14)."""

    def test_init_stores_gc(self):
        """The wrapper stores the provided gspread client."""
        mock_gc = Mock(spec=gspread.Client)
        client = SheetsClient(mock_gc)
        assert client.gc is mock_gc

    def test_create_spreadsheet_success(self):
        """create_spreadsheet(title) calls gc.create(title) and returns the result."""
        mock_gc = Mock(spec=gspread.Client)
        mock_spreadsheet = Mock(spec=gspread.Spreadsheet)
        mock_gc.create.return_value = mock_spreadsheet

        client = SheetsClient(mock_gc)
        result = client.create_spreadsheet("Test Spreadsheet")

        mock_gc.create.assert_called_once_with("Test Spreadsheet")
        assert result == mock_spreadsheet

    def test_create_spreadsheet_api_error(self):
        """Error wrapping: APIError during create_spreadsheet is caught and re-raised as SheetsAPIError."""
        mock_gc = Mock(spec=gspread.Client)
        mock_response = Mock()
        mock_response.json.return_value = {
            "error": {
                "code": 429,
                "message": "Quota exceeded",
                "status": "RESOURCE_EXHAUSTED"
            }
        }
        api_error = APIError(mock_response)
        mock_gc.create.side_effect = api_error

        client = SheetsClient(mock_gc)

        with pytest.raises(SheetsAPIError) as exc_info:
            client.create_spreadsheet("Test Spreadsheet")

        assert "Failed to create spreadsheet" in str(exc_info.value)
        assert "Test Spreadsheet" in str(exc_info.value)
        assert "Quota exceeded" in str(exc_info.value)

    def test_add_sheet_with_defaults(self):
        """add_sheet(name, rows, cols) calls spreadsheet.add_worksheet(...) with correct arguments."""
        mock_gc = Mock(spec=gspread.Client)
        mock_spreadsheet = Mock(spec=gspread.Spreadsheet)
        mock_worksheet = Mock(spec=gspread.Worksheet)
        mock_spreadsheet.add_worksheet.return_value = mock_worksheet

        client = SheetsClient(mock_gc)
        result = client.add_sheet(mock_spreadsheet, "Sheet1")

        mock_spreadsheet.add_worksheet.assert_called_once_with(
            title="Sheet1",
            rows=1000,
            cols=26
        )
        assert result == mock_worksheet

    def test_add_sheet_with_custom_dimensions(self):
        """add_sheet accepts custom rows and cols parameters."""
        mock_gc = Mock(spec=gspread.Client)
        mock_spreadsheet = Mock(spec=gspread.Spreadsheet)
        mock_worksheet = Mock(spec=gspread.Worksheet)
        mock_spreadsheet.add_worksheet.return_value = mock_worksheet

        client = SheetsClient(mock_gc)
        result = client.add_sheet(mock_spreadsheet, "Sheet2", rows=500, cols=10)

        mock_spreadsheet.add_worksheet.assert_called_once_with(
            title="Sheet2",
            rows=500,
            cols=10
        )
        assert result == mock_worksheet

    def test_add_sheet_api_error(self):
        """Error wrapping: APIError during add_sheet is caught and re-raised as SheetsAPIError."""
        mock_gc = Mock(spec=gspread.Client)
        mock_spreadsheet = Mock(spec=gspread.Spreadsheet)
        mock_response = Mock()
        mock_response.json.return_value = {
            "error": {
                "code": 400,
                "message": "Sheet name already exists",
                "status": "INVALID_ARGUMENT"
            }
        }
        api_error = APIError(mock_response)
        mock_spreadsheet.add_worksheet.side_effect = api_error

        client = SheetsClient(mock_gc)

        with pytest.raises(SheetsAPIError) as exc_info:
            client.add_sheet(mock_spreadsheet, "DuplicateSheet")

        assert "Failed to add worksheet" in str(exc_info.value)
        assert "DuplicateSheet" in str(exc_info.value)
        assert "Sheet name already exists" in str(exc_info.value)

    def test_write_values_success(self):
        """write_values(sheet, range, values) calls worksheet.update(range, values)."""
        mock_gc = Mock(spec=gspread.Client)
        mock_worksheet = Mock(spec=gspread.Worksheet)

        client = SheetsClient(mock_gc)
        values = [["A", "B", "C"], [1, 2, 3], [4, 5, 6]]
        client.write_values(mock_worksheet, "A1:C3", values)

        mock_worksheet.update.assert_called_once_with(values, range_name="A1:C3")

    def test_write_values_api_error(self):
        """Error wrapping: APIError during write_values is caught and re-raised as SheetsAPIError."""
        mock_gc = Mock(spec=gspread.Client)
        mock_worksheet = Mock(spec=gspread.Worksheet)
        mock_response = Mock()
        mock_response.json.return_value = {
            "error": {
                "code": 400,
                "message": "Invalid range",
                "status": "INVALID_ARGUMENT"
            }
        }
        api_error = APIError(mock_response)
        mock_worksheet.update.side_effect = api_error

        client = SheetsClient(mock_gc)
        values = [["A", "B"], [1, 2]]

        with pytest.raises(SheetsAPIError) as exc_info:
            client.write_values(mock_worksheet, "Z999:ZZ999", values)

        assert "Failed to write values to range" in str(exc_info.value)
        assert "Z999:ZZ999" in str(exc_info.value)
        assert "Invalid range" in str(exc_info.value)

    def test_write_formula_success(self):
        """write_formula(sheet, cell, formula) calls worksheet.update(cell, formula, raw=False)."""
        mock_gc = Mock(spec=gspread.Client)
        mock_worksheet = Mock(spec=gspread.Worksheet)

        client = SheetsClient(mock_gc)
        formula = "=SUM(A1:A10)"
        client.write_formula(mock_worksheet, "B1", formula)

        mock_worksheet.update.assert_called_once_with([[formula]], range_name="B1", raw=False)

    def test_write_formula_api_error(self):
        """Error wrapping: APIError during write_formula is caught and re-raised as SheetsAPIError."""
        mock_gc = Mock(spec=gspread.Client)
        mock_worksheet = Mock(spec=gspread.Worksheet)
        mock_response = Mock()
        mock_response.json.return_value = {
            "error": {
                "code": 400,
                "message": "Formula syntax error",
                "status": "INVALID_ARGUMENT"
            }
        }
        api_error = APIError(mock_response)
        mock_worksheet.update.side_effect = api_error

        client = SheetsClient(mock_gc)

        with pytest.raises(SheetsAPIError) as exc_info:
            client.write_formula(mock_worksheet, "C5", "=INVALID()")

        assert "Failed to write formula to cell" in str(exc_info.value)
        assert "C5" in str(exc_info.value)
        assert "Formula syntax error" in str(exc_info.value)

    def test_batch_update_values_success(self):
        """batch_update_values calls worksheet.batch_update with correct format."""
        mock_gc = Mock(spec=gspread.Client)
        mock_worksheet = Mock(spec=gspread.Worksheet)

        client = SheetsClient(mock_gc)

        updates = [
            {'range': 'A1:B2', 'values': [[1, 2], [3, 4]]},
            {'range': 'D1:E1', 'values': [[5, 6]]},
        ]
        client.batch_update_values(mock_worksheet, updates)

        mock_worksheet.batch_update.assert_called_once()
        call_args = mock_worksheet.batch_update.call_args
        batch_data = call_args[0][0]

        assert len(batch_data) == 2
        assert batch_data[0] == {'range': 'A1:B2', 'values': [[1, 2], [3, 4]]}
        assert batch_data[1] == {'range': 'D1:E1', 'values': [[5, 6]]}

    def test_batch_update_values_empty_list(self):
        """batch_update_values handles empty list without calling API."""
        mock_gc = Mock(spec=gspread.Client)
        mock_worksheet = Mock(spec=gspread.Worksheet)

        client = SheetsClient(mock_gc)
        client.batch_update_values(mock_worksheet, [])

        mock_worksheet.batch_update.assert_not_called()

    def test_batch_update_formulas_success(self):
        """batch_update_formulas calls worksheet.batch_update with correct format and raw=False."""
        mock_gc = Mock(spec=gspread.Client)
        mock_worksheet = Mock(spec=gspread.Worksheet)

        client = SheetsClient(mock_gc)

        updates = [
            {'range': 'A1', 'values': [["=SUM(B1:B10)"]]},
            {'range': 'C3', 'values': [["=AVERAGE(D1:D5)"]]},
        ]
        client.batch_update_formulas(mock_worksheet, updates)

        mock_worksheet.batch_update.assert_called_once()
        call_args = mock_worksheet.batch_update.call_args
        batch_data = call_args[0][0]

        assert len(batch_data) == 2
        assert batch_data[0] == {'range': 'A1', 'values': [["=SUM(B1:B10)"]]}
        assert batch_data[1] == {'range': 'C3', 'values': [["=AVERAGE(D1:D5)"]]}
        assert call_args[1]["raw"] is False

    def test_batch_update_formulas_empty_list(self):
        """batch_update_formulas handles empty list without calling API."""
        mock_gc = Mock(spec=gspread.Client)
        mock_worksheet = Mock(spec=gspread.Worksheet)

        client = SheetsClient(mock_gc)
        client.batch_update_formulas(mock_worksheet, [])

        mock_worksheet.batch_update.assert_not_called()

    def test_batch_update_values_api_error(self):
        """Error wrapping: APIError during batch_update_values is caught and re-raised as SheetsAPIError."""
        mock_gc = Mock(spec=gspread.Client)
        mock_worksheet = Mock(spec=gspread.Worksheet)
        mock_response = Mock()
        mock_response.json.return_value = {
            "error": {
                "code": 400,
                "message": "Invalid range",
                "status": "INVALID_ARGUMENT"
            }
        }
        api_error = APIError(mock_response)
        mock_worksheet.batch_update.side_effect = api_error

        client = SheetsClient(mock_gc)

        with pytest.raises(SheetsAPIError) as exc_info:
            client.batch_update_values(mock_worksheet, [("A1:B2", [[1, 2]])])

        assert "Failed to batch update" in str(exc_info.value)
        assert "value ranges" in str(exc_info.value)
        assert "Invalid range" in str(exc_info.value)

    def test_error_wrapping_preserves_original_message(self):
        """Error wrapping: the original APIError message is preserved in SheetsAPIError."""
        mock_gc = Mock(spec=gspread.Client)
        original_message = "Rate limit exceeded: too many requests"
        mock_response = Mock()
        mock_response.json.return_value = {
            "error": {
                "code": 429,
                "message": original_message,
                "status": "RESOURCE_EXHAUSTED"
            }
        }
        api_error = APIError(mock_response)
        mock_gc.create.side_effect = api_error

        client = SheetsClient(mock_gc)

        with pytest.raises(SheetsAPIError) as exc_info:
            client.create_spreadsheet("Test")

        assert original_message in str(exc_info.value)

    def test_multiple_operations_in_sequence(self):
        """Integration: verify client can perform multiple operations in sequence."""
        mock_gc = Mock(spec=gspread.Client)
        mock_spreadsheet = Mock(spec=gspread.Spreadsheet)
        mock_worksheet = Mock(spec=gspread.Worksheet)
        mock_gc.create.return_value = mock_spreadsheet
        mock_spreadsheet.add_worksheet.return_value = mock_worksheet

        client = SheetsClient(mock_gc)

        spreadsheet = client.create_spreadsheet("Multi-op Test")
        assert spreadsheet == mock_spreadsheet

        worksheet = client.add_sheet(spreadsheet, "Data")
        assert worksheet == mock_worksheet

        client.write_values(worksheet, "A1:B2", [[1, 2], [3, 4]])
        client.write_formula(worksheet, "C1", "=A1+B1")

        mock_gc.create.assert_called_once()
        mock_spreadsheet.add_worksheet.assert_called_once()
        assert mock_worksheet.update.call_count == 2


class TestExecutionPlan:
    """Test suite for ExecutionPlan (Task 13)."""

    def test_empty_plan(self):
        """An empty operation list produces an empty plan (zero steps) without error."""
        plan = ExecutionPlan.from_operations([])
        assert len(plan.steps) == 0
        assert plan.main_sheet is None

    def test_single_source_plan(self):
        """A single-source plan (one CreateSheet, one SetValues, no formulas) produces two steps in the right order."""
        ops = [
            CreateSheet(name="source", rows=100, cols=5),
            SetValues(
                sheet="source",
                row=0,
                col=0,
                values=[["A", "B", "C"], [1, 2, 3], [4, 5, 6]]
            ),
        ]

        plan = ExecutionPlan.from_operations(ops)

        assert len(plan.steps) == 2
        assert plan.steps[0].step_type == StepType.CREATE_SHEETS
        assert plan.steps[1].step_type == StepType.WRITE_SOURCE_DATA

        assert len(plan.steps[0].operations) == 1
        assert isinstance(plan.steps[0].operations[0], CreateSheet)
        assert len(plan.steps[1].operations) == 1
        assert isinstance(plan.steps[1].operations[0], SetValues)

    def test_cross_sheet_formula_ordering(self):
        """A plan with cross-sheet formulas places WriteSourceData for the referenced sheet before WriteFormulas for the referencing sheet."""
        ops = [
            CreateSheet(name="source", rows=100, cols=3),
            CreateSheet(name="filtered", rows=100, cols=3),
            SetValues(
                sheet="source",
                row=0,
                col=0,
                values=[["A", "B", "C"], [1, 2, 3]]
            ),
            SetFormula(
                sheet="filtered",
                row=0,
                col=0,
                formula="=FILTER(source!A:C, source!A:A > 0)",
                ref="source"
            ),
        ]

        plan = ExecutionPlan.from_operations(ops)

        assert len(plan.steps) == 3
        assert plan.steps[0].step_type == StepType.CREATE_SHEETS
        assert plan.steps[1].step_type == StepType.WRITE_SOURCE_DATA
        assert plan.steps[2].step_type == StepType.WRITE_FORMULAS

        assert plan.steps[1].step_type == StepType.WRITE_SOURCE_DATA
        assert "source" in plan.steps[1].target_sheets

    def test_formula_referencing_nonexistent_sheet(self):
        """A plan with a formula referencing a nonexistent sheet raises PlanValidationError."""
        ops = [
            CreateSheet(name="sheet1", rows=100, cols=3),
            SetFormula(
                sheet="sheet1",
                row=0,
                col=0,
                formula="=SUM(nonexistent!A:A)",
                ref="nonexistent"
            ),
        ]

        with pytest.raises(PlanValidationError) as exc_info:
            ExecutionPlan.from_operations(ops)

        assert "nonexistent" in str(exc_info.value)

    def test_setvalues_nonexistent_sheet(self):
        """SetValues targeting a nonexistent sheet raises PlanValidationError."""
        ops = [
            CreateSheet(name="sheet1", rows=100, cols=3),
            SetValues(
                sheet="nonexistent",
                row=0,
                col=0,
                values=[[1, 2, 3]]
            ),
        ]

        with pytest.raises(PlanValidationError) as exc_info:
            ExecutionPlan.from_operations(ops)

        assert "nonexistent" in str(exc_info.value)

    def test_duplicate_sheet_names(self):
        """CreateSheet operations with duplicate sheet names raise PlanValidationError."""
        ops = [
            CreateSheet(name="sheet1", rows=100, cols=3),
            CreateSheet(name="sheet1", rows=50, cols=2),
        ]

        with pytest.raises(PlanValidationError) as exc_info:
            ExecutionPlan.from_operations(ops)

        assert "Duplicate sheet names" in str(exc_info.value)
        assert "sheet1" in str(exc_info.value)

    def test_named_ranges_after_formulas(self):
        """RegisterNamedRanges step always comes after all WriteFormulas steps."""
        ops = [
            CreateSheet(name="sheet1", rows=100, cols=3),
            SetValues(sheet="sheet1", row=0, col=0, values=[[1, 2, 3]]),
            SetFormula(sheet="sheet1", row=1, col=0, formula="=SUM(A1:C1)"),
            NamedRange(
                name="data_range",
                sheet="sheet1",
                row_start=0,
                col_start=0,
                row_end=10,
                col_end=2
            ),
        ]

        plan = ExecutionPlan.from_operations(ops)

        formula_idx = None
        named_range_idx = None
        for i, step in enumerate(plan.steps):
            if step.step_type == StepType.WRITE_FORMULAS:
                formula_idx = i
            elif step.step_type == StepType.REGISTER_NAMED_RANGES:
                named_range_idx = i

        assert named_range_idx is not None
        assert formula_idx is not None
        assert named_range_idx > formula_idx

    def test_explain_output(self):
        """explain() output includes sheet count, formula count, and step count."""
        ops = [
            CreateSheet(name="source", rows=100, cols=3),
            CreateSheet(name="result", rows=100, cols=3),
            SetValues(sheet="source", row=0, col=0, values=[["A", "B", "C"]]),
            SetFormula(sheet="result", row=0, col=0, formula="=source!A1"),
            SetFormula(sheet="result", row=0, col=1, formula="=source!B1"),
        ]

        plan = ExecutionPlan.from_operations(ops, main_sheet="result")
        explanation = plan.explain()

        assert "Sheets: 2" in explanation
        assert "Formula operations: 2" in explanation
        assert "Source data operations: 1" in explanation
        assert "Total execution steps:" in explanation
        assert "Main output sheet: result" in explanation

    def test_to_dict_round_trip(self):
        """to_dict() round-trips: ExecutionPlan.from_dict(plan.to_dict()) produces an equivalent plan."""
        ops = [
            CreateSheet(name="sheet1", rows=100, cols=3),
            SetValues(sheet="sheet1", row=0, col=0, values=[[1, 2, 3], [4, 5, 6]]),
            SetFormula(sheet="sheet1", row=2, col=0, formula="=SUM(A1:C2)"),
        ]

        original_plan = ExecutionPlan.from_operations(ops, main_sheet="sheet1")
        plan_dict = original_plan.to_dict()
        restored_plan = ExecutionPlan.from_dict(plan_dict)

        assert len(original_plan.steps) == len(restored_plan.steps)
        assert original_plan.main_sheet == restored_plan.main_sheet

        for orig_step, restored_step in zip(original_plan.steps, restored_plan.steps):
            assert orig_step.step_type == restored_step.step_type
            assert len(orig_step.operations) == len(restored_step.operations)
            assert orig_step.target_sheets == restored_step.target_sheets

    def test_topological_sort_multiple_dependencies(self):
        """WriteFormulas step respects topological order: if formula on sheet B references sheet A, sheet A's data step precedes sheet B's formula step."""
        ops = [
            CreateSheet(name="a", rows=10, cols=2),
            CreateSheet(name="b", rows=10, cols=2),
            CreateSheet(name="c", rows=10, cols=2),
            SetValues(sheet="a", row=0, col=0, values=[[1, 2]]),
            SetValues(sheet="b", row=0, col=0, values=[[3, 4]]),
            SetFormula(sheet="c", row=0, col=0, formula="=a!A1 + b!A1", ref="a"),
            SetFormula(sheet="c", row=0, col=1, formula="=a!B1 + b!B1", ref="b"),
        ]

        plan = ExecutionPlan.from_operations(ops)

        assert len(plan.steps) == 3

        source_idx = None
        formula_idx = None
        for i, step in enumerate(plan.steps):
            if step.step_type == StepType.WRITE_SOURCE_DATA:
                source_idx = i
            elif step.step_type == StepType.WRITE_FORMULAS:
                formula_idx = i

        assert source_idx is not None
        assert formula_idx is not None
        assert source_idx < formula_idx

    def test_main_sheet_tracker(self):
        """The main-sheet tracker correctly identifies the root output sheet."""
        ops = [
            CreateSheet(name="input", rows=10, cols=2),
            CreateSheet(name="output", rows=10, cols=2),
        ]

        plan = ExecutionPlan.from_operations(ops, main_sheet="output")
        assert plan.main_sheet == "output"

    def test_topological_sort_chain_dependencies(self):
        """Topological sort handles chain dependencies: A depends on B and C, B depends on C -> order is C, B, A."""
        ops = [
            CreateSheet(name="C", rows=10, cols=2),
            CreateSheet(name="B", rows=10, cols=2),
            CreateSheet(name="A", rows=10, cols=2),
            # All sheets have formulas to be included in topological sort
            # C has no dependencies (base case)
            SetFormula(sheet="C", row=0, col=0, formula="=1+1"),
            # B depends on C
            SetFormula(sheet="B", row=0, col=0, formula="=C!A1", ref="C"),
            # A depends on both B and C
            SetFormula(sheet="A", row=0, col=0, formula="=B!A1 + C!A1", ref="B"),
            SetFormula(sheet="A", row=0, col=1, formula="=C!A1", ref="C"),
        ]

        plan = ExecutionPlan.from_operations(ops)

        # Find the WriteFormulas step
        formula_step = None
        for step in plan.steps:
            if step.step_type == StepType.WRITE_FORMULAS:
                formula_step = step
                break

        assert formula_step is not None

        # Extract the order of sheets from the formulas
        sheet_order = []
        for formula_op in formula_step.operations:
            if formula_op.sheet not in sheet_order:
                sheet_order.append(formula_op.sheet)

        # Verify correct topological order: C before B, and both before A
        c_idx = sheet_order.index("C")
        b_idx = sheet_order.index("B")
        a_idx = sheet_order.index("A")

        assert c_idx < b_idx, "Sheet C (no dependencies) should come before Sheet B (depends on C)"
        assert c_idx < a_idx, "Sheet C should come before Sheet A (depends on C)"
        assert b_idx < a_idx, "Sheet B should come before Sheet A (depends on B)"

    def test_empty_explain(self):
        """explain() on empty plan returns appropriate message."""
        plan = ExecutionPlan.from_operations([])
        explanation = plan.explain()
        assert "Empty execution plan" in explanation


class TestSheetsExecutor:
    """Test suite for SheetsExecutor (Task 15)."""

    def _make_client(self):
        mock_gc = Mock(spec=gspread.Client)
        return SheetsClient(mock_gc), mock_gc

    def test_execute_creates_spreadsheet(self):
        """Executor creates a new spreadsheet with the specified title."""
        client, mock_gc = self._make_client()
        mock_spreadsheet = Mock(spec=gspread.Spreadsheet)
        mock_worksheet = Mock(spec=gspread.Worksheet)
        mock_worksheet.id = 0
        mock_worksheet.row_count = 1000
        mock_worksheet.col_count = 26

        mock_gc.create.return_value = mock_spreadsheet
        mock_spreadsheet.sheet1 = mock_worksheet

        executor = SheetsExecutor(client, rate_limit_delay=0)

        ops = [CreateSheet(name="Sheet1", rows=100, cols=5)]
        plan = ExecutionPlan.from_operations(ops)

        result = executor.execute(plan, "Test Spreadsheet")

        mock_gc.create.assert_called_once_with("Test Spreadsheet")
        assert result == mock_spreadsheet

    def test_execute_creates_multiple_sheets(self):
        """Executor creates multiple sheets in order."""
        client, mock_gc = self._make_client()
        mock_spreadsheet = Mock(spec=gspread.Spreadsheet)
        mock_worksheet1 = Mock(spec=gspread.Worksheet)
        mock_worksheet1.id = 0
        mock_worksheet1.row_count = 100
        mock_worksheet1.col_count = 5
        mock_worksheet2 = Mock(spec=gspread.Worksheet)
        mock_worksheet2.id = 1

        mock_gc.create.return_value = mock_spreadsheet
        mock_spreadsheet.sheet1 = mock_worksheet1
        mock_spreadsheet.add_worksheet.return_value = mock_worksheet2

        executor = SheetsExecutor(client, rate_limit_delay=0)

        ops = [
            CreateSheet(name="Sheet1", rows=100, cols=5),
            CreateSheet(name="Sheet2", rows=50, cols=3),
        ]
        plan = ExecutionPlan.from_operations(ops)

        executor.execute(plan, "Multi-Sheet Test")

        mock_worksheet1.update_title.assert_called_once_with("Sheet1")
        mock_spreadsheet.add_worksheet.assert_called_once()

    def test_execute_writes_values(self):
        """Executor writes values to the correct range."""
        client, mock_gc = self._make_client()
        mock_spreadsheet = Mock(spec=gspread.Spreadsheet)
        mock_worksheet = Mock(spec=gspread.Worksheet)
        mock_worksheet.id = 0
        mock_worksheet.row_count = 100
        mock_worksheet.col_count = 5

        mock_gc.create.return_value = mock_spreadsheet
        mock_spreadsheet.sheet1 = mock_worksheet

        executor = SheetsExecutor(client, rate_limit_delay=0)

        values = [["A", "B", "C"], [1, 2, 3], [4, 5, 6]]
        ops = [
            CreateSheet(name="Data", rows=100, cols=5),
            SetValues(sheet="Data", row=0, col=0, values=values),
        ]
        plan = ExecutionPlan.from_operations(ops)

        executor.execute(plan, "Values Test")

        # Verify batch_update was called (new batching implementation)
        mock_worksheet.batch_update.assert_called()
        call_args = mock_worksheet.batch_update.call_args
        # Should be called with list of dicts: [{'range': ..., 'values': ...}]
        batch_data = call_args[0][0]
        assert len(batch_data) == 1
        assert batch_data[0]['values'] == values

    def test_execute_writes_formulas(self):
        """Executor writes formulas to cells."""
        client, mock_gc = self._make_client()
        mock_spreadsheet = Mock(spec=gspread.Spreadsheet)
        mock_worksheet = Mock(spec=gspread.Worksheet)
        mock_worksheet.id = 0
        mock_worksheet.row_count = 100
        mock_worksheet.col_count = 5
        mock_worksheet.title = "Formulas"

        mock_gc.create.return_value = mock_spreadsheet
        mock_spreadsheet.sheet1 = mock_worksheet

        executor = SheetsExecutor(client, rate_limit_delay=0)

        ops = [
            CreateSheet(name="Formulas", rows=100, cols=5),
            SetFormula(sheet="Formulas", row=0, col=0, formula="=SUM(A2:A10)"),
        ]
        plan = ExecutionPlan.from_operations(ops)

        executor.execute(plan, "Formula Test")

        # Verify batch_update was called with raw=False (new batching implementation)
        mock_worksheet.batch_update.assert_called()
        call_args = mock_worksheet.batch_update.call_args
        # Should be called with list of dicts and raw=False
        batch_data = call_args[0][0]
        assert len(batch_data) == 1
        assert call_args[1]["raw"] is False

    def test_execute_registers_named_ranges(self):
        """Executor registers named ranges using batch_update."""
        client, mock_gc = self._make_client()
        mock_spreadsheet = Mock(spec=gspread.Spreadsheet)
        mock_worksheet = Mock(spec=gspread.Worksheet)
        mock_worksheet.id = 0
        mock_worksheet.row_count = 100
        mock_worksheet.col_count = 5

        mock_gc.create.return_value = mock_spreadsheet
        mock_spreadsheet.sheet1 = mock_worksheet

        executor = SheetsExecutor(client, rate_limit_delay=0)

        ops = [
            CreateSheet(name="Data", rows=100, cols=5),
            NamedRange(
                name="MyRange",
                sheet="Data",
                row_start=0,
                col_start=0,
                row_end=9,
                col_end=2
            ),
        ]
        plan = ExecutionPlan.from_operations(ops)

        executor.execute(plan, "Named Range Test")

        mock_spreadsheet.batch_update.assert_called_once()
        call_args = mock_spreadsheet.batch_update.call_args
        requests = call_args[0][0]["requests"]
        assert len(requests) == 1
        assert "addNamedRange" in requests[0]

    def test_retry_logic_on_api_error(self):
        """Executor retries operations that fail with APIError."""
        client, mock_gc = self._make_client()
        mock_spreadsheet = Mock(spec=gspread.Spreadsheet)
        mock_response = Mock()
        mock_response.json.return_value = {
            "error": {"code": 503, "message": "Service unavailable"}
        }
        api_error = APIError(mock_response)

        mock_gc.create.side_effect = [api_error, api_error, mock_spreadsheet]

        executor = SheetsExecutor(client, max_retries=3, base_delay=0.01, rate_limit_delay=0)

        ops = [CreateSheet(name="Sheet1", rows=100, cols=5)]
        plan = ExecutionPlan.from_operations(ops)

        mock_spreadsheet.sheet1 = Mock()
        mock_spreadsheet.sheet1.id = 0
        mock_spreadsheet.sheet1.row_count = 100
        mock_spreadsheet.sheet1.col_count = 5

        result = executor.execute(plan, "Retry Test")

        assert mock_gc.create.call_count == 3
        assert result == mock_spreadsheet

    def test_retry_exhaustion_raises_error(self):
        """Executor raises SheetsAPIError when retries are exhausted."""
        client, mock_gc = self._make_client()
        mock_response = Mock()
        mock_response.json.return_value = {
            "error": {"code": 503, "message": "Service unavailable"}
        }
        api_error = APIError(mock_response)
        mock_gc.create.side_effect = api_error

        executor = SheetsExecutor(client, max_retries=2, base_delay=0.01, rate_limit_delay=0)

        ops = [CreateSheet(name="Sheet1", rows=100, cols=5)]
        plan = ExecutionPlan.from_operations(ops)

        with pytest.raises(SheetsAPIError) as exc_info:
            executor.execute(plan, "Fail Test")

        assert "after 3 attempts" in str(exc_info.value)

    def test_dataset_size_validation(self):
        """Executor validates dataset size and rejects plans that exceed limits."""
        client, mock_gc = self._make_client()

        executor = SheetsExecutor(client, rate_limit_delay=0)

        ops = [
            CreateSheet(name="Huge", rows=10000, cols=2000),
        ]
        plan = ExecutionPlan.from_operations(ops)

        with pytest.raises(PlanValidationError) as exc_info:
            executor.execute(plan, "Too Large")

        assert "Dataset too large" in str(exc_info.value)

    def test_main_sheet_positioning(self):
        """Executor positions the main sheet as the first tab."""
        client, mock_gc = self._make_client()
        mock_spreadsheet = Mock(spec=gspread.Spreadsheet)
        mock_worksheet1 = Mock(spec=gspread.Worksheet)
        mock_worksheet1.id = 0
        mock_worksheet1.row_count = 100
        mock_worksheet1.col_count = 5
        mock_worksheet2 = Mock(spec=gspread.Worksheet)
        mock_worksheet2.id = 1

        mock_gc.create.return_value = mock_spreadsheet
        mock_spreadsheet.sheet1 = mock_worksheet1
        mock_spreadsheet.add_worksheet.return_value = mock_worksheet2

        executor = SheetsExecutor(client, rate_limit_delay=0)

        ops = [
            CreateSheet(name="Input", rows=100, cols=5),
            CreateSheet(name="Output", rows=100, cols=5),
        ]
        plan = ExecutionPlan.from_operations(ops, main_sheet="Output")

        executor.execute(plan, "Main Sheet Test")

        mock_worksheet2.update_index.assert_called_once_with(0)

    def test_batch_operations_per_sheet(self):
        """Executor groups operations by sheet for efficient batching."""
        client, mock_gc = self._make_client()
        mock_spreadsheet = Mock(spec=gspread.Spreadsheet)
        mock_worksheet = Mock(spec=gspread.Worksheet)
        mock_worksheet.id = 0
        mock_worksheet.row_count = 100
        mock_worksheet.col_count = 5

        mock_gc.create.return_value = mock_spreadsheet
        mock_spreadsheet.sheet1 = mock_worksheet

        executor = SheetsExecutor(client, rate_limit_delay=0)

        ops = [
            CreateSheet(name="Data", rows=100, cols=5),
            SetValues(sheet="Data", row=0, col=0, values=[[1, 2, 3]]),
            SetValues(sheet="Data", row=1, col=0, values=[[4, 5, 6]]),
        ]
        plan = ExecutionPlan.from_operations(ops)

        executor.execute(plan, "Batch Test")

        # With batching, both SetValues should be combined into one batch_update call
        assert mock_worksheet.batch_update.call_count == 1
        call_args = mock_worksheet.batch_update.call_args
        batch_data = call_args[0][0]
        # Should have 2 updates in the batch
        assert len(batch_data) == 2
