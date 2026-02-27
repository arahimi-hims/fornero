"""
Google Sheets executor with batch operations and retry logic.

This module provides the SheetsExecutor class, which executes an ExecutionPlan
against the Google Sheets API. It handles:
- Batch operations to minimize API calls
- Rate limiting to stay within quota
- Retry logic with exponential backoff for transient failures
- Dataset size validation
"""

import time
from typing import Any, Dict, List

import gspread
from gspread.exceptions import APIError

from fornero.exceptions import PlanValidationError, SheetsAPIError
from fornero.executor.plan import ExecutionPlan, ExecutionStep, StepType
from fornero.executor.sheets_client import SheetsClient
from fornero.spreadsheet.operations import (
    CreateSheet,
    SetValues,
    SetFormula,
    NamedRange,
)


# Google Sheets limits
MAX_CELLS = 10_000_000  # 10 million cells per spreadsheet
MAX_FORMULA_CELLS = 5_000_000  # ~5 million formula cells (conservative estimate)


class SheetsExecutor:
    """Executes execution plans against Google Sheets API.

    This executor owns all communication with Google Sheets, providing:
    - Batch operations where possible
    - Rate limiting and retry logic
    - Dataset size validation
    - Post-operation validation

    Attributes:
        client: SheetsClient wrapper for API calls
        max_retries: Maximum number of retry attempts for transient failures
        base_delay: Base delay in seconds for exponential backoff
        rate_limit_delay: Delay between batch operations to avoid rate limits
    """

    def __init__(
        self,
        client: SheetsClient,
        max_retries: int = 3,
        base_delay: float = 1.0,
        rate_limit_delay: float = 0.5,
    ):
        """Initialize the executor.

        Args:
            client: Authenticated SheetsClient
            max_retries: Maximum retry attempts (default: 3)
            base_delay: Base delay for exponential backoff in seconds (default: 1.0)
            rate_limit_delay: Delay between batch operations in seconds (default: 0.5)
        """
        self.client = client
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.rate_limit_delay = rate_limit_delay

    def execute(self, plan: ExecutionPlan, title: str) -> gspread.Spreadsheet:
        """Execute an execution plan, creating a new spreadsheet.

        Args:
            plan: The execution plan to execute
            title: Title for the new spreadsheet

        Returns:
            The created Spreadsheet object

        Raises:
            PlanValidationError: If the plan is invalid or dataset is too large
            SheetsAPIError: If API calls fail after retries
        """
        # Validate plan before execution
        self._validate_plan_size(plan)

        # Create the spreadsheet
        spreadsheet = self._retry_operation(
            lambda: self.client.create_spreadsheet(title),
            f"create spreadsheet '{title}'"
        )

        # Track created worksheets for quick lookup
        worksheets: Dict[str, gspread.Worksheet] = {}

        # Execute steps in order
        for step in plan.steps:
            if step.step_type == StepType.CREATE_SHEETS:
                self._execute_create_sheets(spreadsheet, step, worksheets)
            elif step.step_type == StepType.WRITE_SOURCE_DATA:
                self._execute_write_source_data(step, worksheets)
            elif step.step_type == StepType.WRITE_FORMULAS:
                self._execute_write_formulas(step, worksheets)
            elif step.step_type == StepType.REGISTER_NAMED_RANGES:
                self._execute_register_named_ranges(spreadsheet, step, worksheets)

            # Rate limiting: pause between steps
            if self.rate_limit_delay > 0:
                time.sleep(self.rate_limit_delay)

        # Position main sheet as first tab if specified
        if plan.main_sheet and plan.main_sheet in worksheets:
            main_ws = worksheets[plan.main_sheet]
            self._retry_operation(
                lambda: main_ws.update_index(0),
                f"reorder main sheet '{plan.main_sheet}'"
            )

        return spreadsheet

    def _validate_plan_size(self, plan: ExecutionPlan) -> None:
        """Validate that the plan fits within Google Sheets limits.

        Args:
            plan: The execution plan to validate

        Raises:
            PlanValidationError: If dataset is too large
        """
        total_cells = 0
        total_formula_cells = 0

        for step in plan.steps:
            if step.step_type == StepType.CREATE_SHEETS:
                for op in step.operations:
                    if isinstance(op, CreateSheet):
                        total_cells += op.rows * op.cols

            elif step.step_type == StepType.WRITE_SOURCE_DATA:
                for op in step.operations:
                    if isinstance(op, SetValues):
                        num_rows = len(op.values)
                        num_cols = len(op.values[0]) if op.values else 0
                        total_cells += num_rows * num_cols

            elif step.step_type == StepType.WRITE_FORMULAS:
                for op in step.operations:
                    if isinstance(op, SetFormula):
                        total_formula_cells += 1

        if total_cells > MAX_CELLS:
            raise PlanValidationError(
                f"Dataset too large for Google Sheets: {total_cells:,} cells "
                f"(limit: {MAX_CELLS:,}). Please reduce the data size."
            )

        if total_formula_cells > MAX_FORMULA_CELLS:
            raise PlanValidationError(
                f"Too many formulas for Google Sheets: {total_formula_cells:,} "
                f"(limit: ~{MAX_FORMULA_CELLS:,}). Please reduce the complexity."
            )

    def _execute_create_sheets(
        self,
        spreadsheet: gspread.Spreadsheet,
        step: ExecutionStep,
        worksheets: Dict[str, gspread.Worksheet]
    ) -> None:
        """Execute CreateSheet operations.

        Args:
            spreadsheet: The target spreadsheet
            step: Execution step containing CreateSheet operations
            worksheets: Dictionary to populate with created worksheets
        """
        # Get the default sheet created with the spreadsheet
        default_sheet = spreadsheet.sheet1

        for i, op in enumerate(step.operations):
            if not isinstance(op, CreateSheet):
                continue

            if i == 0:
                # Reuse the default sheet for the first CreateSheet operation
                worksheet = self._retry_operation(
                    lambda: spreadsheet.sheet1,
                    f"get default sheet"
                )
                # Rename it
                self._retry_operation(
                    lambda: worksheet.update_title(op.name),
                    f"rename default sheet to '{op.name}'"
                )
                # Resize if needed
                if worksheet.row_count != op.rows or worksheet.col_count != op.cols:
                    self._retry_operation(
                        lambda: worksheet.resize(rows=op.rows, cols=op.cols),
                        f"resize sheet '{op.name}'"
                    )
            else:
                # Create a new sheet
                worksheet = self._retry_operation(
                    lambda: self.client.add_sheet(
                        spreadsheet,
                        op.name,
                        rows=op.rows,
                        cols=op.cols
                    ),
                    f"create sheet '{op.name}'"
                )

            worksheets[op.name] = worksheet

    def _execute_write_source_data(
        self,
        step: ExecutionStep,
        worksheets: Dict[str, gspread.Worksheet]
    ) -> None:
        """Execute SetValues operations.

        Groups operations by sheet and batches them into single API calls.

        Args:
            step: Execution step containing SetValues operations
            worksheets: Dictionary of available worksheets
        """
        # Group operations by sheet
        ops_by_sheet: Dict[str, List[SetValues]] = {}
        for op in step.operations:
            if not isinstance(op, SetValues):
                continue
            if op.sheet not in ops_by_sheet:
                ops_by_sheet[op.sheet] = []
            ops_by_sheet[op.sheet].append(op)

        # Execute operations for each sheet using batch updates
        for sheet_name, ops in ops_by_sheet.items():
            worksheet = worksheets.get(sheet_name)
            if not worksheet:
                raise PlanValidationError(
                    f"Cannot write values: sheet '{sheet_name}' not found"
                )

            # Prepare batch updates
            batch_updates = []
            for op in ops:
                if not op.values:
                    continue  # Skip empty operations

                # Convert 0-indexed to 1-indexed (A1 notation)
                start_row = op.row + 1
                start_col = op.col + 1
                num_rows = len(op.values)
                num_cols = len(op.values[0]) if op.values else 0

                # Build A1 notation range
                end_row = start_row + num_rows - 1
                end_col = start_col + num_cols - 1
                range_str = self._build_a1_range(start_row, start_col, end_row, end_col)

                batch_updates.append({
                    'range': range_str,
                    'values': op.values
                })

            # Execute batch update with retry
            if batch_updates:
                self._retry_operation(
                    lambda: self.client.batch_update_values(worksheet, batch_updates),
                    f"batch update {len(batch_updates)} value ranges to {worksheet.title}"
                )

    def _execute_write_formulas(
        self,
        step: ExecutionStep,
        worksheets: Dict[str, gspread.Worksheet]
    ) -> None:
        """Execute SetFormula operations.

        Formulas are already sorted in topological order by the plan.
        Groups operations by sheet and batches them into single API calls.

        Args:
            step: Execution step containing SetFormula operations
            worksheets: Dictionary of available worksheets
        """
        # Group operations by sheet while preserving topological order
        ops_by_sheet: Dict[str, List[SetFormula]] = {}
        for op in step.operations:
            if not isinstance(op, SetFormula):
                continue

            if op.sheet not in ops_by_sheet:
                ops_by_sheet[op.sheet] = []
            ops_by_sheet[op.sheet].append(op)

        # Execute operations for each sheet using batch updates
        for sheet_name, ops in ops_by_sheet.items():
            worksheet = worksheets.get(sheet_name)
            if not worksheet:
                raise PlanValidationError(
                    f"Cannot write formula: sheet '{sheet_name}' not found"
                )

            # Prepare batch updates
            batch_updates = []
            for op in ops:
                # Convert 0-indexed to 1-indexed (A1 notation)
                row = op.row + 1
                col = op.col + 1
                cell = self._build_a1_cell(row, col)

                # Ensure formula starts with '='
                formula = op.formula if op.formula.startswith("=") else f"={op.formula}"

                batch_updates.append({
                    'range': cell,
                    'values': [[formula]]
                })

            # Execute batch update with retry
            if batch_updates:
                self._retry_operation(
                    lambda: self.client.batch_update_formulas(worksheet, batch_updates),
                    f"batch update {len(batch_updates)} formulas to {worksheet.title}"
                )

    def _execute_register_named_ranges(
        self,
        spreadsheet: gspread.Spreadsheet,
        step: ExecutionStep,
        worksheets: Dict[str, gspread.Worksheet]
    ) -> None:
        """Execute NamedRange operations.

        Named ranges are registered using the batch_update API.

        Args:
            spreadsheet: The target spreadsheet
            step: Execution step containing NamedRange operations
            worksheets: Dictionary of available worksheets
        """
        # Build batch request for all named ranges
        requests = []

        for op in step.operations:
            if not isinstance(op, NamedRange):
                continue

            worksheet = worksheets.get(op.sheet)
            if not worksheet:
                raise PlanValidationError(
                    f"Cannot create named range: sheet '{op.sheet}' not found"
                )

            # Convert 0-indexed to 1-indexed
            start_row = op.row_start
            start_col = op.col_start
            end_row = op.row_end + 1  # end is exclusive in API
            end_col = op.col_end + 1  # end is exclusive in API

            # Build named range request
            request = {
                "addNamedRange": {
                    "namedRange": {
                        "name": op.name,
                        "range": {
                            "sheetId": worksheet.id,
                            "startRowIndex": start_row,
                            "endRowIndex": end_row,
                            "startColumnIndex": start_col,
                            "endColumnIndex": end_col,
                        }
                    }
                }
            }
            requests.append(request)

        # Execute batch request if there are any named ranges
        if requests:
            self._retry_operation(
                lambda: spreadsheet.batch_update({"requests": requests}),
                f"register {len(requests)} named range(s)"
            )

    def _retry_operation(
        self,
        operation: Any,
        description: str
    ) -> Any:
        """Execute an operation with retry logic and exponential backoff.

        Args:
            operation: Callable that performs the operation
            description: Human-readable description for error messages

        Returns:
            Result of the operation

        Raises:
            SheetsAPIError: If operation fails after all retries
        """
        last_error = None

        for attempt in range(self.max_retries + 1):
            try:
                return operation()
            except (APIError, SheetsAPIError) as e:
                last_error = e

                # Retry on transient errors
                if attempt < self.max_retries:
                    # Calculate exponential backoff delay
                    delay = self.base_delay * (2 ** attempt)
                    time.sleep(delay)
                    continue
                else:
                    # Final attempt failed
                    break

        # All retries exhausted
        raise SheetsAPIError(
            f"Failed to {description} after {self.max_retries + 1} attempts: {last_error}"
        )

    @staticmethod
    def _build_a1_range(start_row: int, start_col: int, end_row: int, end_col: int) -> str:
        """Build A1 notation range string.

        Args:
            start_row: Starting row (1-indexed)
            start_col: Starting column (1-indexed)
            end_row: Ending row (1-indexed)
            end_col: Ending column (1-indexed)

        Returns:
            A1 notation string (e.g., "A1:C10")
        """
        start_cell = SheetsExecutor._build_a1_cell(start_row, start_col)
        end_cell = SheetsExecutor._build_a1_cell(end_row, end_col)
        return f"{start_cell}:{end_cell}"

    @staticmethod
    def _build_a1_cell(row: int, col: int) -> str:
        """Build A1 notation cell reference.

        Args:
            row: Row number (1-indexed)
            col: Column number (1-indexed)

        Returns:
            A1 notation string (e.g., "B5")
        """
        col_str = ""
        while col > 0:
            col -= 1
            col_str = chr(65 + (col % 26)) + col_str
            col //= 26
        return f"{col_str}{row}"
