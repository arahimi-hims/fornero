"""
Local spreadsheet executor backed by formualizer.

Replays the spreadsheet operations produced by the translator into an
in-memory formualizer Workbook, evaluates all formulas locally, and
exposes the results via ``read_sheet()``.  No network access or Google
credentials are required.

Google Sheets-specific functions (FILTER, QUERY, ARRAY_CONSTRAIN, etc.)
are provided by custom Python callbacks registered in
``fornero.executor.gsheets_functions``.
"""

from __future__ import annotations

import math
import re
from typing import Any

import formualizer as fz

from fornero.executor.gsheets_functions import register_gsheets_functions
from fornero.executor.plan import ExecutionPlan
from fornero.spreadsheet.operations import (
    CreateSheet,
    NamedRange,
    SetFormula,
    SetValues,
    SpreadsheetOp,
)


class LocalExecutor:
    """In-process spreadsheet executor using formualizer.

    Usage::

        executor = LocalExecutor()
        plan = ExecutionPlan.from_operations(operations)
        executor.execute(plan, "Test Spreadsheet")
        actual = executor.read_sheet("Select_2")
    """

    def __init__(self) -> None:
        self.wb = fz.Workbook()
        self._sheet_dims: dict[str, tuple[int, int]] = {}
        register_gsheets_functions(self.wb)

    def execute(self, plan: ExecutionPlan, title: str) -> None:
        """Execute an execution plan, creating a local in-memory workbook.

        Args:
            plan: The execution plan containing ordered operations
            title: Title for the spreadsheet (not used in local execution,
                   but included for protocol compliance)

        Returns:
            None. The spreadsheet is maintained internally and accessed
            via read_sheet().
        """
        # Extract all operations from the plan steps
        operations: list[SpreadsheetOp] = []
        for step in plan.steps:
            operations.extend(step.operations)

        # Execute operations in order
        for op in operations:
            if isinstance(op, CreateSheet):
                self._execute_create_sheet(op)
            elif isinstance(op, SetValues):
                self._execute_set_values(op)
            elif isinstance(op, SetFormula):
                self._execute_set_formula(op)
            elif isinstance(op, NamedRange):
                pass

        self._materialise_all()

    def read_sheet(self, sheet_name: str) -> list[list[Any]]:
        """Evaluate and read back all cells for *sheet_name*.

        Returns a 2D list (rows x cols) with trailing all-empty rows
        trimmed.  Empty or ``None`` cells are normalised to ``""``.
        """
        rows, cols = self._sheet_dims[sheet_name]
        matrix: list[list[Any]] = []
        for r in range(1, rows + 1):
            row: list[Any] = []
            for c in range(1, cols + 1):
                val = self.wb.evaluate_cell(sheet_name, r, c)
                row.append(_normalize(val))
            matrix.append(row)
        return _trim_trailing_empty_rows(matrix)

    def _materialise_all(self) -> None:
        """Force evaluation of every cell so spilled values are visible.

        formualizer evaluates lazily — a formula that produces a spilled
        array only materialises the spilled cells when the anchor cell is
        evaluated.  By walking all sheets in creation order (which
        mirrors dependency order) and touching every cell, we ensure that
        downstream sheets see fully-materialised upstream data.
        """
        for sheet_name, (rows, cols) in self._sheet_dims.items():
            for r in range(1, rows + 1):
                for c in range(1, cols + 1):
                    self.wb.evaluate_cell(sheet_name, r, c)

    def _execute_create_sheet(self, op: CreateSheet) -> None:
        self.wb.add_sheet(op.name)
        self._sheet_dims[op.name] = (op.rows, op.cols)

    def _execute_set_values(self, op: SetValues) -> None:
        if not op.values:
            return
        sheet = self.wb.sheet(op.sheet)
        for ri, data_row in enumerate(op.values):
            for ci, val in enumerate(data_row):
                r = op.row + ri + 1  # formualizer is 1-indexed
                c = op.col + ci + 1
                sheet.set_value(r, c, _to_literal(val))

    def _execute_set_formula(self, op: SetFormula) -> None:
        formula = op.formula if op.formula.startswith("=") else f"={op.formula}"
        formula = _rewrite_array_literals(formula)
        r = op.row + 1
        c = op.col + 1
        self.wb.set_formula(op.sheet, r, c, formula)


_ARRAY_LITERAL_RE = re.compile(r"\{([^{}]+)\}")


def _rewrite_array_literals(formula: str) -> str:
    """Replace ``{a, b}`` / ``{a; b}`` array literals with HSTACK / VSTACK.

    formualizer supports ``{1,2,3}`` (inline values) but not
    ``{Sheet!R1, Sheet!R2}`` (range-based array literals).  The translator
    generates both forms, so we rewrite the range-based ones to function
    calls that formualizer *does* support.
    """

    def _replace(m: re.Match[str]) -> str:
        inner = m.group(1)
        if ";" in inner:
            parts = [p.strip() for p in inner.split(";")]
            return f"VSTACK({', '.join(parts)})"
        if "," in inner:
            parts = [p.strip() for p in inner.split(",")]
            if any("!" in p or re.search(r"[A-Z]+\d+", p) for p in parts):
                return f"HSTACK({', '.join(parts)})"
        return m.group(0)

    return _ARRAY_LITERAL_RE.sub(_replace, formula)


def _to_literal(value: Any) -> fz.LiteralValue:
    """Convert a Python value to a formualizer LiteralValue."""
    if value is None:
        return fz.LiteralValue.empty()
    if isinstance(value, bool):
        return fz.LiteralValue.boolean(value)
    if isinstance(value, int):
        return fz.LiteralValue.number(float(value))
    if isinstance(value, float):
        if math.isnan(value):
            return fz.LiteralValue.empty()
        return fz.LiteralValue.number(value)
    return fz.LiteralValue.text(str(value))


def _normalize(value: Any) -> Any:
    """Normalise a cell value returned by formualizer.

    * ``None`` → ``""``
    * Error dicts → ``""``
    * Float integers (e.g. ``3.0``) remain as float for comparison
    """
    if value is None:
        return ""
    if isinstance(value, dict):
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return value


def _trim_trailing_empty_rows(matrix: list[list[Any]]) -> list[list[Any]]:
    while matrix and all(cell == "" for cell in matrix[-1]):
        matrix.pop()
    return matrix
