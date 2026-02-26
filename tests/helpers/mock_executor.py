"""
Mock executor for offline / CI testing.

Captures the spreadsheet operations produced by the translator without
hitting the Google Sheets API.  Provides inspection helpers so tests can
verify the structural correctness of the generated plan.  No cell grid is
maintained and no formulas are evaluated — this is a shape-level assertion
tool, not a functional spreadsheet simulator.
"""

from typing import Any, Dict, List, Optional, Set

from fornero.spreadsheet.operations import (
    SpreadsheetOp, CreateSheet, SetValues, SetFormula,
)


class MockExecutor:
    """Structural recorder for spreadsheet operations — no execution, no evaluation.

    Replays the operation list produced by ``Translator.translate()`` into
    internal dictionaries for inspection. Tests can verify that the right
    sheets, headers, and formula patterns were generated without hitting the
    Google Sheets API or simulating a spreadsheet engine.

    Fidelity notes:

    * **CreateSheet** — records sheet name and declared (rows, cols).
      No cell grid is allocated.
    * **SetValues** — appended to a per-sheet list; queryable by sheet or
      by header row.  Values are never written into a cell grid, so
      point lookups like "what is cell B3?" are not supported.
    * **SetFormula** — appended to a per-sheet list; queryable by
      substring or sheet.  Formulas are **never evaluated**; they are
      treated as opaque strings.
    * **NamedRange** — silently ignored (not dispatched in ``load``).

    Usage::

        executor = MockExecutor()
        executor.load(operations)

        assert executor.sheet_names == {"Source_0", "Filter_1"}
        assert executor.has_formula_containing("Filter_1", "FILTER")
    """

    def __init__(self) -> None:
        self.operations: List[SpreadsheetOp] = []
        self._sheets: Dict[str, Dict[str, Any]] = {}
        self._values: Dict[str, List[SetValues]] = {}
        self._formulas: Dict[str, List[SetFormula]] = {}

    def load(self, operations: List[SpreadsheetOp]) -> None:
        """Load a list of spreadsheet operations (as produced by ``Translator.translate``).

        Args:
            operations: List of SpreadsheetOp dataclass instances
        """
        self.operations = list(operations)
        self._sheets.clear()
        self._values.clear()
        self._formulas.clear()

        for op in self.operations:
            if isinstance(op, CreateSheet):
                self._sheets[op.name] = {"rows": op.rows, "cols": op.cols}
                self._values.setdefault(op.name, [])
                self._formulas.setdefault(op.name, [])
            elif isinstance(op, SetValues):
                self._values.setdefault(op.sheet, []).append(op)
            elif isinstance(op, SetFormula):
                self._formulas.setdefault(op.sheet, []).append(op)

    @property
    def sheet_names(self) -> Set[str]:
        return set(self._sheets.keys())

    @property
    def num_sheets(self) -> int:
        return len(self._sheets)

    def sheet_dims(self, name: str) -> tuple:
        """Return (rows, cols) for a sheet."""
        info = self._sheets[name]
        return info["rows"], info["cols"]

    def values_for(self, sheet: str) -> List[SetValues]:
        return self._values.get(sheet, [])

    def formulas_for(self, sheet: str) -> List[SetFormula]:
        return self._formulas.get(sheet, [])

    def all_formulas(self) -> List[SetFormula]:
        result: List[SetFormula] = []
        for ops in self._formulas.values():
            result.extend(ops)
        return result

    def headers_for(self, sheet: str) -> Optional[List[str]]:
        """Return the header row written via SetValues at row 0, or None."""
        for sv in self._values.get(sheet, []):
            if sv.row == 0 and sv.values:
                return list(sv.values[0])
        return None

    def has_formula_containing(self, sheet: str, substring: str) -> bool:
        """Check whether any formula on *sheet* contains *substring*."""
        for f in self._formulas.get(sheet, []):
            if substring in f.formula:
                return True
        return False

    def formula_strings(self, sheet: str) -> List[str]:
        """Return all formula expression strings for *sheet*."""
        return [f.formula for f in self._formulas.get(sheet, [])]

    def source_sheets(self) -> Set[str]:
        """Return sheet names that have SetValues data (row > 0) but no formulas."""
        result: Set[str] = set()
        for name in self._sheets:
            has_data_values = any(
                sv.row > 0 for sv in self._values.get(name, [])
            )
            has_formulas = bool(self._formulas.get(name))
            if has_data_values and not has_formulas:
                result.add(name)
        return result

    def derived_sheets(self) -> Set[str]:
        """Return sheet names that contain formulas (non-source sheets)."""
        return {name for name in self._sheets if self._formulas.get(name)}

    def last_sheet_name(self) -> Optional[str]:
        """Return the name of the last-created sheet (likely the final output)."""
        create_ops = [op for op in self.operations if isinstance(op, CreateSheet)]
        if create_ops:
            return create_ops[-1].name
        return None
