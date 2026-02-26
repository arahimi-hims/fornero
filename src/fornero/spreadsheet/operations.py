"""
Spreadsheet algebra operation classes.

This module defines the core operations that can be performed on a spreadsheet:
- CreateSheet: Create a new sheet with specified dimensions
- SetValues: Write static values to a cell range
- SetFormula: Install a formula in a cell
- NamedRange: Register a named range for use in formulas

These operations form the target language for the translator. They represent
pure state transitions on a workbook and are executed by the SheetsExecutor.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union


@dataclass
class CreateSheet:
    """Create a new sheet in the workbook.

    Attributes:
        name: The sheet name (must be unique within the workbook)
        rows: Number of rows in the new sheet
        cols: Number of columns in the new sheet
    """
    name: str
    rows: int
    cols: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "type": "CreateSheet",
            "name": self.name,
            "rows": self.rows,
            "cols": self.cols,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CreateSheet":
        """Create from dictionary representation."""
        return cls(
            name=data["name"],
            rows=data["rows"],
            cols=data["cols"],
        )


@dataclass
class SetValues:
    """Write static values to a rectangular cell region.

    Attributes:
        sheet: The target sheet name
        row: Starting row (0-indexed)
        col: Starting column (0-indexed)
        values: 2D list of cell values (rows × columns)
    """
    sheet: str
    row: int
    col: int
    values: List[List[Any]]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "type": "SetValues",
            "sheet": self.sheet,
            "row": self.row,
            "col": self.col,
            "values": self.values,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SetValues":
        """Create from dictionary representation."""
        return cls(
            sheet=data["sheet"],
            row=data["row"],
            col=data["col"],
            values=data["values"],
        )


@dataclass
class SetFormula:
    """Install a formula in a specific cell.

    The formula may reference other cells, ranges, or named ranges. Cross-sheet
    references are expressed using Sheet!Range notation.

    Attributes:
        sheet: The target sheet name
        row: Target row (0-indexed)
        col: Target column (0-indexed)
        formula: The formula expression (with or without leading '=')
        ref: Optional sheet reference that this formula depends on (for dependency tracking)
    """
    sheet: str
    row: int
    col: int
    formula: str
    ref: Optional[str] = None  # Referenced sheet name for dependency tracking

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "type": "SetFormula",
            "sheet": self.sheet,
            "row": self.row,
            "col": self.col,
            "formula": self.formula,
            "ref": self.ref,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SetFormula":
        """Create from dictionary representation."""
        return cls(
            sheet=data["sheet"],
            row=data["row"],
            col=data["col"],
            formula=data["formula"],
            ref=data.get("ref"),
        )


@dataclass
class NamedRange:
    """Register a named range for use in formulas.

    Named ranges provide symbolic names for cell ranges, making formulas more
    readable and maintainable.

    Attributes:
        name: The symbolic name for the range
        sheet: The sheet containing the range
        row_start: Starting row (0-indexed)
        col_start: Starting column (0-indexed)
        row_end: Ending row (0-indexed, inclusive)
        col_end: Ending column (0-indexed, inclusive)
    """
    name: str
    sheet: str
    row_start: int
    col_start: int
    row_end: int
    col_end: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "type": "NamedRange",
            "name": self.name,
            "sheet": self.sheet,
            "row_start": self.row_start,
            "col_start": self.col_start,
            "row_end": self.row_end,
            "col_end": self.col_end,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NamedRange":
        """Create from dictionary representation."""
        return cls(
            name=data["name"],
            sheet=data["sheet"],
            row_start=data["row_start"],
            col_start=data["col_start"],
            row_end=data["row_end"],
            col_end=data["col_end"],
        )


# Type alias for all operation types
SpreadsheetOp = Union[CreateSheet, SetValues, SetFormula, NamedRange]


def op_from_dict(data: Dict[str, Any]) -> SpreadsheetOp:
    """Deserialize an operation from dictionary representation.

    Args:
        data: Dictionary with 'type' key indicating operation type

    Returns:
        The corresponding operation object

    Raises:
        ValueError: If the operation type is unknown
    """
    op_type = data.get("type")
    if op_type == "CreateSheet":
        return CreateSheet.from_dict(data)
    elif op_type == "SetValues":
        return SetValues.from_dict(data)
    elif op_type == "SetFormula":
        return SetFormula.from_dict(data)
    elif op_type == "NamedRange":
        return NamedRange.from_dict(data)
    else:
        raise ValueError(f"Unknown operation type: {op_type}")
