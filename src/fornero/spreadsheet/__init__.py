"""
Spreadsheet algebra module.

This module provides abstractions for working with spreadsheet concepts
and translating dataframe operations to spreadsheet formulas.
"""

from fornero.spreadsheet.model import (
    Sheet,
    Range,
    Formula,
    Value,
    Reference,
)
from fornero.spreadsheet.operations import (
    CreateSheet,
    SetValues,
    SetFormula,
    NamedRange,
    SpreadsheetOp,
    op_from_dict,
)

__all__ = [
    "Sheet",
    "Range",
    "Formula",
    "Value",
    "Reference",
    "CreateSheet",
    "SetValues",
    "SetFormula",
    "NamedRange",
    "SpreadsheetOp",
    "op_from_dict",
]
