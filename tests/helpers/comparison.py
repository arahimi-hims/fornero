"""
Comparison utilities for end-to-end correctness tests.

Provides matrix-level comparison between a pandas DataFrame (the expected result)
and a 2D list of values read back from a spreadsheet (the actual result).
"""

import math
from typing import Any, List

import pandas as pd


def dataframe_to_matrix(df: pd.DataFrame, include_header: bool = True) -> List[List[Any]]:
    """Convert a pandas DataFrame to a 2D list of values.

    Args:
        df: DataFrame to convert
        include_header: If True, the first row is the column headers

    Returns:
        2D list where each inner list is a row of cell values
    """
    rows: List[List[Any]] = []
    if include_header:
        rows.append(list(df.columns))
    for _, row in df.iterrows():
        rows.append([_normalize_value(v) for v in row])
    return rows


def _normalize_value(v: Any) -> Any:
    """Normalise a Python value to something comparable with spreadsheet output.

    Spreadsheets return strings for everything read back via get_all_values,
    so convert numerics to their string form when needed.  For in-memory
    comparison we keep native types and rely on _values_equal for tolerance.
    """
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    return v


def _values_equal(expected: Any, actual: Any, rtol: float = 1e-6) -> bool:
    """Compare two cell values with tolerance for floats.

    Args:
        expected: Value from the pandas DataFrame
        actual: Value from the spreadsheet
        rtol: Relative tolerance for floating-point comparison

    Returns:
        True if values are considered equal
    """
    if expected == "" and actual == "":
        return True

    # Try numeric comparison
    try:
        e = float(expected)
        a = float(actual)
        if e == 0 and a == 0:
            return True
        return abs(e - a) <= rtol * max(abs(e), abs(a))
    except (ValueError, TypeError):
        pass

    return str(expected) == str(actual)


def assert_matrix_equal(
    expected: List[List[Any]],
    actual: List[List[Any]],
    rtol: float = 1e-6,
    check_shape: bool = True,
) -> None:
    """Assert that two 2D matrices are cell-by-cell equal (with float tolerance).

    Args:
        expected: The reference matrix (from pandas)
        actual: The matrix under test (from the spreadsheet or mock)
        rtol: Relative tolerance for floating-point values
        check_shape: If True, also assert identical row/col counts

    Raises:
        AssertionError with a message pinpointing the first mismatch
    """
    if check_shape:
        assert len(expected) == len(actual), (
            f"Row count mismatch: expected {len(expected)}, got {len(actual)}"
        )
        for i, (e_row, a_row) in enumerate(zip(expected, actual)):
            assert len(e_row) == len(a_row), (
                f"Column count mismatch in row {i}: expected {len(e_row)}, got {len(a_row)}"
            )

    for i, (e_row, a_row) in enumerate(zip(expected, actual)):
        for j, (e_val, a_val) in enumerate(zip(e_row, a_row)):
            assert _values_equal(e_val, a_val, rtol), (
                f"Cell mismatch at ({i}, {j}): expected {e_val!r}, got {a_val!r}"
            )


def extract_source_data(df: pd.DataFrame) -> List[List[Any]]:
    """Convert a DataFrame to the source_data row format expected by the translator.

    Args:
        df: Source DataFrame

    Returns:
        List of rows, each row a list of cell values
    """
    rows: List[List[Any]] = []
    for _, row in df.iterrows():
        rows.append([_normalize_value(v) for v in row])
    return rows
