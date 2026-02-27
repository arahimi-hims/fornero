"""
fornero - A compiler that converts dataframe programs to Google Sheets spreadsheets.

This package provides a pandas-compatible DataFrame API that tracks operations in a logical
plan. The plan can be translated to Google Sheets formulas, allowing dataframe programs to
be compiled to spreadsheets.

Usage:
    >>> import fornero as pd
    >>> df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    >>> result = df[df['a'] > 1][['a', 'b']]
    >>> plan = result.to_spreadsheet_plan()

Key components:
- DataFrame: pandas-compatible DataFrame that tracks operations
- LogicalPlan: Intermediate representation of dataframe operations
- Translator: Converts logical plans to Google Sheets operations
- Executor: Executes spreadsheet operations on Google Sheets
"""

import pandas as _pd

# Import core components
from .core import DataFrame
from .algebra import LogicalPlan
from .compiler import compile, compile_to_sheets, compile_locally
from .exceptions import *

# Version
__version__ = "0.1.0"

# Re-export DataFrame as the primary interface
__all__ = [
    'DataFrame',
    'LogicalPlan',
    'compile',
    'compile_to_sheets',
    'compile_locally',
    'read_csv',
    'merge',
    'concat',
    'Series',
]


# Tracked variants of pandas functions

def read_csv(filepath_or_buffer, **kwargs):
    """Read a CSV file into a fornero DataFrame.

    This is a wrapper around pandas.read_csv that returns a fornero DataFrame
    with a Source node in the logical plan.

    Args:
        filepath_or_buffer: File path or file-like object
        **kwargs: Additional arguments passed to pandas.read_csv

    Returns:
        fornero.DataFrame with data from CSV

    Note:
        This function reads the CSV using pandas and wraps the result in a
        fornero DataFrame, which creates a Source node tracking the file path.
    """
    # Read using pandas
    df = _pd.read_csv(filepath_or_buffer, **kwargs)

    # Wrap in fornero DataFrame with source_id
    source_id = str(filepath_or_buffer) if not hasattr(filepath_or_buffer, 'read') else "<csv_file>"
    return DataFrame(df, source_id=source_id)


def merge(left, right, on=None, left_on=None, right_on=None, how='inner', suffixes=('_x', '_y'), **kwargs):
    """Merge two DataFrames (tracked operation).

    This is a wrapper around pandas.merge that returns a fornero DataFrame
    with a Join node in the logical plan.

    Args:
        left: Left DataFrame
        right: Right DataFrame
        on: Column name to join on (for both sides)
        left_on: Column name to join on (left side)
        right_on: Column name to join on (right side)
        how: Join type ('inner', 'left', 'right', 'outer')
        suffixes: Tuple of suffixes for overlapping columns
        **kwargs: Additional arguments passed to pandas.merge

    Returns:
        fornero.DataFrame with merged data and Join node in plan
    """
    # Convert left to fornero DataFrame if needed
    if not isinstance(left, DataFrame):
        left = DataFrame(left)

    # Use the DataFrame.merge method which tracks the operation
    return left.merge(right, on=on, left_on=left_on, right_on=right_on, how=how, suffixes=suffixes, **kwargs)


def concat(objs, axis=0, **kwargs):
    """Concatenate DataFrames (tracked operation).

    This is a wrapper around pandas.concat that returns a fornero DataFrame
    with appropriate tracking in the logical plan.

    Args:
        objs: List of DataFrames to concatenate
        axis: Axis along which to concatenate (0 for rows, 1 for columns)
        **kwargs: Additional arguments passed to pandas.concat

    Returns:
        fornero.DataFrame with concatenated data

    Note:
        For axis=0 (row-wise concat), this creates a Union node in the plan.
        For axis=1 (column-wise concat), the operation is tracked but may not
        be fully supported in translation.
    """
    # Execute concatenation with pandas
    result = _pd.concat(objs, axis=axis, **kwargs)

    # Wrap result in fornero DataFrame
    result = DataFrame(result)

    # If we have exactly two DataFrames and axis=0, create a Union node
    if axis == 0 and len(objs) == 2:
        from .algebra import Union, Source

        # Get plan roots from input DataFrames
        roots = []
        for obj in objs:
            if isinstance(obj, DataFrame):
                roots.append(obj._plan.root)
            else:
                # Create Source node for regular pandas DataFrame
                schema = list(obj.columns) if hasattr(obj, 'columns') else None
                roots.append(Source(source_id="<dataframe>", schema=schema))

        union_op = Union(inputs=roots)
        result._plan = LogicalPlan(union_op)

    return result


# Re-export pandas Series for convenience
Series = _pd.Series


# Re-export other commonly used pandas functions (untracked)
# These are provided for convenience but don't create logical plans

def to_datetime(*args, **kwargs):
    """Wrapper for pandas.to_datetime."""
    return _pd.to_datetime(*args, **kwargs)


def to_numeric(*args, **kwargs):
    """Wrapper for pandas.to_numeric."""
    return _pd.to_numeric(*args, **kwargs)


def to_timedelta(*args, **kwargs):
    """Wrapper for pandas.to_timedelta."""
    return _pd.to_timedelta(*args, **kwargs)


# Re-export pandas constants and types
NA = _pd.NA
NaT = _pd.NaT
NaN = _pd.np.nan if hasattr(_pd, 'np') else float('nan')


# Provide access to underlying pandas for advanced use
pandas = _pd
