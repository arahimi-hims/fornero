"""
Google Sheets function implementations for the local executor.

Registers custom Python callbacks for Google Sheets functions that are not
natively supported (or not fully supported) by formualizer's built-in
Excel engine.  Each function receives its arguments as already-evaluated
Python values — ranges arrive as nested lists (2D), scalars as plain
values, and boolean expressions as nested lists of booleans.

Functions registered here:

  FILTER        — array filtering by boolean mask
  UNIQUE        — deduplicate rows (preserves first-appearance order)
  ARRAYFORMULA  — identity; formualizer spills natively
  ARRAY_CONSTRAIN — truncate an array to (max_rows, max_cols)
  TRANSPOSE     — matrix transpose
  SORT          — multi-key sort supporting both column-index and
                  array-of-values sort keys (overrides built-in so it
                  can consume output from other custom functions)
  XMATCH        — vectorised position lookup (overrides built-in to
                  accept array lookup_value and custom-function results)
  XLOOKUP       — vectorised lookup (overrides built-in to support array
                  lookup_value, which the built-in does not handle)
  QUERY         — minimal SQL-like aggregation (SELECT … GROUP BY)
  BYROW         — not implementable as a custom function (requires LAMBDA);
                  Pivot programs are expected to xfail for now
"""

from __future__ import annotations

import re
from typing import Any, List

import formualizer as fz


def _flatten_col(col: Any) -> Any:
    """Unwrap a single-column nested list to a scalar."""
    if isinstance(col, list) and len(col) == 1:
        return _flatten_col(col[0])
    return col


def _ensure_2d(data: Any) -> List[List[Any]]:
    """Normalise data to a 2D list (list of rows)."""
    if not isinstance(data, list):
        return [[data]]
    if not data:
        return []
    if not isinstance(data[0], list):
        return [[v] for v in data]
    return data


def _gsheets_filter(data: Any, condition: Any, if_empty: Any = None) -> Any:
    """FILTER(data, condition [, if_empty])

    Keeps rows from *data* where the corresponding *condition* value is
    truthy.  *condition* is already evaluated (e.g. a column of booleans).
    """
    data_2d = _ensure_2d(data)
    cond_2d = _ensure_2d(condition)

    result = []
    for row, cond_row in zip(data_2d, cond_2d):
        cond_val = _flatten_col(cond_row)
        if cond_val:
            result.append(row)

    if not result:
        if if_empty is not None:
            return _ensure_2d(if_empty)
        return [[""]]
    return result


def _gsheets_unique(data: Any) -> Any:
    """UNIQUE(data)

    Returns distinct rows in first-appearance order.
    """
    data_2d = _ensure_2d(data)
    seen: list[list[Any]] = []
    for row in data_2d:
        if row not in seen:
            seen.append(row)
    return seen if seen else [[""]]


def _gsheets_arrayformula(value: Any) -> Any:
    """ARRAYFORMULA(expr)

    Identity — formualizer already spills array results natively.
    """
    return value


def _gsheets_array_constrain(data: Any, max_rows: Any, max_cols: Any) -> Any:
    """ARRAY_CONSTRAIN(data, max_rows, max_cols)

    Truncates *data* to at most *max_rows* rows and *max_cols* columns.
    """
    data_2d = _ensure_2d(data)
    mr = int(max_rows)
    mc = int(max_cols)
    return [row[:mc] for row in data_2d[:mr]]


def _gsheets_transpose(data: Any) -> Any:
    """TRANSPOSE(data)

    Swaps rows and columns.
    """
    data_2d = _ensure_2d(data)
    if not data_2d:
        return [[""]]
    rows = len(data_2d)
    cols = len(data_2d[0])
    return [[data_2d[r][c] for r in range(rows)] for c in range(cols)]


def _gsheets_sort(data: Any, *sort_args: Any) -> Any:
    """SORT(data, sort_col_or_array, is_ascending [, ...])

    Multi-key stable sort.  The sort key can be either a 1-based column
    index (integer/float) or an array of values with the same height as
    *data* (used by the ``XMATCH(…, UNIQUE(…))`` pattern).
    """
    data_2d = _ensure_2d(data)
    if not data_2d:
        return [[""]]

    pairs = list(zip(sort_args[::2], sort_args[1::2]))

    result = list(data_2d)
    for key_spec, asc in reversed(pairs):
        if isinstance(key_spec, list):
            key_vals = [_flatten_col(row) for row in _ensure_2d(key_spec)]
            indexed = list(zip(result, key_vals))
            indexed.sort(
                key=lambda t: _sort_coerce(t[1]),
                reverse=not asc,
            )
            result = [row for row, _ in indexed]
        else:
            idx = int(key_spec) - 1
            result.sort(
                key=lambda r, i=idx: _sort_coerce(r[i] if i < len(r) else ""),
                reverse=not asc,
            )
    return result


def _sort_coerce(v: Any) -> Any:
    """Coerce a value for sorting — numbers before strings."""
    try:
        return (0, float(v))
    except (ValueError, TypeError):
        return (1, str(v))


def _gsheets_xmatch(
    lookup_value: Any,
    lookup_array: Any,
    *_rest: Any,
) -> Any:
    """XMATCH(lookup_value, lookup_array [, match_mode, search_mode])

    Returns the relative position (1-based) of *lookup_value* within
    *lookup_array*.  Supports both scalar and array *lookup_value*.
    """
    search = [_flatten_col(row) for row in _ensure_2d(lookup_array)]

    def _find(key: Any) -> Any:
        for i, sv in enumerate(search):
            if _values_match(sv, key):
                return i + 1
        return {"type": "Error", "kind": "NA"}

    lv = _ensure_2d(lookup_value)
    if len(lv) == 1:
        return _find(_flatten_col(lv[0]))

    return [[_find(_flatten_col(row))] for row in lv]


def _gsheets_xlookup(
    lookup_value: Any,
    lookup_array: Any,
    return_array: Any,
    if_not_found: Any = None,
    *_rest: Any,
) -> Any:
    """XLOOKUP(lookup_value, lookup_array, return_array [, if_not_found])

    Supports both scalar and array *lookup_value*.  When *lookup_value*
    is a 2D list (column vector), returns one result row per lookup value.
    """
    lookup_flat = _ensure_2d(lookup_array)
    return_flat = _ensure_2d(return_array)
    default = if_not_found if if_not_found is not None else {"type": "Error", "kind": "NA"}

    search_keys = [_flatten_col(row) for row in lookup_flat]
    return_rows = return_flat

    def _find(key: Any) -> list[Any]:
        for sk, rr in zip(search_keys, return_rows):
            if _values_match(sk, key):
                return rr
        if isinstance(default, list):
            return _ensure_2d(default)[0] if default else [""]
        return_width = len(return_rows[0]) if return_rows else 1
        return [default] * return_width

    lookup_vals = _ensure_2d(lookup_value)
    if len(lookup_vals) == 1:
        result_row = _find(_flatten_col(lookup_vals[0]))
        if len(result_row) == 1:
            return result_row[0]
        return [result_row]

    return [_find(_flatten_col(lv)) for lv in lookup_vals]


def _values_match(a: Any, b: Any) -> bool:
    """Compare two cell values, coercing types where sensible."""
    if a == b:
        return True
    try:
        return float(a) == float(b)
    except (ValueError, TypeError):
        return str(a) == str(b)


_AGG_FUNCS = {
    "SUM": "sum",
    "AVG": "mean",
    "COUNT": "count",
    "MIN": "min",
    "MAX": "max",
}

_QUERY_RE = re.compile(
    r"SELECT\s+(?P<select>.+?)\s+GROUP\s+BY\s+(?P<groupby>.+?)(?:\s+LABEL\s+.+)?$",
    re.IGNORECASE,
)

_COL_LETTER_RE = re.compile(r"Col(\d+)", re.IGNORECASE)
_AGG_RE = re.compile(r"(\w+)\(Col(\d+)\)", re.IGNORECASE)


def _gsheets_query(data: Any, query_str: Any, *_rest: Any) -> Any:
    """QUERY(data, query_string)

    Minimal implementation covering the patterns Fornero generates:
    ``SELECT ColN, AGG(ColM) GROUP BY ColN``.

    The result includes a header row (matching Google Sheets behaviour).
    """
    import pandas as pd

    data_2d = _ensure_2d(data)
    if not data_2d:
        return [[""]]

    query_text = str(query_str).strip()

    header = data_2d[0]
    body = data_2d[1:]

    if not body:
        return [header]

    num_cols = len(header)
    df = pd.DataFrame(body, columns=[f"Col{i+1}" for i in range(num_cols)])

    m = _QUERY_RE.match(query_text)
    if not m:
        return data_2d

    select_clause = m.group("select").strip()
    groupby_clause = m.group("groupby").strip()

    group_cols = [c.strip() for c in groupby_clause.split(",")]

    select_parts = [p.strip() for p in select_clause.split(",")]
    agg_funcs: dict[str, str] = {}  # col -> pandas function name
    agg_labels: dict[str, str] = {}  # col -> output label
    plain_cols: list[str] = []

    for part in select_parts:
        agg_m = _AGG_RE.match(part)
        if agg_m:
            func_name = agg_m.group(1).upper()
            col_name = f"Col{agg_m.group(2)}"
            pd_func = _AGG_FUNCS.get(func_name, func_name.lower())
            out_label = f"{func_name.lower()}_{col_name}"
            agg_funcs[col_name] = pd_func
            agg_labels[col_name] = out_label
        else:
            col = part.strip()
            if _COL_LETTER_RE.match(col):
                plain_cols.append(col)

    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass

    if agg_funcs:
        grouped = df.groupby(group_cols, sort=True).agg(agg_funcs).reset_index()

        out_header = list(group_cols)
        for col in agg_labels:
            out_header.append(agg_labels[col])

        result_header = []
        for h in out_header:
            cm = _COL_LETTER_RE.match(h)
            if cm:
                idx = int(cm.group(1)) - 1
                result_header.append(header[idx] if idx < len(header) else h)
            else:
                result_header.append(h)

        rows: list[list[Any]] = [result_header]
        for _, row in grouped.iterrows():
            rows.append([_query_normalize(v) for v in row])
        return rows

    return data_2d


def _query_normalize(v: Any) -> Any:
    """Convert pandas types to plain Python for spreadsheet consumption."""
    import math

    if v is None:
        return ""
    try:
        if math.isnan(v):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(v, float) and v == int(v):
        return v
    return v


def register_gsheets_functions(wb: fz.Workbook) -> None:
    """Register all Google Sheets compatibility functions on *wb*.

    Must be called before any formulas are set.
    """
    wb.register_function(
        "FILTER", _gsheets_filter,
        min_args=2, max_args=3, allow_override_builtin=True,
    )
    wb.register_function(
        "UNIQUE", _gsheets_unique,
        min_args=1, max_args=1, allow_override_builtin=True,
    )
    wb.register_function(
        "ARRAYFORMULA", _gsheets_arrayformula,
        min_args=1, max_args=1,
    )
    wb.register_function(
        "ARRAY_CONSTRAIN", _gsheets_array_constrain,
        min_args=3, max_args=3,
    )
    wb.register_function(
        "TRANSPOSE", _gsheets_transpose,
        min_args=1, max_args=1, allow_override_builtin=True,
    )
    wb.register_function(
        "SORT", _gsheets_sort,
        min_args=3, max_args=11, allow_override_builtin=True,
    )
    wb.register_function(
        "XMATCH", _gsheets_xmatch,
        min_args=2, max_args=4, allow_override_builtin=True,
    )
    wb.register_function(
        "XLOOKUP", _gsheets_xlookup,
        min_args=3, max_args=6, allow_override_builtin=True,
    )
    wb.register_function(
        "QUERY", _gsheets_query,
        min_args=2, max_args=3,
    )
