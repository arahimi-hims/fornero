"""
Operation tracer for fornero DataFrames.

This module provides utilities for tracing pandas operations and converting them
to logical plan operations. The tracer captures operation arguments and builds
the appropriate algebra nodes.

The tracer is designed to work in dual mode: operations execute normally in pandas
while simultaneously recording their intent in the logical plan.
"""

from __future__ import annotations

from ..algebra import (
    LogicalPlan,
    Source,
    Select,
    Filter,
    Join,
    GroupBy,
    Aggregate,
    Sort,
    Limit,
    WithColumn,
    Union,
)


def trace_filter(df, condition, predicate_str: str | None = None) -> LogicalPlan:
    """Trace a filter operation.

    Args:
        df: Source DataFrame
        condition: Boolean condition (can be a Series or array)
        predicate_str: Optional string representation of the predicate

    Returns:
        New LogicalPlan with Filter node
    """
    if predicate_str is None:
        # Try to extract predicate string from condition
        if hasattr(condition, 'name'):
            predicate_str = f"{condition.name} filter"
        else:
            predicate_str = "boolean filter"

    filter_op = Filter(predicate=predicate_str, inputs=[df._plan.root])
    return LogicalPlan(filter_op)


def trace_select(df, columns: list[str]) -> LogicalPlan:
    """Trace a column selection operation.

    Args:
        df: Source DataFrame
        columns: List of column names to select

    Returns:
        New LogicalPlan with Select node
    """
    select_op = Select(columns=columns, inputs=[df._plan.root])
    return LogicalPlan(select_op)


def trace_sort(df, by: str | list[str], ascending: bool | list[bool] = True) -> LogicalPlan:
    """Trace a sort operation.

    Args:
        df: Source DataFrame
        by: Column name(s) to sort by
        ascending: Sort direction(s)

    Returns:
        New LogicalPlan with Sort node
    """
    # Normalize to lists
    if isinstance(by, str):
        by = [by]
    if isinstance(ascending, bool):
        ascending = [ascending] * len(by)

    # Build sort keys
    keys = [(col, "asc" if asc else "desc") for col, asc in zip(by, ascending)]

    sort_op = Sort(keys=keys, inputs=[df._plan.root])
    return LogicalPlan(sort_op)


def trace_limit(df, count: int, end: str = "head") -> LogicalPlan:
    """Trace a limit operation (head or tail).

    Args:
        df: Source DataFrame
        count: Number of rows to keep
        end: Which end to limit from ('head' or 'tail')

    Returns:
        New LogicalPlan with Limit node
    """
    limit_op = Limit(count=count, end=end, inputs=[df._plan.root])
    return LogicalPlan(limit_op)


def trace_groupby(
    df,
    keys: str | list[str],
    aggregations: list[tuple]
) -> LogicalPlan:
    """Trace a groupby operation.

    Args:
        df: Source DataFrame
        keys: Column(s) to group by
        aggregations: List of (output_name, function, input_column) tuples

    Returns:
        New LogicalPlan with GroupBy node
    """
    # Normalize keys to list
    if isinstance(keys, str):
        keys = [keys]

    groupby_op = GroupBy(keys=keys, aggregations=aggregations, inputs=[df._plan.root])
    return LogicalPlan(groupby_op)


def trace_aggregate(df, aggregations: list[tuple]) -> LogicalPlan:
    """Trace a global aggregation operation (no grouping).

    Args:
        df: Source DataFrame
        aggregations: List of (output_name, function, input_column) tuples

    Returns:
        New LogicalPlan with Aggregate node
    """
    agg_op = Aggregate(aggregations=aggregations, inputs=[df._plan.root])
    return LogicalPlan(agg_op)


def trace_with_column(df, column: str, expression: str) -> LogicalPlan:
    """Trace a column addition/modification operation.

    Args:
        df: Source DataFrame
        column: Name of column to add/modify
        expression: String representation of the expression

    Returns:
        New LogicalPlan with WithColumn node
    """
    with_col_op = WithColumn(column=column, expression=expression, inputs=[df._plan.root])
    return LogicalPlan(with_col_op)


def trace_join(
    left_df,
    right_df,
    left_on: str | list[str],
    right_on: str | list[str],
    join_type: str = "inner",
    suffixes: tuple = ("_x", "_y")
) -> LogicalPlan:
    """Trace a join operation.

    Args:
        left_df: Left DataFrame
        right_df: Right DataFrame
        left_on: Column(s) to join on (left side)
        right_on: Column(s) to join on (right side)
        join_type: Type of join ('inner', 'left', 'right', 'outer')
        suffixes: Suffixes for overlapping column names

    Returns:
        New LogicalPlan with Join node
    """
    # Get right DataFrame's plan root
    if hasattr(right_df, '_plan'):
        right_root = right_df._plan.root
    else:
        # Create a Source node for regular pandas DataFrame
        schema = list(right_df.columns) if hasattr(right_df, 'columns') else None
        right_root = Source(source_id="<right_dataframe>", schema=schema)

    join_op = Join(
        left_on=left_on,
        right_on=right_on,
        join_type=join_type,
        suffixes=suffixes,
        inputs=[left_df._plan.root, right_root]
    )
    return LogicalPlan(join_op)


def trace_union(df1, df2) -> LogicalPlan:
    """Trace a union operation (vertical concatenation).

    Args:
        df1: First DataFrame
        df2: Second DataFrame

    Returns:
        New LogicalPlan with Union node
    """
    # Get second DataFrame's plan root
    if hasattr(df2, '_plan'):
        df2_root = df2._plan.root
    else:
        # Create a Source node for regular pandas DataFrame
        schema = list(df2.columns) if hasattr(df2, 'columns') else None
        df2_root = Source(source_id="<dataframe>", schema=schema)

    union_op = Union(inputs=[df1._plan.root, df2_root])
    return LogicalPlan(union_op)


def build_predicate_string(condition) -> str:
    """Build a string representation of a filter predicate.

    This is a best-effort function that tries to extract a meaningful
    predicate string from a boolean condition.

    Args:
        condition: Boolean condition (Series, array, or expression)

    Returns:
        String representation of the predicate
    """
    # Try to get a meaningful representation
    if hasattr(condition, 'name') and condition.name:
        return f"{condition.name} filter"
    elif hasattr(condition, '__str__'):
        s = str(condition)
        # Limit length to avoid very long strings
        if len(s) > 100:
            return "complex filter"
        return s
    else:
        return "boolean filter"
