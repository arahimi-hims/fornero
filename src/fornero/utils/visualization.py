"""
Plan visualization utilities.

Provides text-based tree rendering for logical plans. The visualization shows
the operation tree structure with indentation and connectors, making it easy
to understand the dataflow from sources to final result.
"""

from typing import Set
from ..algebra.logical_plan import LogicalPlan
from ..algebra.operations import (
    Operation, Source, Select, Filter, Join, GroupBy,
    Aggregate, Sort, Limit, WithColumn, Union, Pivot, Melt, Window
)


def visualize(plan: LogicalPlan) -> str:
    """Generate a text-based tree visualization of the plan.

    The visualization shows the operation tree structure with indentation
    to represent parent-child relationships. Each operation is shown with
    its key parameters.

    Args:
        plan: The logical plan to visualize

    Returns:
        A string containing the tree-shaped visualization

    Example:
        >>> source = Source(source_id="data.csv", schema=["a", "b"])
        >>> filtered = Filter(predicate="a > 10", inputs=[source])
        >>> plan = LogicalPlan(filtered)
        >>> print(visualize(plan))
        Filter(predicate='a > 10')
        └── Source(source_id='data.csv')
    """
    if not isinstance(plan, LogicalPlan):
        raise TypeError(f"Expected LogicalPlan, got {type(plan)}")

    lines = []
    visited: Set[int] = set()
    _visualize_operation(plan.root, lines, prefix=None, is_last=True, visited=visited)
    return "\n".join(lines)


def _visualize_operation(
    op: Operation,
    lines: list,
    prefix: str,
    is_last: bool,
    visited: Set[int]
) -> None:
    """Recursively visualize an operation and its inputs.

    Args:
        op: Operation to visualize
        lines: List to append visualization lines to
        prefix: Current prefix string for indentation
        is_last: Whether this is the last child of its parent
        visited: Set of operation IDs already visited (for cycle detection)
    """
    # Mark as visited
    op_id = id(op)
    if op_id in visited:
        # Already processed, skip to avoid infinite loops
        lines.append(prefix + ("└── " if is_last else "├── ") + "[already shown]")
        return
    visited.add(op_id)

    # Format this operation
    op_desc = _format_operation(op)

    # Add connector and operation description
    if prefix is None:
        # Root node - no prefix or connector
        lines.append(op_desc)
        # Children of root should start with empty prefix (they will get connectors)
        child_prefix = ""
    elif prefix == "":
        # First level children of root
        connector = "└── " if is_last else "├── "
        lines.append(connector + op_desc)
        # Extension for children depends on whether this is the last child
        extension = "    " if is_last else "│   "
        child_prefix = extension
    else:
        # Deeper nodes - add connector with full prefix
        connector = "└── " if is_last else "├── "
        lines.append(prefix + connector + op_desc)
        # Extension for children depends on whether this is the last child
        extension = "    " if is_last else "│   "
        child_prefix = prefix + extension

    # Process children (inputs)
    if op.inputs:
        for i, input_op in enumerate(op.inputs):
            is_last_child = (i == len(op.inputs) - 1)
            _visualize_operation(input_op, lines, child_prefix, is_last_child, visited)


def _format_operation(op: Operation) -> str:
    """Format an operation as a string with its key parameters.

    Args:
        op: Operation to format

    Returns:
        Formatted string representation
    """
    op_type = op.__class__.__name__

    if isinstance(op, Source):
        if op.schema:
            return f"{op_type}(source_id='{op.source_id}', schema={op.schema})"
        return f"{op_type}(source_id='{op.source_id}')"

    elif isinstance(op, Select):
        return f"{op_type}(columns={op.columns})"

    elif isinstance(op, Filter):
        return f"{op_type}(predicate='{op.predicate}')"

    elif isinstance(op, Join):
        return f"{op_type}(left_on={op.left_on}, right_on={op.right_on}, type='{op.join_type}')"

    elif isinstance(op, GroupBy):
        return f"{op_type}(keys={op.keys}, aggregations={op.aggregations})"

    elif isinstance(op, Aggregate):
        return f"{op_type}(aggregations={op.aggregations})"

    elif isinstance(op, Sort):
        return f"{op_type}(keys={op.keys})"

    elif isinstance(op, Limit):
        return f"{op_type}(count={op.count}, end='{op.end}')"

    elif isinstance(op, WithColumn):
        return f"{op_type}(column='{op.column}', expression='{op.expression}')"

    elif isinstance(op, Union):
        return f"{op_type}()"

    elif isinstance(op, Pivot):
        agg_str = f", aggfunc='{op.aggfunc}'" if op.aggfunc != "first" else ""
        return f"{op_type}(index={op.index}, columns='{op.columns}', values='{op.values}'{agg_str})"

    elif isinstance(op, Melt):
        value_vars_str = f", value_vars={op.value_vars}" if op.value_vars else ""
        return f"{op_type}(id_vars={op.id_vars}{value_vars_str})"

    elif isinstance(op, Window):
        parts = [f"function='{op.function}'", f"output='{op.output_column}'"]
        if op.partition_by:
            parts.append(f"partition_by={op.partition_by}")
        if op.order_by:
            parts.append(f"order_by={op.order_by}")
        if op.input_column:
            parts.append(f"input='{op.input_column}'")
        return f"{op_type}({', '.join(parts)})"

    else:
        # Fallback for unknown operation types
        return f"{op_type}()"
