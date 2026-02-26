"""
Logical plan representation for dataframe operations.

The LogicalPlan class wraps the root operation of a dataframe algebra tree and provides
methods for introspection, serialization, and debugging. It tracks the complete computation
graph from source to final result.
"""

from typing import Dict, Any
from .operations import Operation, Source


class LogicalPlan:
    """Logical plan for dataframe operations.

    A LogicalPlan wraps the root operation of a dataframe algebra tree. The tree is built
    by composing operations, with each operation referencing its input operations. The plan
    can be serialized to JSON, explained for debugging, and traversed for analysis.

    Attributes:
        root: The root operation of the plan (final result)

    Example:
        >>> source = Source(source_id="data.csv", schema=["a", "b", "c"])
        >>> filtered = Filter(predicate="a > 10", inputs=[source])
        >>> selected = Select(columns=["a", "b"], inputs=[filtered])
        >>> plan = LogicalPlan(selected)
        >>> print(plan.explain())
    """

    def __init__(self, root: Operation):
        """Initialize a logical plan with a root operation.

        Args:
            root: The root operation of the plan
        """
        if not isinstance(root, Operation):
            raise TypeError(f"Plan root must be an Operation, got {type(root)}")
        self._root = root

    @property
    def root(self) -> Operation:
        """Get the root operation of the plan."""
        return self._root

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the plan to a dictionary.

        Returns the root operation's dict directly so ``plan.to_dict()["type"]``
        gives the root operation type.
        """
        return self._root.to_dict()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LogicalPlan':
        """Deserialize a plan from a dictionary.

        Accepts either a wrapped ``{"root": ...}`` dict or the root operation
        dict directly (must have a ``"type"`` key).
        """
        if 'root' in data:
            root = Operation.from_dict(data['root'])
        elif 'type' in data:
            root = Operation.from_dict(data)
        else:
            raise ValueError("Plan dict must have 'root' or 'type' key")
        return cls(root)

    def explain(self, verbose: bool = False) -> str:
        """Generate a human-readable explanation of the plan.

        The explanation shows the operation tree from leaves (sources) to root (final result).
        Each operation is indented based on its depth in the tree.

        Args:
            verbose: If True, include more detailed information about each operation

        Returns:
            String explanation of the plan
        """
        lines = []
        lines.append("Logical Plan:")
        lines.append("=" * 60)

        # Traverse the tree and build explanation
        self._explain_operation(self._root, lines, indent=0, verbose=verbose)

        return "\n".join(lines)

    def _explain_operation(self, op: Operation, lines: list, indent: int, verbose: bool):
        """Recursively explain an operation and its inputs.

        Args:
            op: Operation to explain
            lines: List to append explanation lines to
            indent: Current indentation level
            verbose: Whether to include verbose details
        """
        # Process inputs first (leaves before root)
        for input_op in op.inputs:
            self._explain_operation(input_op, lines, indent, verbose)

        # Format this operation
        prefix = "  " * indent
        op_type = op.__class__.__name__

        # Build operation description
        if isinstance(op, Source):
            desc = f"{prefix}{op_type}(source_id='{op.source_id}'"
            if op.schema:
                desc += f", schema={op.schema}"
            desc += ")"

        elif hasattr(op, 'columns') and isinstance(op.columns, list):
            # Select operation
            from .operations import Select
            if isinstance(op, Select):
                desc = f"{prefix}{op_type}(columns={op.columns})"

        elif hasattr(op, 'predicate'):
            # Filter operation
            from .operations import Filter
            if isinstance(op, Filter):
                desc = f"{prefix}{op_type}(predicate='{op.predicate}')"

        elif hasattr(op, 'join_type'):
            # Join operation
            from .operations import Join
            if isinstance(op, Join):
                desc = f"{prefix}{op_type}(left_on={op.left_on}, right_on={op.right_on}, type='{op.join_type}')"

        elif hasattr(op, 'keys') and hasattr(op, 'aggregations'):
            # GroupBy operation
            from .operations import GroupBy
            if isinstance(op, GroupBy):
                desc = f"{prefix}{op_type}(keys={op.keys}, aggregations={op.aggregations})"

        elif hasattr(op, 'aggregations'):
            # Aggregate operation
            from .operations import Aggregate
            if isinstance(op, Aggregate):
                desc = f"{prefix}{op_type}(aggregations={op.aggregations})"

        elif hasattr(op, 'keys') and isinstance(getattr(op, 'keys', None), list) and len(op.keys) > 0 and isinstance(op.keys[0], (tuple, list)):
            # Sort operation
            from .operations import Sort
            if isinstance(op, Sort):
                desc = f"{prefix}{op_type}(keys={op.keys})"

        elif hasattr(op, 'count') and hasattr(op, 'end'):
            # Limit operation
            from .operations import Limit
            if isinstance(op, Limit):
                desc = f"{prefix}{op_type}(count={op.count}, end='{op.end}')"

        elif hasattr(op, 'column') and hasattr(op, 'expression'):
            # WithColumn operation
            from .operations import WithColumn
            if isinstance(op, WithColumn):
                desc = f"{prefix}{op_type}(column='{op.column}', expression='{op.expression}')"

        elif hasattr(op, 'index') and hasattr(op, 'values'):
            # Pivot operation
            from .operations import Pivot
            if isinstance(op, Pivot):
                desc = f"{prefix}{op_type}(index={op.index}, columns='{op.columns}', values='{op.values}')"

        elif hasattr(op, 'id_vars') and hasattr(op, 'var_name'):
            # Melt operation
            from .operations import Melt
            if isinstance(op, Melt):
                value_vars_str = f", value_vars={op.value_vars}" if op.value_vars else ""
                desc = f"{prefix}{op_type}(id_vars={op.id_vars}{value_vars_str})"

        elif hasattr(op, 'function') and hasattr(op, 'output_column'):
            # Window operation
            from .operations import Window
            if isinstance(op, Window):
                desc = f"{prefix}{op_type}(function='{op.function}', output='{op.output_column}'"
                if op.partition_by:
                    desc += f", partition_by={op.partition_by}"
                if op.order_by:
                    desc += f", order_by={op.order_by}"
                desc += ")"

        else:
            # Generic fallback - Union or unknown operation
            desc = f"{prefix}{op_type}()"

        lines.append(desc)

    def copy(self) -> 'LogicalPlan':
        """Create a shallow copy of the plan.

        Returns:
            New LogicalPlan instance with the same root operation
        """
        return LogicalPlan(self._root)

    def __repr__(self) -> str:
        """String representation of the plan."""
        return f"LogicalPlan(root={self._root.__class__.__name__})"

    def __str__(self) -> str:
        """String representation showing the plan explanation."""
        return self.explain()
