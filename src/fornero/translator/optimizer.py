"""
Optimization passes for logical plans.

These optimizations operate purely on the plan structure without inspecting data:
- Predicate pushdown: Move filters closer to sources
- Projection pushdown: Select only needed columns early
- Formula simplification: Eliminate identity operations
"""

from typing import List, Set, Optional
from fornero.algebra.operations import (
    Operation, Source, Select, Filter, GroupBy, Aggregate,
    Sort, WithColumn, Limit
)
from fornero.algebra.logical_plan import LogicalPlan


class Optimizer:
    """Optimizes logical plans through structural transformations."""

    def __init__(self):
        """Initialize optimizer."""
        pass

    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        """Apply all optimization passes to a plan.

        Args:
            plan: LogicalPlan to optimize

        Returns:
            Optimized LogicalPlan
        """
        optimized_root = plan.root

        # Apply optimization passes
        optimized_root = self._predicate_pushdown(optimized_root)
        optimized_root = self._projection_pushdown(optimized_root)
        optimized_root = self._fuse_operations(optimized_root)
        optimized_root = self._simplify_operations(optimized_root)

        return LogicalPlan(optimized_root)

    def _fuse_operations(self, op: Operation) -> Operation:
        """Fuse adjacent compatible operations.

        Optimizations:
        - Limit(Sort(...)) -> Sort(..., limit=n)
        - Sort(Filter(...)) -> Sort(..., predicate=p)
        """
        # Recursively optimize inputs first
        optimized_inputs = [self._fuse_operations(inp) for inp in op.inputs]

        # 1. Limit(Sort) fusion
        if isinstance(op, Limit):
            if len(optimized_inputs) == 1 and isinstance(optimized_inputs[0], Sort):
                child_sort = optimized_inputs[0]
                # Push limit into Sort. If Sort already has limit, take the smaller one.
                new_limit = op.count
                if child_sort.limit is not None:
                    new_limit = min(new_limit, child_sort.limit)

                # Clone sort with new limit
                new_sort = Sort(keys=child_sort.keys, inputs=child_sort.inputs,
                               limit=new_limit, predicate=child_sort.predicate)
                return new_sort

        # 2. Sort(Filter) fusion
        if isinstance(op, Sort):
            if len(optimized_inputs) == 1 and isinstance(optimized_inputs[0], Filter):
                child_filter = optimized_inputs[0]
                # Fuse filter into sort
                # If Sort already has a predicate (unlikely unless we have multiple layers), AND them.
                new_pred = child_filter.predicate
                if op.predicate:
                    new_pred = f"({op.predicate}) AND ({child_filter.predicate})"

                new_sort = Sort(keys=op.keys, inputs=child_filter.inputs,
                               limit=op.limit, predicate=new_pred)
                return new_sort

        # 3. Select(Filter) fusion
        if isinstance(op, Select):
            if len(optimized_inputs) == 1 and isinstance(optimized_inputs[0], Filter):
                child_filter = optimized_inputs[0]
                # Fuse filter into select
                new_pred = child_filter.predicate
                if op.predicate:
                    new_pred = f"({op.predicate}) AND ({child_filter.predicate})"

                # Clone select with fused predicate and inputs of filter
                new_select = Select(columns=op.columns, inputs=child_filter.inputs, predicate=new_pred)
                return new_select

        # Update with optimized inputs
        if optimized_inputs != op.inputs:
            return self._clone_with_inputs(op, optimized_inputs)

        return op

    def _predicate_pushdown(self, op: Operation) -> Operation:
        """Push filter predicates down toward sources.

        This reduces the amount of data flowing through the plan by filtering early.

        Args:
            op: Operation to optimize

        Returns:
            Optimized operation
        """
        # Recursively optimize inputs first
        optimized_inputs = [self._predicate_pushdown(inp) for inp in op.inputs]

        # If this is a Filter, try to push it down
        if isinstance(op, Filter):
            if len(optimized_inputs) == 1:
                child = optimized_inputs[0]

                # Can push filter down past Select if predicate only references selected columns
                if isinstance(child, Select):
                    predicate_cols = self._extract_column_references(op.predicate)
                    if predicate_cols.issubset(set(child.columns)):
                        # Push filter below select: Select(Filter(child.input))
                        if len(child.inputs) == 1:
                            new_filter = Filter(predicate=op.predicate, inputs=child.inputs)
                            new_select = Select(columns=child.columns, inputs=[new_filter])
                            return new_select

                # Can push filter down past another filter (combine them)
                if isinstance(child, Filter):
                    # Combine predicates with AND
                    combined_predicate = f"({child.predicate}) AND ({op.predicate})"
                    return Filter(predicate=combined_predicate, inputs=child.inputs)

        # For other operations, just update with optimized inputs
        if optimized_inputs != op.inputs:
            return self._clone_with_inputs(op, optimized_inputs)

        return op

    def _projection_pushdown(self, op: Operation) -> Operation:
        """Push column projections down toward sources.

        This reduces data volume by selecting only needed columns early.

        Args:
            op: Operation to optimize

        Returns:
            Optimized operation
        """
        # Recursively optimize inputs first
        optimized_inputs = [self._projection_pushdown(inp) for inp in op.inputs]

        # If this is a Select followed by another Select, merge them
        if isinstance(op, Select):
            if len(optimized_inputs) == 1 and isinstance(optimized_inputs[0], Select):
                child = optimized_inputs[0]
                # The outer select's columns must be a subset of the inner select's columns
                # Keep only the outer select (more restrictive)
                return Select(columns=op.columns, inputs=child.inputs)

        # For Join, we could push down projections to only fetch needed columns
        # (more complex, not implemented in this basic version)

        # Update with optimized inputs
        if optimized_inputs != op.inputs:
            return self._clone_with_inputs(op, optimized_inputs)

        return op

    def _simplify_operations(self, op: Operation) -> Operation:
        """Simplify or eliminate trivial operations.

        Examples:
        - Select with all columns (identity) -> remove
        - Filter with tautological predicate -> remove
        - Limit with count >= data size -> remove

        Args:
            op: Operation to optimize

        Returns:
            Optimized operation
        """
        # Recursively optimize inputs first
        optimized_inputs = [self._simplify_operations(inp) for inp in op.inputs]

        # Detect identity Select: if selecting all columns in order, just pass through
        if isinstance(op, Select):
            if len(optimized_inputs) == 1:
                child = optimized_inputs[0]
                # Get child's output schema (heuristic: for Source, it's schema; others pass through)
                child_schema = self._get_output_schema(child)
                if child_schema and op.columns == child_schema:
                    # Identity select - eliminate
                    return child

        # Detect tautological Filter
        if isinstance(op, Filter):
            if op.predicate.strip() in ("1", "TRUE", "True", "true"):
                # Always-true filter - eliminate
                if len(optimized_inputs) == 1:
                    return optimized_inputs[0]

        # Detect consecutive Sorts (keep only the last one)
        if isinstance(op, Sort):
            if len(optimized_inputs) == 1 and isinstance(optimized_inputs[0], Sort):
                # Outer sort overrides inner sort
                return Sort(keys=op.keys, inputs=optimized_inputs[0].inputs)

        # Update with optimized inputs
        if optimized_inputs != op.inputs:
            return self._clone_with_inputs(op, optimized_inputs)

        return op

    def _extract_column_references(self, predicate: str) -> Set[str]:
        """Extract column names referenced in a predicate.

        This is a simplified implementation using tokenization.

        Args:
            predicate: Predicate string

        Returns:
            Set of column names
        """
        # Simple heuristic: split by common operators and keywords
        import re

        # Split on operators and punctuation
        tokens = re.split(r'[<>=!()&|+\-*/\s]+', predicate)

        # Filter out numbers and common keywords
        keywords = {'AND', 'OR', 'NOT', 'TRUE', 'FALSE', 'NULL'}
        columns = set()

        for token in tokens:
            token = token.strip().strip("'\"")
            if token and not token.isnumeric() and token.upper() not in keywords:
                # Try to parse as number
                try:
                    float(token)
                except ValueError:
                    # Not a number, likely a column name
                    columns.add(token)

        return columns

    def _get_output_schema(self, op: Operation) -> List[str]:
        """Heuristically determine the output schema of an operation.

        Args:
            op: Operation

        Returns:
            List of column names, or empty list if unknown
        """
        if isinstance(op, Source):
            return op.schema or []
        elif isinstance(op, Select):
            return op.columns
        elif isinstance(op, GroupBy):
            return op.keys + [agg[0] for agg in op.aggregations]
        elif isinstance(op, Aggregate):
            return [agg[0] for agg in op.aggregations]
        elif isinstance(op, WithColumn):
            # Add or replace column
            if len(op.inputs) == 1:
                input_schema = self._get_output_schema(op.inputs[0])
                if op.column in input_schema:
                    return input_schema
                else:
                    return input_schema + [op.column]
        # For most operations, schema passes through
        elif len(op.inputs) == 1:
            return self._get_output_schema(op.inputs[0])

        return []

    def _clone_with_inputs(self, op: Operation, new_inputs: List[Operation]) -> Operation:
        """Clone an operation with new inputs.

        Args:
            op: Operation to clone
            new_inputs: New input operations

        Returns:
            New operation instance with same parameters but different inputs
        """
        # Use to_dict/from_dict for cloning
        data = op.to_dict()
        data['inputs'] = [inp.to_dict() for inp in new_inputs]
        return Operation.from_dict(data)


def optimize_plan(plan: LogicalPlan) -> LogicalPlan:
    """Convenience function to optimize a plan.

    Args:
        plan: LogicalPlan to optimize

    Returns:
        Optimized LogicalPlan
    """
    optimizer = Optimizer()
    return optimizer.optimize(plan)
