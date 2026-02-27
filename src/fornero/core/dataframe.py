"""
fornero.DataFrame subclass with logical plan tracking.

This module defines a pandas DataFrame subclass that tracks operations in a logical plan.
The plan is built up as operations are performed, allowing the full computation graph to be
inspected, optimized, and translated to Google Sheets.

Key features:
- Dual-mode execution: operations execute eagerly in pandas AND record in the logical plan
- Plan propagation: the _plan attribute survives pandas operations via _metadata
- Translation API: to_spreadsheet_plan() converts the logical plan to spreadsheet operations
"""

import ast
import inspect
import re
import textwrap

import pandas as pd
from ..algebra import LogicalPlan, Source, Select, Filter, Sort, Limit, GroupBy, WithColumn, Join, Pivot
from ..algebra.expressions import Column


def _extract_lambda_expression(func, kwarg_name=None):
    """Extract an arithmetic expression string from a lambda callable.

    Uses inspect.getsource + AST analysis to decompose simple lambdas
    (e.g. ``lambda x: x["salary"] / 1000``) into formula-ready expressions
    (e.g. ``(salary / 1000)``).

    Falls back to ``"lambda expression"`` if analysis fails for any reason
    (unsupported syntax, dynamic code, interactive session, etc.).
    """
    try:
        source = textwrap.dedent(inspect.getsource(func)).strip()
        tree = ast.parse(source)
    except (OSError, TypeError, IndentationError):
        return "lambda expression"

    from ..translator.lambda_analyzer import LambdaAnalyzer
    analyzer = LambdaAnalyzer()

    if kwarg_name:
        for node in ast.walk(tree):
            if (isinstance(node, ast.keyword)
                    and node.arg == kwarg_name
                    and isinstance(node.value, ast.Lambda)):
                return _lambda_body_to_expr(analyzer, node.value)

    for node in ast.walk(tree):
        if isinstance(node, ast.Lambda):
            return _lambda_body_to_expr(analyzer, node)

    return "lambda expression"


def _lambda_body_to_expr(analyzer, lambda_node):
    """Convert a Lambda AST node into a plain arithmetic expression string.

    The LambdaAnalyzer produces a template with ``{{col}}`` placeholders;
    we strip the braces so downstream ``_translate_expression`` can do its
    normal column-name → range-reference replacement.
    """
    try:
        formula_template, _refs = analyzer._analyze_expression(lambda_node.body)
        return re.sub(r"\{\{(\w+)\}\}", r"\1", formula_template)
    except Exception:
        return "lambda expression"


class _TrackedSeries(pd.Series):
    """Series subclass that records comparison predicates for filter translation.

    When the user writes ``df["age"] > 30``, pandas creates a boolean Series.
    This subclass captures the comparison as a ``_predicate`` string (e.g.
    ``"age > 30"``) so the translator can emit a valid FILTER condition.
    """

    _metadata = ["_plan", "_predicate"]

    @property
    def _constructor(self):
        return _TrackedSeries

    @property
    def _constructor_expanddim(self):
        return lambda *a, **kw: DataFrame(*a, **kw)

    def __gt__(self, other):
        result = super().__gt__(other)
        result._predicate = Column(self.name) > other
        return result

    def __ge__(self, other):
        result = super().__ge__(other)
        result._predicate = Column(self.name) >= other
        return result

    def __lt__(self, other):
        result = super().__lt__(other)
        result._predicate = Column(self.name) < other
        return result

    def __le__(self, other):
        result = super().__le__(other)
        result._predicate = Column(self.name) <= other
        return result

    def __eq__(self, other):
        result = super().__eq__(other)
        result._predicate = Column(self.name) == other
        return result

    def __ne__(self, other):
        result = super().__ne__(other)
        result._predicate = Column(self.name) != other
        return result

    def __and__(self, other):
        result = super().__and__(other)
        left = getattr(self, "_predicate", None)
        right = getattr(other, "_predicate", None)
        if left and right:
            result._predicate = left & right
        return result

    def __or__(self, other):
        result = super().__or__(other)
        left = getattr(self, "_predicate", None)
        right = getattr(other, "_predicate", None)
        if left and right:
            result._predicate = left | right
        return result


class DataFrame(pd.DataFrame):
    """DataFrame subclass that tracks operations in a logical plan.

    This class extends pandas DataFrame to maintain a logical plan of all operations
    performed on the data. The plan can be translated to Google Sheets formulas while
    the data continues to be processed normally by pandas.

    Attributes:
        _plan: LogicalPlan tracking the computation graph from source to current state

    Example:
        >>> df = DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        >>> result = df[df['a'] > 1][['a', 'b']]
        >>> plan = result._plan
        >>> print(plan.explain())
    """

    # Tell pandas to preserve _plan when creating new DataFrames
    _metadata = ['_plan']

    @property
    def _constructor(self):
        """Return constructor for creating new instances of this class."""
        return DataFrame

    @property
    def _constructor_sliced(self):
        """Return _TrackedSeries so column access captures comparison predicates."""
        return _TrackedSeries

    def __init__(self, data=None, plan=None, source_id=None, **kwargs):
        """Initialize a fornero DataFrame.

        Args:
            data: Data to initialize the DataFrame with (same as pandas)
            plan: Optional LogicalPlan to attach (for internal use)
            source_id: Optional source identifier for the Source node
            **kwargs: Additional arguments passed to pandas DataFrame
        """
        # If data is a pandas DataFrame, extract its values
        if isinstance(data, pd.DataFrame) and not isinstance(data, DataFrame):
            # This handles the case of DataFrame(pd_df)
            super().__init__(data, **kwargs)
        else:
            super().__init__(data, **kwargs)

        # Attach or create a logical plan
        if plan is not None:
            self._plan = plan
        elif hasattr(data, '_plan'):
            # Preserve plan from another fornero DataFrame
            self._plan = data._plan
        else:
            # Create a new plan with a Source node
            if source_id is None:
                source_id = "<dataframe>"
            schema = list(self.columns) if len(self.columns) > 0 else None
            source = Source(source_id=source_id, schema=schema)
            self._plan = LogicalPlan(source)

    def to_spreadsheet_plan(self):
        """Convert the logical plan to a spreadsheet execution plan.

        This method translates the logical plan to a sequence of spreadsheet operations
        that can be executed on Google Sheets.

        Returns:
            ExecutionPlan containing the spreadsheet operations

        Note:
            This is a stub implementation. The actual translation logic is in the
            translator module and will be integrated when that module is available.
        """
        # Import here to avoid circular dependencies
        from ..translator import Translator

        translator = Translator()
        return translator.translate(self._plan)

    def filter(self, condition):
        """Filter rows based on a condition (tracked operation).

        Args:
            condition: Boolean array-like indicating which rows to keep

        Returns:
            New DataFrame with filtered rows and updated plan
        """
        # Execute the filter in pandas
        result = self[condition]

        predicate_str = getattr(condition, "_predicate", None)
        if not predicate_str:
            predicate_str = f"{condition.name} filter" if hasattr(condition, "name") else "boolean filter"

        # Create new plan with Filter operation
        filter_op = Filter(predicate=predicate_str, inputs=[self._plan.root])
        new_plan = LogicalPlan(filter_op)

        # Attach the new plan
        result._plan = new_plan
        return result

    def __getitem__(self, key):
        """Override indexing to track Select operations for column selection.

        Args:
            key: Column name(s) or boolean mask

        Returns:
            New DataFrame or Series with updated plan if applicable
        """
        result = super().__getitem__(key)

        # If result is a DataFrame (column selection), track as Select
        if isinstance(result, pd.DataFrame):
            # Ensure result is a fornero DataFrame
            if not isinstance(result, DataFrame):
                result = DataFrame(result)

            # If key is a list of columns, track as Select
            if isinstance(key, list):
                select_op = Select(columns=key, inputs=[self._plan.root])
                result._plan = LogicalPlan(select_op)
            # If key is a boolean mask, track as Filter
            elif hasattr(key, 'dtype') and key.dtype == bool:
                predicate_str = getattr(key, "_predicate", None)
                if not predicate_str:
                    predicate_str = f"{key.name} filter" if hasattr(key, "name") else "boolean filter"
                filter_op = Filter(predicate=predicate_str, inputs=[self._plan.root])
                result._plan = LogicalPlan(filter_op)
            else:
                # For single column (returns Series), preserve plan
                result._plan = self._plan
        elif isinstance(result, pd.Series):
            # Preserve plan on Series (for chaining)
            if hasattr(self, '_plan'):
                result._plan = self._plan

        return result

    def sort_values(self, by, ascending=True, **kwargs):
        """Sort by column(s) (tracked operation).

        Args:
            by: Column name or list of column names to sort by
            ascending: Sort ascending vs. descending. Can be bool or list of bools
            **kwargs: Additional arguments passed to pandas sort_values

        Returns:
            New DataFrame with sorted rows and updated plan
        """
        # Execute the sort in pandas
        result = super().sort_values(by=by, ascending=ascending, **kwargs)

        # Build sort keys list
        if isinstance(by, str):
            by = [by]
        if isinstance(ascending, bool):
            ascending = [ascending] * len(by)

        keys = [(col, "asc" if asc else "desc") for col, asc in zip(by, ascending)]

        # Create new plan with Sort operation
        sort_op = Sort(keys=keys, inputs=[self._plan.root])
        new_plan = LogicalPlan(sort_op)

        result._plan = new_plan
        return result

    def head(self, n=5):
        """Return first n rows (tracked operation).

        Args:
            n: Number of rows to return

        Returns:
            New DataFrame with limited rows and updated plan
        """
        # Execute the head in pandas
        result = super().head(n)

        # Create new plan with Limit operation
        limit_op = Limit(count=n, end="head", inputs=[self._plan.root])
        new_plan = LogicalPlan(limit_op)

        result._plan = new_plan
        return result

    def tail(self, n=5):
        """Return last n rows (tracked operation).

        Args:
            n: Number of rows to return

        Returns:
            New DataFrame with limited rows and updated plan
        """
        # Execute the tail in pandas
        result = super().tail(n)

        # Create new plan with Limit operation
        limit_op = Limit(count=n, end="tail", inputs=[self._plan.root])
        new_plan = LogicalPlan(limit_op)

        result._plan = new_plan
        return result

    def groupby(self, by, **kwargs):
        """Group by column(s) (returns GroupBy object that tracks operations).

        Args:
            by: Column name or list of column names to group by
            **kwargs: Additional arguments passed to pandas groupby

        Returns:
            DataFrameGroupBy object that tracks aggregation operations
        """
        # Return a custom GroupBy object that tracks operations
        return DataFrameGroupBy(self, by, **kwargs)

    def assign(self, **kwargs):
        """Add new columns or replace existing ones (tracked operation).

        Args:
            **kwargs: Column name -> value or callable mappings

        Returns:
            New DataFrame with added/modified columns and updated plan
        """
        # Execute the assign in pandas
        result = super().assign(**kwargs)

        current_root = self._plan.root
        for col_name, value in kwargs.items():
            if callable(value):
                expr_str = _extract_lambda_expression(value, col_name)
            else:
                expr_str = str(value)

            with_col_op = WithColumn(column=col_name, expression=expr_str, inputs=[current_root])
            current_root = with_col_op

        new_plan = LogicalPlan(current_root)
        result._plan = new_plan
        return result

    def merge(self, right, on=None, left_on=None, right_on=None, how='inner', suffixes=('_x', '_y'), **kwargs):
        """Merge with another DataFrame (tracked operation).

        Args:
            right: DataFrame to merge with
            on: Column name to join on (for both sides)
            left_on: Column name to join on (left side)
            right_on: Column name to join on (right side)
            how: Join type ('inner', 'left', 'right', 'outer')
            suffixes: Tuple of suffixes for overlapping columns
            **kwargs: Additional arguments passed to pandas merge

        Returns:
            New DataFrame with merged data and updated plan
        """
        # Execute the merge in pandas
        result = super().merge(right, on=on, left_on=left_on, right_on=right_on, how=how, suffixes=suffixes, **kwargs)

        # Determine join keys
        if on is not None:
            left_key = on
            right_key = on
        else:
            left_key = left_on if left_on is not None else []
            right_key = right_on if right_on is not None else []

        # Get the right DataFrame's plan
        if isinstance(right, DataFrame):
            right_plan = right._plan.root
        else:
            # If right is a regular pandas DataFrame, create a Source node
            schema = list(right.columns)
            right_plan = Source(source_id="<right_dataframe>", schema=schema)

        # Create new plan with Join operation
        join_op = Join(
            left_on=left_key,
            right_on=right_key,
            join_type=how,
            suffixes=suffixes,
            inputs=[self._plan.root, right_plan]
        )
        new_plan = LogicalPlan(join_op)

        result._plan = new_plan
        return result


    def pivot_table(self, index=None, columns=None, values=None, aggfunc='sum', **kwargs):
        """Reshape from long to wide format (tracked operation).

        Args:
            index: Column(s) to use as the new row labels.
            columns: Column whose unique values become new column headers.
            values: Column whose data fills the pivoted cells.
            aggfunc: Aggregation function for duplicate entries
                     ('sum', 'mean', 'count', 'min', 'max', 'first').
            **kwargs: Extra arguments forwarded to ``pandas.DataFrame.pivot_table``.

        Returns:
            New DataFrame with pivoted data and updated plan.
        """
        result = self.to_pandas().pivot_table(
            index=index, columns=columns, values=values,
            aggfunc=aggfunc, **kwargs,
        ).reset_index()

        # Flatten any MultiIndex columns pandas may create
        if hasattr(result.columns, 'levels'):
            result.columns = [
                str(c) if not isinstance(c, tuple) else '_'.join(str(x) for x in c if x)
                for c in result.columns
            ]

        result = DataFrame(result)

        # Normalize index to list for the algebra node
        if isinstance(index, str):
            index = [index]

        pivot_op = Pivot(
            index=index,
            columns=columns,
            values=values,
            aggfunc=aggfunc if isinstance(aggfunc, str) else 'first',
            inputs=[self._plan.root],
        )
        result._plan = LogicalPlan(pivot_op)
        return result


class DataFrameGroupBy:
    """GroupBy object that tracks aggregation operations.

    This wraps pandas GroupBy to capture aggregation functions and build
    GroupBy nodes in the logical plan.
    """

    def __init__(self, df, by, **kwargs):
        """Initialize GroupBy wrapper.

        Args:
            df: Source DataFrame
            by: Column(s) to group by
            **kwargs: Additional groupby arguments
        """
        self._df = df
        self._by = [by] if isinstance(by, str) else list(by)
        self._groupby = df.to_pandas().groupby(by, **kwargs)

    def agg(self, func=None, **kwargs):
        """Apply aggregation function(s) (tracked operation).

        Args:
            func: Aggregation function or dict mapping columns to functions
            **kwargs: Column -> function mappings

        Returns:
            New DataFrame with aggregated data and updated plan
        """
        # Execute the aggregation in pandas
        result = self._groupby.agg(func, **kwargs)
        result = DataFrame(result.reset_index())

        # Build aggregation tuples
        aggregations = []

        if func is not None:
            if isinstance(func, dict):
                for col, agg_func in func.items():
                    agg_name = agg_func if isinstance(agg_func, str) else "agg"
                    aggregations.append((col, agg_name, col))
            elif isinstance(func, str):
                # Single function applied to all columns
                for col in self._groupby.obj.columns:
                    if col not in self._by:
                        aggregations.append((col, func, col))

        if kwargs:
            for col, agg_func in kwargs.items():
                agg_name = agg_func if isinstance(agg_func, str) else "agg"
                aggregations.append((col, agg_name, col))

        # If no aggregations were captured, create a default one
        if not aggregations:
            # Use first non-groupby column
            cols = [c for c in self._groupby.obj.columns if c not in self._by]
            if cols:
                aggregations.append((cols[0], "first", cols[0]))

        # Create new plan with GroupBy operation
        groupby_op = GroupBy(keys=self._by, aggregations=aggregations, inputs=[self._df._plan.root])
        new_plan = LogicalPlan(groupby_op)

        result._plan = new_plan
        return result

    def sum(self, **kwargs):
        """Compute sum for each group."""
        return self.agg('sum', **kwargs)

    def mean(self, **kwargs):
        """Compute mean for each group."""
        return self.agg('mean', **kwargs)

    def count(self, **kwargs):
        """Compute count for each group."""
        return self.agg('count', **kwargs)

    def min(self, **kwargs):
        """Compute min for each group."""
        return self.agg('min', **kwargs)

    def max(self, **kwargs):
        """Compute max for each group."""
        return self.agg('max', **kwargs)


# Add helper method to convert fornero DataFrame to pandas DataFrame
def _to_pandas(self):
    """Convert to a regular pandas DataFrame (without plan tracking)."""
    return pd.DataFrame(self)

DataFrame.to_pandas = _to_pandas
