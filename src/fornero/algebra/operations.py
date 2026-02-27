"""
Operation nodes for the dataframe algebra.

Each operation is a node in the logical plan tree. Operations are immutable dataclasses
that capture the intent of a transformation without executing it.

Constructor shortcuts
---------------------
Unary operations accept ``input=<op>`` as shorthand for ``inputs=[<op>]``.
Binary operations (Join, Union) accept ``left=`` / ``right=`` as shorthand for
``inputs=[left, right]``.  Additional aliases (e.g. ``n`` for ``count``,
``how`` for ``join_type``) are listed per-class.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Tuple
from enum import Enum

import pandas as pd


class JoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    OUTER = "outer"


class SortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


class LimitEnd(str, Enum):
    HEAD = "head"
    TAIL = "tail"


def _resolve_inputs(
    inputs: List["Operation"],
    *,
    input: Optional["Operation"] = None,
    left: Optional["Operation"] = None,
    right: Optional["Operation"] = None,
) -> List["Operation"]:
    """Build the inputs list from explicit inputs or convenience aliases."""
    if inputs:
        return inputs
    if left is not None or right is not None:
        result: list[Operation] = []
        if left is not None:
            result.append(left)
        if right is not None:
            result.append(right)
        return result
    if input is not None:
        return [input]
    return []


@dataclass
class Operation:
    """Base class for all operations."""

    inputs: List["Operation"] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        raise NotImplementedError(
            f"to_dict not implemented for {self.__class__.__name__}"
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Operation":
        op_type = data.get("type")
        if not op_type:
            raise ValueError("Operation dict must have 'type' field")

        type_map = {
            "source": Source,
            "select": Select,
            "filter": Filter,
            "join": Join,
            "groupby": GroupBy,
            "aggregate": Aggregate,
            "sort": Sort,
            "limit": Limit,
            "with_column": WithColumn,
            "union": Union,
            "pivot": Pivot,
            "melt": Melt,
            "window": Window,
        }

        op_class = type_map.get(op_type)
        if not op_class:
            raise ValueError(f"Unknown operation type: {op_type}")

        inputs_data = data.get("inputs", [])
        if not inputs_data:
            single = data.get("input")
            if single is not None:
                inputs_data = [single] if isinstance(single, dict) else single
        if isinstance(inputs_data, dict):
            inputs_data = [inputs_data]
        inputs = [Operation.from_dict(inp) for inp in inputs_data]

        kwargs = {k: v for k, v in data.items() if k not in ("type", "inputs", "input")}
        kwargs["inputs"] = inputs

        if op_type in ("groupby", "aggregate"):
            if "aggregations" in kwargs:
                kwargs["aggregations"] = [tuple(agg) for agg in kwargs["aggregations"]]
        elif op_type == "sort":
            if "keys" in kwargs:
                kwargs["keys"] = [tuple(key) for key in kwargs["keys"]]
        elif op_type == "window":
            if "order_by" in kwargs:
                kwargs["order_by"] = [tuple(key) for key in kwargs["order_by"]]
        elif op_type == "join":
            if "suffixes" in kwargs:
                kwargs["suffixes"] = tuple(kwargs["suffixes"])

        return op_class(**kwargs)


@dataclass
class Source(Operation):
    """Data source — always a leaf node.

    Aliases: ``name`` → ``source_id``.
    """

    source_id: str = ""
    schema: Optional[List[str]] = None
    data: Optional[pd.DataFrame] = field(default=None, repr=False)
    name: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        if self.name is not None and not self.source_id:
            self.source_id = self.name
        if self.inputs:
            raise ValueError("Source operation cannot have inputs")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "source",
            "source_id": self.source_id,
            "schema": self.schema,
            "inputs": [],
        }


@dataclass
class Select(Operation):
    """Column projection.

    Aliases: ``input`` → ``inputs[0]``.
    """

    columns: List[str] = field(default_factory=list)
    predicate: Any = None  # Optional filter predicate pushed down
    input: Optional[Operation] = field(default=None, repr=False)

    def __post_init__(self):
        self.inputs = _resolve_inputs(self.inputs, input=self.input)
        self.input = None
        if len(self.inputs) != 1:
            raise ValueError("Select operation must have exactly one input")
        if not self.columns:
            raise ValueError("Select operation must specify at least one column")

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "type": "select",
            "columns": self.columns,
            "input": self.inputs[0].to_dict(),
        }
        if self.predicate is not None:
            pred = self.predicate
            if hasattr(pred, "to_dict"):
                pred = pred.to_dict()
            result["predicate"] = pred
        return result


@dataclass
class Filter(Operation):
    """Row filtering.

    Aliases: ``input`` → ``inputs[0]``.
    """

    predicate: Any = ""
    input: Optional[Operation] = field(default=None, repr=False)

    def __post_init__(self):
        self.inputs = _resolve_inputs(self.inputs, input=self.input)
        self.input = None
        if len(self.inputs) != 1:
            raise ValueError("Filter operation must have exactly one input")
        if not self.predicate and self.predicate != 0:
            raise ValueError("Filter operation must specify a predicate")

    def to_dict(self) -> Dict[str, Any]:
        pred = self.predicate
        if hasattr(pred, "to_dict"):
            pred = pred.to_dict()
        return {
            "type": "filter",
            "predicate": pred,
            "input": self.inputs[0].to_dict(),
        }


@dataclass
class Join(Operation):
    """Equi-join.

    Aliases: ``left`` / ``right`` → ``inputs[0]`` / ``inputs[1]``,
    ``left_key`` → ``left_on``, ``right_key`` → ``right_on``,
    ``how`` → ``join_type``.
    """

    left_on: str | list[str] = ""
    right_on: str | list[str] = ""
    join_type: str = "inner"
    suffixes: Tuple[str, str] = ("_x", "_y")
    left: Optional[Operation] = field(default=None, repr=False)
    right: Optional[Operation] = field(default=None, repr=False)
    left_key: str | list[str] | None = field(default=None, repr=False)
    right_key: str | list[str] | None = field(default=None, repr=False)
    how: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        self.inputs = _resolve_inputs(self.inputs, left=self.left, right=self.right)
        self.left = None
        self.right = None
        if self.left_key is not None and not self.left_on:
            self.left_on = self.left_key
        if self.right_key is not None and not self.right_on:
            self.right_on = self.right_key
        if self.how is not None and self.join_type == "inner":
            self.join_type = self.how
        self.left_key = None
        self.right_key = None
        self.how = None

        if len(self.inputs) != 2:
            raise ValueError("Join operation must have exactly two inputs")
        if isinstance(self.left_on, str):
            self.left_on = [self.left_on] if self.left_on else []
        if isinstance(self.right_on, str):
            self.right_on = [self.right_on] if self.right_on else []
        if not self.left_on or not self.right_on:
            raise ValueError("Join operation must specify join keys")
        valid_types = {"inner", "left", "right", "outer"}
        if self.join_type not in valid_types:
            raise ValueError(f"Join type must be one of {valid_types}, got: {self.join_type}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "join",
            "left_on": self.left_on if isinstance(self.left_on, list) else [self.left_on],
            "right_on": self.right_on if isinstance(self.right_on, list) else [self.right_on],
            "join_type": self.join_type,
            "suffixes": list(self.suffixes),
            "inputs": [inp.to_dict() for inp in self.inputs],
        }


@dataclass
class GroupBy(Operation):
    """Partitioned aggregation.

    Aliases: ``input`` → ``inputs[0]``.
    """

    keys: List[str] = field(default_factory=list)
    aggregations: List[Tuple[str, str, str]] = field(default_factory=list)
    sort_keys: Optional[List[Tuple[str, str]]] = None  # Optional sort pushed down
    limit: Optional[int] = None  # Optional limit pushed down
    input: Optional[Operation] = field(default=None, repr=False)

    def __post_init__(self):
        self.inputs = _resolve_inputs(self.inputs, input=self.input)
        self.input = None
        if len(self.inputs) != 1:
            raise ValueError("GroupBy operation must have exactly one input")
        if not self.aggregations:
            raise ValueError("GroupBy operation must specify at least one aggregation")
        if self.sort_keys:
            for col, direction in self.sort_keys:
                if direction not in ("asc", "desc"):
                    raise ValueError(f"Sort direction must be 'asc' or 'desc', got: {direction}")

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "type": "groupby",
            "keys": self.keys,
            "aggregations": [list(agg) for agg in self.aggregations],
            "input": self.inputs[0].to_dict(),
        }
        if self.sort_keys:
            result["sort_keys"] = [list(key) for key in self.sort_keys]
        if self.limit is not None:
            result["limit"] = self.limit
        return result


@dataclass
class Aggregate(Operation):
    """Global aggregation (GroupBy with empty keys).

    Aliases: ``input`` → ``inputs[0]``.
    """

    aggregations: List[Tuple[str, str, str]] = field(default_factory=list)
    input: Optional[Operation] = field(default=None, repr=False)

    def __post_init__(self):
        self.inputs = _resolve_inputs(self.inputs, input=self.input)
        self.input = None
        if len(self.inputs) != 1:
            raise ValueError("Aggregate operation must have exactly one input")
        if not self.aggregations:
            raise ValueError("Aggregate operation must specify at least one aggregation")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "aggregate",
            "aggregations": [list(agg) for agg in self.aggregations],
            "input": self.inputs[0].to_dict(),
        }


@dataclass
class Sort(Operation):
    """Row reordering.

    Aliases: ``input`` → ``inputs[0]``.
    """

    keys: List[Tuple[str, str]] = field(default_factory=list)
    limit: Optional[int] = None
    predicate: Any = None
    input: Optional[Operation] = field(default=None, repr=False)

    def __post_init__(self):
        self.inputs = _resolve_inputs(self.inputs, input=self.input)
        self.input = None
        if len(self.inputs) != 1:
            raise ValueError("Sort operation must have exactly one input")
        if not self.keys:
            raise ValueError("Sort operation must specify at least one sort key")
        for col, direction in self.keys:
            if direction not in ("asc", "desc"):
                raise ValueError(f"Sort direction must be 'asc' or 'desc', got: {direction}")
        if self.limit is not None and self.limit < 0:
            raise ValueError("Sort limit must be non-negative")

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "type": "sort",
            "keys": [list(key) for key in self.keys],
            "input": self.inputs[0].to_dict(),
        }
        if self.limit is not None:
            result["limit"] = self.limit
        if self.predicate is not None:
            pred = self.predicate
            if hasattr(pred, "to_dict"):
                pred = pred.to_dict()
            result["predicate"] = pred
        return result


@dataclass
class Limit(Operation):
    """Row truncation.

    Aliases: ``input`` → ``inputs[0]``, ``n`` → ``count``.
    """

    count: int = 0
    end: str = "head"
    input: Optional[Operation] = field(default=None, repr=False)
    n: Optional[int] = field(default=None, repr=False)

    def __post_init__(self):
        self.inputs = _resolve_inputs(self.inputs, input=self.input)
        self.input = None
        if self.n is not None and self.count == 0:
            self.count = self.n
        self.n = None
        if len(self.inputs) != 1:
            raise ValueError("Limit operation must have exactly one input")
        if self.count < 0:
            raise ValueError("Limit count must be non-negative")
        if self.end not in ("head", "tail"):
            raise ValueError(f"Limit end must be 'head' or 'tail', got: {self.end}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "limit",
            "count": self.count,
            "end": self.end,
            "input": self.inputs[0].to_dict(),
        }


@dataclass
class WithColumn(Operation):
    """Add or replace a column.

    Aliases: ``input`` → ``inputs[0]``, ``column_name`` → ``column``.
    """

    column: str = ""
    expression: Any = ""
    input: Optional[Operation] = field(default=None, repr=False)
    column_name: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        self.inputs = _resolve_inputs(self.inputs, input=self.input)
        self.input = None
        if self.column_name is not None and not self.column:
            self.column = self.column_name
        self.column_name = None
        if len(self.inputs) != 1:
            raise ValueError("WithColumn operation must have exactly one input")
        if not self.column:
            raise ValueError("WithColumn operation must specify a column name")
        if not self.expression and self.expression != 0:
            raise ValueError("WithColumn operation must specify an expression")

    def to_dict(self) -> Dict[str, Any]:
        expr = self.expression
        if hasattr(expr, "to_dict"):
            expr = expr.to_dict()
        return {
            "type": "with_column",
            "column": self.column,
            "expression": expr,
            "input": self.inputs[0].to_dict(),
        }


@dataclass
class Union(Operation):
    """Vertical concatenation of two relations.

    Aliases: ``left`` / ``right`` → ``inputs[0]`` / ``inputs[1]``.
    """

    left: Optional[Operation] = field(default=None, repr=False)
    right: Optional[Operation] = field(default=None, repr=False)

    def __post_init__(self):
        self.inputs = _resolve_inputs(self.inputs, left=self.left, right=self.right)
        self.left = None
        self.right = None
        if len(self.inputs) != 2:
            raise ValueError("Union operation must have exactly two inputs")

    def to_dict(self) -> Dict[str, Any]:
        return {"type": "union", "inputs": [inp.to_dict() for inp in self.inputs]}


@dataclass
class Pivot(Operation):
    """Long-to-wide reshaping.

    Aliases: ``input`` → ``inputs[0]``, ``pivot_column`` → ``columns``,
    ``values_column`` → ``values``.
    """

    index: str | list[str] = ""
    columns: str = ""
    values: str = ""
    aggfunc: str = "first"
    input: Optional[Operation] = field(default=None, repr=False)
    pivot_column: Optional[str] = field(default=None, repr=False)
    values_column: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        self.inputs = _resolve_inputs(self.inputs, input=self.input)
        self.input = None
        if self.pivot_column and not self.columns:
            self.columns = self.pivot_column
        if self.values_column and not self.values:
            self.values = self.values_column
        self.pivot_column = None
        self.values_column = None

        if len(self.inputs) != 1:
            raise ValueError("Pivot operation must have exactly one input")
        if isinstance(self.index, str):
            self.index = [self.index] if self.index else []
        if not self.index:
            raise ValueError("Pivot operation must specify index column(s)")
        if not self.columns:
            raise ValueError("Pivot operation must specify columns")
        if not self.values:
            raise ValueError("Pivot operation must specify values")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "pivot",
            "index": self.index if isinstance(self.index, list) else [self.index],
            "columns": self.columns,
            "values": self.values,
            "aggfunc": self.aggfunc,
            "input": self.inputs[0].to_dict(),
        }


@dataclass
class Melt(Operation):
    """Wide-to-long reshaping (unpivot).

    Aliases: ``input`` → ``inputs[0]``.
    """

    id_vars: List[str] = field(default_factory=list)
    value_vars: Optional[List[str]] = None
    var_name: str = "variable"
    value_name: str = "value"
    input: Optional[Operation] = field(default=None, repr=False)

    def __post_init__(self):
        self.inputs = _resolve_inputs(self.inputs, input=self.input)
        self.input = None
        if len(self.inputs) != 1:
            raise ValueError("Melt operation must have exactly one input")
        if not self.id_vars:
            raise ValueError("Melt operation must specify id_vars")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "melt",
            "id_vars": self.id_vars,
            "value_vars": self.value_vars,
            "var_name": self.var_name,
            "value_name": self.value_name,
            "input": self.inputs[0].to_dict(),
        }


@dataclass
class Window(Operation):
    """Windowed computation.

    Aliases: ``input`` → ``inputs[0]``, ``func`` → ``function``,
    ``input_col`` → ``input_column``, ``output_col`` → ``output_column``.
    """

    function: str = ""
    input_column: Optional[str] = None
    output_column: str = ""
    partition_by: List[str] = field(default_factory=list)
    order_by: List[Tuple[str, str]] = field(default_factory=list)
    frame: Optional[str] = None
    input: Optional[Operation] = field(default=None, repr=False)
    func: Optional[str] = field(default=None, repr=False)
    input_col: Optional[str] = field(default=None, repr=False)
    output_col: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        self.inputs = _resolve_inputs(self.inputs, input=self.input)
        self.input = None
        if self.func is not None and not self.function:
            self.function = self.func
        if self.input_col is not None and self.input_column is None:
            self.input_column = self.input_col
        if self.output_col is not None and not self.output_column:
            self.output_column = self.output_col
        self.func = None
        self.input_col = None
        self.output_col = None

        if len(self.inputs) != 1:
            raise ValueError("Window operation must have exactly one input")
        if not self.function:
            raise ValueError("Window operation must specify a function")
        if not self.output_column:
            raise ValueError("Window operation must specify an output column")
        for _col, direction in self.order_by:
            if direction not in ("asc", "desc"):
                raise ValueError(f"Window order direction must be 'asc' or 'desc', got: {direction}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "window",
            "function": self.function,
            "input_column": self.input_column,
            "output_column": self.output_column,
            "partition_by": self.partition_by,
            "order_by": [list(key) for key in self.order_by],
            "frame": self.frame,
            "input": self.inputs[0].to_dict(),
        }
