"""
Dataframe algebra module.

This module defines the intermediate representation (IR) for dataframe operations.
The algebra provides a composable, serializable representation of transformations
that can be analyzed, optimized, and translated to different execution targets.

Key components:
- LogicalPlan: Container for the operation tree
- Operation classes: Source, Select, Filter, Join, GroupBy, Aggregate, Sort, Limit,
                     WithColumn, Union, Pivot, Melt, Window
- Expression AST: Column, Literal, BinaryOp, UnaryOp, FunctionCall
"""

from .logical_plan import LogicalPlan
from .operations import (
    Operation,
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
    Pivot,
    Melt,
    Window,
    JoinType,
    SortDirection,
    LimitEnd,
)
from .expressions import (
    Expression,
    Column,
    Literal,
    BinaryOp,
    UnaryOp,
    FunctionCall,
    col,
    expr,
)
from .eager import execute

__all__ = [
    "LogicalPlan",
    "Operation",
    "Source",
    "Select",
    "Filter",
    "Join",
    "GroupBy",
    "Aggregate",
    "Sort",
    "Limit",
    "WithColumn",
    "Union",
    "Pivot",
    "Melt",
    "Window",
    "JoinType",
    "SortDirection",
    "LimitEnd",
    "Expression",
    "Column",
    "Literal",
    "BinaryOp",
    "UnaryOp",
    "FunctionCall",
    "col",
    "expr",
    "execute",
]
