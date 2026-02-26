"""
Expression nodes for the dataframe algebra.

This module provides expression representations for use in operations like Filter,
WithColumn, and Window. Expressions are represented as an AST: Column references,
Literal values, BinaryOp / UnaryOp for arithmetic/comparison/logic, and FunctionCall
for built-in functions. A plain-string form (Expression with just `expr`) is retained
for backward compatibility and serialization.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


def _wrap(other: Any) -> "Expression":
    """Promote a plain Python value to a Literal when needed."""
    if isinstance(other, Expression):
        return other
    return Literal(value=other)


@dataclass(eq=False)
class Expression:
    """Base class for all expression types.

    Supports Python operators so you can write ``col("age") > 30`` and get
    back a ``BinaryOp`` AST node.
    """

    expr: str = ""

    def __str__(self) -> str:
        return self.expr

    # Arithmetic
    def __add__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op="+", left=self, right=_wrap(other))

    def __radd__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op="+", left=_wrap(other), right=self)

    def __sub__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op="-", left=self, right=_wrap(other))

    def __rsub__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op="-", left=_wrap(other), right=self)

    def __mul__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op="*", left=self, right=_wrap(other))

    def __rmul__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op="*", left=_wrap(other), right=self)

    def __truediv__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op="/", left=self, right=_wrap(other))

    def __rtruediv__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op="/", left=_wrap(other), right=self)

    def __mod__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op="%", left=self, right=_wrap(other))

    def __neg__(self) -> "UnaryOp":
        return UnaryOp(op="neg", operand=self)

    # Comparison — returns BinaryOp nodes, NOT Python bools
    def __gt__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op=">", left=self, right=_wrap(other))

    def __ge__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op=">=", left=self, right=_wrap(other))

    def __lt__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op="<", left=self, right=_wrap(other))

    def __le__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op="<=", left=self, right=_wrap(other))

    def __eq__(self, other: Any) -> "BinaryOp":  # type: ignore[override]
        return BinaryOp(op="==", left=self, right=_wrap(other))

    def __ne__(self, other: Any) -> "BinaryOp":  # type: ignore[override]
        return BinaryOp(op="!=", left=self, right=_wrap(other))

    # Logical (bitwise operators used as logical, like pandas)
    def __and__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op="and", left=self, right=_wrap(other))

    def __or__(self, other: Any) -> "BinaryOp":
        return BinaryOp(op="or", left=self, right=_wrap(other))

    def __invert__(self) -> "UnaryOp":
        return UnaryOp(op="not", operand=self)

    # Serialization
    def to_dict(self) -> Dict[str, Any]:
        return {"type": "expression", "expr": self.expr}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Expression":
        type_name = data.get("type", "expression")
        type_map: dict[str, type] = {
            "expression": Expression,
            "column": Column,
            "literal": Literal,
            "binary_op": BinaryOp,
            "unary_op": UnaryOp,
            "function_call": FunctionCall,
        }
        target = type_map.get(type_name, Expression)
        if target is Expression:
            return Expression(expr=data.get("expr", ""))
        return target._from_dict(data)  # type: ignore[attr-defined]


@dataclass(eq=False)
class Column(Expression):
    """Reference to a named DataFrame column."""

    name: str = ""

    def __post_init__(self):
        if not self.name and self.expr:
            self.name = self.expr
            self.expr = ""

    def __str__(self) -> str:
        return self.name

    def to_dict(self) -> Dict[str, Any]:
        return {"type": "column", "name": self.name}

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "Column":
        return cls(name=data["name"])


@dataclass(eq=False)
class Literal(Expression):
    """A constant / literal value.

    ``Literal(42)`` and ``Literal(value=42)`` are both accepted.
    """

    value: Any = None

    def __post_init__(self):
        if self.value is None and self.expr != "" and self.expr is not None:
            self.value = self.expr
            self.expr = ""

    def __str__(self) -> str:
        return repr(self.value)

    def to_dict(self) -> Dict[str, Any]:
        return {"type": "literal", "value": self.value}

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "Literal":
        return cls(value=data["value"])


@dataclass(eq=False)
class BinaryOp(Expression):
    """Binary operation (arithmetic, comparison, or logical)."""

    op: str = ""
    left: Expression = field(default_factory=lambda: Expression())
    right: Expression = field(default_factory=lambda: Expression())

    def __str__(self) -> str:
        return f"({self.left} {self.op} {self.right})"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "binary_op",
            "op": self.op,
            "left": self.left.to_dict(),
            "right": self.right.to_dict(),
        }

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "BinaryOp":
        return cls(
            op=data["op"],
            left=Expression.from_dict(data["left"]),
            right=Expression.from_dict(data["right"]),
        )


@dataclass(eq=False)
class UnaryOp(Expression):
    """Unary operation (negation, logical NOT)."""

    op: str = ""
    operand: Expression = field(default_factory=lambda: Expression())

    def __str__(self) -> str:
        return f"({self.op} {self.operand})"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "unary_op",
            "op": self.op,
            "operand": self.operand.to_dict(),
        }

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "UnaryOp":
        return cls(
            op=data["op"],
            operand=Expression.from_dict(data["operand"]),
        )


@dataclass(eq=False)
class FunctionCall(Expression):
    """Application of a named function to argument expressions."""

    func: str = ""
    args: List[Expression] = field(default_factory=list)

    def __str__(self) -> str:
        args_str = ", ".join(str(a) for a in self.args)
        return f"{self.func}({args_str})"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "function_call",
            "func": self.func,
            "args": [a.to_dict() for a in self.args],
        }

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "FunctionCall":
        return cls(
            func=data["func"],
            args=[Expression.from_dict(a) for a in data.get("args", [])],
        )


def col(name: str) -> Column:
    """Create a Column reference expression.

    Example:
        >>> c = col("age")
        >>> pred = c > 30           # BinaryOp(op='>', left=Column('age'), right=Literal(30))
    """
    return Column(name=name)


def expr(s: str) -> Expression:
    """Create an expression from a string.

    Args:
        s: String representation of the expression

    Returns:
        Expression instance

    Example:
        >>> e = expr("age > 25")
        >>> str(e)
        'age > 25'
    """
    return Expression(s)
