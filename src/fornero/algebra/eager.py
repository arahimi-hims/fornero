"""Eager executor: evaluates an operation tree bottom-up using pandas.

This is the "dual-mode" eager path — every operation produces a concrete
pandas DataFrame so users can inspect intermediate results, while the
operation tree is simultaneously available for later translation.
"""

from __future__ import annotations

import operator
from typing import Any

import numpy as np
import pandas as pd

from fornero.algebra.expressions import (
    BinaryOp,
    Column,
    Expression,
    FunctionCall,
    Literal,
    UnaryOp,
)
from fornero.algebra.operations import (
    Aggregate,
    Filter,
    GroupBy,
    Join,
    Limit,
    Melt,
    Operation,
    Pivot,
    Select,
    Sort,
    Source,
    Union,
    Window,
    WithColumn,
)

_COMPARE_OPS: dict[str, Any] = {
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
}

_ARITH_OPS: dict[str, Any] = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "/": operator.truediv,
    "%": operator.mod,
}

_AGG_FUNCS: dict[str, str] = {
    "sum": "sum",
    "mean": "mean",
    "count": "count",
    "min": "min",
    "max": "max",
    "first": "first",
    "last": "last",
    "std": "std",
    "var": "var",
    "median": "median",
}

_BUILTIN_FUNCS: dict[str, Any] = {
    "abs": np.abs,
    "round": np.round,
    "sqrt": np.sqrt,
    "log": np.log,
    "exp": np.exp,
    "floor": np.floor,
    "ceil": np.ceil,
}


def evaluate_expression(expr: Expression, df: pd.DataFrame) -> pd.Series | Any:
    """Evaluate an Expression AST against a DataFrame, producing a Series."""
    match expr:
        case Column(name=name):
            return df[name]

        case Literal(value=value):
            return value

        case BinaryOp(op=op, left=left, right=right):
            lval = evaluate_expression(left, df)
            rval = evaluate_expression(right, df)

            if op in _COMPARE_OPS:
                return _COMPARE_OPS[op](lval, rval)
            if op in _ARITH_OPS:
                return _ARITH_OPS[op](lval, rval)
            if op == "and":
                return lval & rval
            if op == "or":
                return lval | rval
            raise ValueError(f"Unknown binary operator: {op!r}")

        case UnaryOp(op="neg", operand=operand):
            return -evaluate_expression(operand, df)

        case UnaryOp(op="not", operand=operand):
            return ~evaluate_expression(operand, df)

        case FunctionCall(func=func, args=args):
            evaluated_args = [evaluate_expression(a, df) for a in args]
            if func not in _BUILTIN_FUNCS:
                raise ValueError(f"Unknown function: {func!r}")
            return _BUILTIN_FUNCS[func](*evaluated_args)

        case _:
            raise TypeError(f"Unknown expression type: {type(expr).__name__}")


def execute(op: Operation) -> pd.DataFrame:
    """Recursively execute an operation tree, returning a pandas DataFrame."""
    match op:
        case Source(data=data):
            if data is None:
                raise ValueError(
                    "Source operation has no data for eager execution. "
                    "Set Source.data to a DataFrame before calling execute()."
                )
            return data.copy()

        case Select(columns=columns, inputs=[child]):
            df = execute(child)
            return df[columns].reset_index(drop=True)

        case Filter(predicate=predicate, inputs=[child]):
            df = execute(child)
            mask = evaluate_expression(predicate, df)
            return df.loc[mask].reset_index(drop=True)

        case Sort(keys=keys, inputs=[child]):
            df = execute(child)
            cols = [k[0] for k in keys]
            ascending = [k[1] == "asc" for k in keys]
            return df.sort_values(
                cols, ascending=ascending, kind="mergesort"
            ).reset_index(drop=True)

        case Limit(count=n, end=end, inputs=[child]):
            df = execute(child)
            if end == "head":
                return df.head(n).reset_index(drop=True)
            return df.tail(n).reset_index(drop=True)

        case WithColumn(column=col_name, expression=expr, inputs=[child]):
            df = execute(child)
            if isinstance(expr, Expression) and not isinstance(expr, str):
                df = df.copy()
                df[col_name] = evaluate_expression(expr, df)
                return df
            raise ValueError(
                f"Cannot eagerly evaluate string expression: {expr!r}. "
                "Use an Expression AST for eager execution."
            )

        case GroupBy(keys=keys, aggregations=aggs, inputs=[child]):
            df = execute(child)
            grouped = df.groupby(keys, sort=False)
            named_aggs = {
                out_name: pd.NamedAgg(
                    column=in_col, aggfunc=_AGG_FUNCS.get(func, func)
                )
                for out_name, func, in_col in aggs
            }
            return grouped.agg(**named_aggs).reset_index()

        case Aggregate(aggregations=aggs, inputs=[child]):
            df = execute(child)
            row: dict[str, Any] = {}
            for out_name, func, in_col in aggs:
                pandas_func = _AGG_FUNCS.get(func, func)
                row[out_name] = df[in_col].agg(pandas_func)
            return pd.DataFrame([row])

        case Join(
            left_on=lk, right_on=rk, join_type=how, suffixes=suffixes,
            inputs=[left, right],
        ):
            ldf = execute(left)
            rdf = execute(right)
            return ldf.merge(
                rdf, left_on=lk, right_on=rk, how=how, suffixes=suffixes
            ).reset_index(drop=True)

        case Union(inputs=[left, right]):
            ldf = execute(left)
            rdf = execute(right)
            if list(ldf.columns) != list(rdf.columns):
                raise ValueError(
                    f"Union requires identical schemas, got {list(ldf.columns)} "
                    f"and {list(rdf.columns)}"
                )
            return pd.concat([ldf, rdf], ignore_index=True)

        case Pivot(
            index=index, columns=pc, values=vc, aggfunc=af, inputs=[child],
        ):
            df = execute(child)
            result = df.pivot_table(
                index=index,
                columns=pc,
                values=vc,
                aggfunc=af,
            )
            result.columns = result.columns.astype(str)
            result = result.reset_index()
            result.columns.name = None
            return result

        case Melt(
            id_vars=id_vars, value_vars=value_vars,
            var_name=var_name, value_name=value_name, inputs=[child],
        ):
            df = execute(child)
            return pd.melt(
                df,
                id_vars=id_vars,
                value_vars=value_vars,
                var_name=var_name,
                value_name=value_name,
            ).reset_index(drop=True)

        case Window(
            partition_by=partition_by,
            order_by=order_by,
            function=func,
            input_column=input_col,
            output_column=output_col,
            frame=frame,
            inputs=[child],
        ):
            df = execute(child)
            return _execute_window(
                df, partition_by, order_by, func, input_col, output_col, frame
            )

        case _:
            raise TypeError(f"Unknown operation type: {type(op).__name__}")


def _execute_window(
    df: pd.DataFrame,
    partition_by: list[str],
    order_by: list[tuple[str, str]],
    func: str,
    input_col: str | None,
    output_col: str,
    frame: str | None,
) -> pd.DataFrame:
    """Execute a window function, preserving original row order."""
    df = df.copy()

    if order_by:
        sort_cols = [o[0] for o in order_by]
        sort_asc = [o[1] == "asc" for o in order_by]
        df = df.sort_values(sort_cols, ascending=sort_asc, kind="mergesort")

    if partition_by and input_col:
        grouped = df.groupby(partition_by, sort=False)[input_col]
    elif input_col:
        grouped = df[input_col]
    else:
        grouped = None

    _is_running = frame == "unbounded preceding to current row" or (
        frame is None and bool(order_by)
    )

    match func:
        case "sum":
            if _is_running:
                result = grouped.expanding().sum()
                if partition_by:
                    result = result.reset_index(level=0, drop=True)
                df[output_col] = result
            else:
                df[output_col] = grouped.transform("sum")

        case "cumsum":
            df[output_col] = grouped.cumsum()

        case "rank":
            if partition_by:
                df[output_col] = grouped.rank(method="min").astype(int)
            else:
                df[output_col] = df[input_col].rank(method="min").astype(int)

        case "row_number":
            if partition_by:
                df[output_col] = grouped.cumcount() + 1
            else:
                df[output_col] = range(1, len(df) + 1)

        case "min":
            if _is_running:
                result = grouped.expanding().min()
                if partition_by:
                    result = result.reset_index(level=0, drop=True)
                df[output_col] = result
            else:
                df[output_col] = grouped.transform("min")

        case "max":
            if _is_running:
                result = grouped.expanding().max()
                if partition_by:
                    result = result.reset_index(level=0, drop=True)
                df[output_col] = result
            else:
                df[output_col] = grouped.transform("max")

        case "mean":
            if _is_running:
                result = grouped.expanding().mean()
                if partition_by:
                    result = result.reset_index(level=0, drop=True)
                df[output_col] = result
            else:
                df[output_col] = grouped.transform("mean")

        case "count":
            if _is_running:
                result = grouped.expanding().count().astype(int)
                if partition_by:
                    result = result.reset_index(level=0, drop=True)
                df[output_col] = result
            else:
                df[output_col] = grouped.transform("count").astype(int)

        case "lag":
            offset = _parse_offset(frame)
            if partition_by:
                df[output_col] = df.groupby(partition_by, sort=False)[input_col].shift(offset)
            else:
                df[output_col] = df[input_col].shift(offset)

        case "lead":
            offset = _parse_offset(frame)
            if partition_by:
                df[output_col] = df.groupby(partition_by, sort=False)[input_col].shift(-offset)
            else:
                df[output_col] = df[input_col].shift(-offset)

        case _:
            raise ValueError(f"Unsupported window function: {func!r}")

    return df.sort_index().reset_index(drop=True)


def _parse_offset(frame: str | None) -> int:
    """Extract an integer offset from a frame spec string. Defaults to 1."""
    if frame is None:
        return 1
    try:
        return int(frame)
    except (ValueError, TypeError):
        return 1
