"""
Main translator class that converts dataframe algebra trees to spreadsheet operations.

The Translator walks the operation tree, maintains a mapping of operations to their
materialized ranges, and orchestrates the translation strategies.
"""

from dataclasses import dataclass
from typing import Dict, List, Any, Optional
from fornero.algebra.operations import (
    Operation, Source, Select, Filter, Join, GroupBy, Aggregate,
    Sort, Limit, WithColumn, Union, Pivot, Melt, Window
)
from fornero.algebra.logical_plan import LogicalPlan
from fornero.spreadsheet.model import Range
from fornero.spreadsheet.operations import (
    SpreadsheetOp, CreateSheet, SetValues, SetFormula,
)
from fornero.exceptions import UnsupportedOperationError, PlanValidationError
from fornero.translator import strategies


_DICT_TO_OP = {
    "create_sheet": lambda d: CreateSheet(name=d["name"], rows=d["rows"], cols=d["cols"]),
    "set_values": lambda d: SetValues(sheet=d["sheet"], row=d["row"], col=d["col"], values=d["values"]),
    "set_formula": lambda d: SetFormula(sheet=d["sheet"], row=d["row"], col=d["col"],
                                        formula=d["formula"], ref=d.get("ref")),
}


@dataclass
class MaterializationContext:
    """Context for materialized intermediate results.

    Attributes:
        sheet_name: Name of the sheet containing the materialized data
        output_range: Range where the data is located (1-indexed)
        schema: List of column names in the output
    """
    sheet_name: str
    output_range: Range
    schema: List[str]


_DICT_TO_OP = {
    "create_sheet": lambda d: CreateSheet(name=d["name"], rows=d["rows"], cols=d["cols"]),
    "set_values": lambda d: SetValues(sheet=d["sheet"], row=d["row"], col=d["col"], values=d["values"]),
    "set_formula": lambda d: SetFormula(sheet=d["sheet"], row=d["row"], col=d["col"],
                                        formula=d["formula"], ref=d.get("ref")),
}


class Translator:
    """Translates dataframe algebra plans to spreadsheet operations.

    The translator walks the logical plan tree and generates a sequence of spreadsheet
    operations (create sheet, set values, set formulas) that implement the plan.

    Attributes:
        operations: List of spreadsheet operations to execute
        materialized: Mapping from operation to (sheet_name, range, schema)
        counter: Counter for generating unique sheet names
    """

    def __init__(self):
        """Initialize a new Translator."""
        self.operations: List[Dict[str, Any]] = []
        self.materialized: Dict[int, MaterializationContext] = {}
        self.counter = 0

    def translate(self, plan: LogicalPlan, source_data: Optional[Dict[str, Any]] = None) -> List[SpreadsheetOp]:
        """Translate a logical plan to spreadsheet operations.

        Args:
            plan: LogicalPlan to translate
            source_data: Optional mapping of source_id to data (for Source nodes)

        Returns:
            List of SpreadsheetOp dataclass instances (CreateSheet, SetValues, SetFormula)

        Raises:
            UnsupportedOperationError: If plan contains untranslatable operations
            PlanValidationError: If plan structure is invalid
        """
        self.operations = []
        self.materialized = {}
        self.counter = 0

        if source_data is None:
            source_data = {}

        self._translate_operation(plan.root, source_data)

        return [_DICT_TO_OP[op["type"]](op) for op in self.operations]

    def _translate_operation(self, op: Operation, source_data: Dict[str, Any]) -> MaterializationContext:
        """Recursively translate an operation and its inputs.

        Args:
            op: Operation to translate
            source_data: Source data mapping

        Returns:
            MaterializationContext with sheet name, output range, and schema

        Raises:
            UnsupportedOperationError: If operation cannot be translated
            PlanValidationError: If operation structure is invalid
        """
        # Check if already materialized
        op_id = id(op)
        if op_id in self.materialized:
            return self.materialized[op_id]

        # Get input materializations
        input_results = []
        for input_op in op.inputs:
            input_results.append(self._translate_operation(input_op, source_data))

        # Translate based on operation type
        if isinstance(op, Source):
            result = self._translate_source(op, source_data)

        elif isinstance(op, Select):
            if len(input_results) != 1:
                raise PlanValidationError("Select operation must have exactly one input")
            result = self._translate_select(op, input_results[0])

        elif isinstance(op, Filter):
            if len(input_results) != 1:
                raise PlanValidationError("Filter operation must have exactly one input")
            result = self._translate_filter(op, input_results[0])

        elif isinstance(op, Join):
            if len(input_results) != 2:
                raise PlanValidationError("Join operation must have exactly two inputs")
            result = self._translate_join(op, input_results[0], input_results[1])

        elif isinstance(op, GroupBy):
            if len(input_results) != 1:
                raise PlanValidationError("GroupBy operation must have exactly one input")
            result = self._translate_groupby(op, input_results[0])

        elif isinstance(op, Aggregate):
            if len(input_results) != 1:
                raise PlanValidationError("Aggregate operation must have exactly one input")
            result = self._translate_aggregate(op, input_results[0])

        elif isinstance(op, Sort):
            if len(input_results) != 1:
                raise PlanValidationError("Sort operation must have exactly one input")
            result = self._translate_sort(op, input_results[0])

        elif isinstance(op, Limit):
            if len(input_results) != 1:
                raise PlanValidationError("Limit operation must have exactly one input")
            result = self._translate_limit(op, input_results[0])

        elif isinstance(op, WithColumn):
            if len(input_results) != 1:
                raise PlanValidationError("WithColumn operation must have exactly one input")
            result = self._translate_with_column(op, input_results[0])

        elif isinstance(op, Union):
            if len(input_results) != 2:
                raise PlanValidationError("Union operation must have exactly two inputs")
            result = self._translate_union(op, input_results[0], input_results[1])

        elif isinstance(op, Pivot):
            if len(input_results) != 1:
                raise PlanValidationError("Pivot operation must have exactly one input")
            result = self._translate_pivot(op, input_results[0], source_data)

        elif isinstance(op, Melt):
            if len(input_results) != 1:
                raise PlanValidationError("Melt operation must have exactly one input")
            result = self._translate_melt(op, input_results[0])

        elif isinstance(op, Window):
            if len(input_results) != 1:
                raise PlanValidationError("Window operation must have exactly one input")
            result = self._translate_window(op, input_results[0])

        else:
            raise UnsupportedOperationError(f"Unknown operation type: {type(op).__name__}")

        # Cache the result
        self.materialized[op_id] = result

        return result

    def _translate_source(self, op: Source, source_data: Dict[str, Any]) -> MaterializationContext:
        """Translate a Source operation."""
        data = source_data.get(op.source_id, [])

        ops, sheet_name, output_range = strategies.translate_source(op, self.counter, data)
        self.counter += 1

        self.operations.extend(ops)

        return MaterializationContext(sheet_name, output_range, op.schema or [])

    def _translate_select(self, op: Select, input_ctx: MaterializationContext) -> MaterializationContext:
        """Translate a Select operation."""
        ops, sheet_name, output_range = strategies.translate_select(
            op, self.counter, input_ctx.sheet_name, input_ctx.output_range, input_ctx.schema
        )
        self.counter += 1

        self.operations.extend(ops)

        return MaterializationContext(sheet_name, output_range, op.columns)

    def _translate_filter(self, op: Filter, input_ctx: MaterializationContext) -> MaterializationContext:
        """Translate a Filter operation."""
        ops, sheet_name, output_range = strategies.translate_filter(
            op, self.counter, input_ctx.sheet_name, input_ctx.output_range, input_ctx.schema
        )
        self.counter += 1

        self.operations.extend(ops)

        return MaterializationContext(sheet_name, output_range, input_ctx.schema)

    def _translate_join(self, op: Join, left_ctx: MaterializationContext,
                       right_ctx: MaterializationContext) -> MaterializationContext:
        """Translate a Join operation."""
        ops, sheet_name, output_range = strategies.translate_join(
            op, self.counter,
            left_ctx.sheet_name, left_ctx.output_range, left_ctx.schema,
            right_ctx.sheet_name, right_ctx.output_range, right_ctx.schema
        )
        self.counter += 1

        self.operations.extend(ops)

        right_keys = set(op.right_on) if isinstance(op.right_on, list) else {op.right_on}
        output_schema = left_ctx.schema.copy()
        for col in right_ctx.schema:
            if col not in right_keys:
                output_schema.append(col)

        return MaterializationContext(sheet_name, output_range, output_schema)

    def _translate_groupby(self, op: GroupBy, input_ctx: MaterializationContext) -> MaterializationContext:
        """Translate a GroupBy operation."""
        ops, sheet_name, output_range = strategies.translate_groupby(
            op, self.counter, input_ctx.sheet_name, input_ctx.output_range, input_ctx.schema
        )
        self.counter += 1

        self.operations.extend(ops)

        # Output schema: keys + aggregation outputs
        output_schema = op.keys.copy()
        for agg_name, _, _ in op.aggregations:
            output_schema.append(agg_name)

        return MaterializationContext(sheet_name, output_range, output_schema)

    def _translate_aggregate(self, op: Aggregate, input_ctx: MaterializationContext) -> MaterializationContext:
        """Translate an Aggregate operation."""
        ops, sheet_name, output_range = strategies.translate_aggregate(
            op, self.counter, input_ctx.sheet_name, input_ctx.output_range, input_ctx.schema
        )
        self.counter += 1

        self.operations.extend(ops)

        # Output schema: aggregation output names
        output_schema = [agg_name for agg_name, _, _ in op.aggregations]

        return MaterializationContext(sheet_name, output_range, output_schema)

    def _translate_sort(self, op: Sort, input_ctx: MaterializationContext) -> MaterializationContext:
        """Translate a Sort operation."""
        ops, sheet_name, output_range = strategies.translate_sort(
            op, self.counter, input_ctx.sheet_name, input_ctx.output_range, input_ctx.schema
        )
        self.counter += 1

        self.operations.extend(ops)

        return MaterializationContext(sheet_name, output_range, input_ctx.schema)

    def _translate_limit(self, op: Limit, input_ctx: MaterializationContext) -> MaterializationContext:
        """Translate a Limit operation."""
        ops, sheet_name, output_range = strategies.translate_limit(
            op, self.counter, input_ctx.sheet_name, input_ctx.output_range, input_ctx.schema
        )
        self.counter += 1

        self.operations.extend(ops)

        return MaterializationContext(sheet_name, output_range, input_ctx.schema)

    def _translate_with_column(self, op: WithColumn, input_ctx: MaterializationContext) -> MaterializationContext:
        """Translate a WithColumn operation."""
        ops, sheet_name, output_range = strategies.translate_with_column(
            op, self.counter, input_ctx.sheet_name, input_ctx.output_range, input_ctx.schema
        )
        self.counter += 1

        self.operations.extend(ops)

        # Output schema: existing columns + new/replaced column
        if op.column in input_ctx.schema:
            output_schema = input_ctx.schema.copy()
        else:
            output_schema = input_ctx.schema + [op.column]

        return MaterializationContext(sheet_name, output_range, output_schema)

    def _translate_union(self, op: Union, left_ctx: MaterializationContext,
                        right_ctx: MaterializationContext) -> MaterializationContext:
        """Translate a Union operation."""
        ops, sheet_name, output_range = strategies.translate_union(
            op, self.counter,
            left_ctx.sheet_name, left_ctx.output_range, left_ctx.schema,
            right_ctx.sheet_name, right_ctx.output_range, right_ctx.schema
        )
        self.counter += 1

        self.operations.extend(ops)

        return MaterializationContext(sheet_name, output_range, left_ctx.schema)

    def _translate_pivot(self, op: Pivot, input_ctx: MaterializationContext,
                         source_data: Dict[str, Any]) -> MaterializationContext:
        """Translate a Pivot operation."""
        num_pivot_values = self._count_distinct_pivot_values(op, source_data)
        num_index_values = self._count_distinct_index_values(op, source_data)

        ops, sheet_name, output_range = strategies.translate_pivot(
            op, self.counter, input_ctx.sheet_name, input_ctx.output_range, input_ctx.schema,
            num_pivot_values=num_pivot_values,
            num_index_values=num_index_values,
        )
        self.counter += 1

        self.operations.extend(ops)

        # Pivot output schema is dynamic - placeholder
        output_schema = []

        return MaterializationContext(sheet_name, output_range, output_schema)

    @staticmethod
    def _count_distinct_pivot_values(op: Pivot, source_data: Dict[str, Any]) -> Optional[int]:
        """Walk the input chain to find the number of distinct pivot column values."""
        pivot_col = op.columns
        current = op.inputs[0] if op.inputs else None
        while current is not None:
            if isinstance(current, Source):
                data = source_data.get(current.source_id, [])
                if data and pivot_col in current.schema:
                    col_idx = current.schema.index(pivot_col)
                    distinct = {row[col_idx] for row in data}
                    return len(distinct)
                break
            current = current.inputs[0] if current.inputs else None
        return None

    @staticmethod
    def _count_distinct_index_values(op: Pivot, source_data: Dict[str, Any]) -> Optional[int]:
        """Walk the input chain to find the number of distinct index column values."""
        index_col = op.index[0] if isinstance(op.index, list) else op.index
        current = op.inputs[0] if op.inputs else None
        while current is not None:
            if isinstance(current, Source):
                data = source_data.get(current.source_id, [])
                if data and index_col in current.schema:
                    col_idx = current.schema.index(index_col)
                    distinct = {row[col_idx] for row in data}
                    return len(distinct)
                break
            current = current.inputs[0] if current.inputs else None
        return None

    def _translate_melt(self, op: Melt, input_ctx: MaterializationContext) -> MaterializationContext:
        """Translate a Melt operation."""
        ops, sheet_name, output_range = strategies.translate_melt(
            op, self.counter, input_ctx.sheet_name, input_ctx.output_range, input_ctx.schema
        )
        self.counter += 1

        self.operations.extend(ops)

        # Melt output schema: id_vars + var_name + value_name
        output_schema = op.id_vars + [op.var_name, op.value_name]

        return MaterializationContext(sheet_name, output_range, output_schema)

    def _translate_window(self, op: Window, input_ctx: MaterializationContext) -> MaterializationContext:
        """Translate a Window operation."""
        ops, sheet_name, output_range = strategies.translate_window(
            op, self.counter, input_ctx.sheet_name, input_ctx.output_range, input_ctx.schema
        )
        self.counter += 1

        self.operations.extend(ops)

        # Window output schema: all columns + output_column
        output_schema = input_ctx.schema + [op.output_column]

        return MaterializationContext(sheet_name, output_range, output_schema)
