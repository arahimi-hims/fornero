"""
Translation strategies for converting dataframe algebra operations to spreadsheet formulas.

Each strategy function takes an operation node and its input ranges, and produces a sequence
of spreadsheet operations (CreateSheet, SetValues, SetFormula) along with the output range.

All strategies operate purely on the plan structure and never inspect actual data values.
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Any, Set
from fornero.algebra.operations import (
    Operation, Source, Select, Filter, Join, GroupBy, Aggregate,
    Sort, Limit, WithColumn, Union, Pivot, Melt, Window
)
from fornero.algebra.expressions import Expression, BinaryOp, UnaryOp, Column, Literal, FunctionCall
from fornero.spreadsheet.model import Range, Reference
from fornero.exceptions import UnsupportedOperationError


@dataclass
class TranslationResult:
    """Result of translating an operation to spreadsheet operations.

    Attributes:
        operations: List of spreadsheet operation dictionaries
        sheet_name: Name of the output sheet
        output_range: Range where the output data is located
    """
    operations: List[Dict[str, Any]]
    sheet_name: str
    output_range: Range


@dataclass
class JoinTranslationContext:
    """Context for translating join operations.

    Attributes:
        op: Join operation being translated
        counter: Sheet counter for naming
        left_sheet: Left input sheet name
        left_range: Left input data range
        left_schema: Left input column names
        right_sheet: Right input sheet name
        right_range: Right input data range
        right_schema: Right input column names
    """
    op: Join
    counter: int
    left_sheet: str
    left_range: Range
    left_schema: List[str]
    right_sheet: str
    right_range: Range
    right_schema: List[str]

    @property
    def left_key(self) -> str:
        """Get the left join key (first key if multiple)."""
        return self.op.left_on[0] if isinstance(self.op.left_on, list) else self.op.left_on

    @property
    def right_key(self) -> str:
        """Get the right join key (first key if multiple)."""
        return self.op.right_on[0] if isinstance(self.op.right_on, list) else self.op.right_on

    @property
    def right_keys(self) -> Set[str]:
        """Get set of all right join keys."""
        return set(self.op.right_on) if isinstance(self.op.right_on, list) else {self.op.right_on}

    @property
    def output_schema(self) -> List[str]:
        """Compute output schema (left columns + right non-key columns)."""
        result = self.left_schema.copy()
        for col in self.right_schema:
            if col not in self.right_keys:
                result.append(col)
        return result

    @property
    def num_cols(self) -> int:
        """Get number of output columns."""
        return len(self.output_schema)


@dataclass
class WindowTranslationContext:
    """Context for translating window operations.

    Attributes:
        op: Window operation being translated
        operations: List to append operations to
        sheet_name: Name of the output sheet
        input_sheet: Input sheet name
        input_range: Input data range
        input_schema: Input column names
        window_col_idx: Index where window column is appended
        data_rows: Number of data rows
    """
    op: Window
    operations: List[Dict[str, Any]]
    sheet_name: str
    input_sheet: str
    input_range: Range
    input_schema: List[str]
    window_col_idx: int
    data_rows: int


def _generate_sheet_name(op: Operation, counter: int) -> str:
    """Generate a unique sheet name for an operation.

    Args:
        op: Operation node
        counter: Unique counter for this operation in the plan

    Returns:
        Sheet name string
    """
    op_type = op.__class__.__name__
    return f"{op_type}_{counter}"


def _col_to_range_ref(sheet: str, range_obj: Range, col_name: str, col_idx: int, data_only: bool = True) -> str:
    """Generate a range reference for a single column.

    Args:
        sheet: Sheet name
        range_obj: Full data range
        col_name: Column name (unused, for clarity)
        col_idx: 0-based column index
        data_only: If True, exclude header row

    Returns:
        Range reference string like "Sheet1!A2:A100"
    """
    # Calculate 0-indexed start and end rows
    start_row_0indexed = range_obj.row + (1 if data_only else 0)
    end_row_0indexed = range_obj.row_end

    # Convert to 1-indexed for A1 notation
    start_row_a1 = start_row_0indexed + 1
    end_row_a1 = end_row_0indexed + 1

    col_letter = Range._col_to_letter(range_obj.col + col_idx)

    ref = Reference(f"{col_letter}{start_row_a1}:{col_letter}{end_row_a1}", sheet_name=sheet)
    return ref.to_string()


def _full_range_ref(sheet: str, range_obj: Range, data_only: bool = True) -> str:
    """Generate a range reference for all columns.

    Args:
        sheet: Sheet name
        range_obj: Full data range (0-indexed)
        data_only: If True, exclude header row

    Returns:
        Range reference string
    """
    if data_only:
        # Skip header row (row 0) and start from data rows (row 1+)
        # In 0-indexed: start_row = 1, end_row = range_obj.row_end
        start_row = range_obj.row + 1
        end_row = max(start_row, range_obj.row_end)
        r = Range(start_row, range_obj.col, end_row, range_obj.col_end)
    else:
        r = range_obj
    ref = Reference(r.to_a1(), sheet_name=sheet)
    return ref.to_string()


def translate_source(op: Source, counter: int, data: Any) -> TranslationResult:
    """Translate Source operation - the only operation that writes static values.

    Args:
        op: Source operation
        counter: Sheet counter
        data: Actual data rows (list of lists or similar structure)

    Returns:
        (operations, sheet_name, output_range)
    """
    if not op.schema:
        raise ValueError("Source operation must have schema defined for translation")

    sheet_name = _generate_sheet_name(op, counter)
    num_cols = len(op.schema)

    # Determine number of rows from data
    if hasattr(data, '__len__'):
        num_rows = len(data)
    else:
        num_rows = 0

    operations = []

    # Create sheet
    operations.append({
        'type': 'create_sheet',
        'name': sheet_name,
        'rows': num_rows + 1,  # +1 for header
        'cols': num_cols
    })

    # Write header
    operations.append({
        'type': 'set_values',
        'sheet': sheet_name,
        'row': 0,
        'col': 0,
        'values': [op.schema]
    })

    # Write data
    operations.append({
        'type': 'set_values',
        'sheet': sheet_name,
        'row': 1,
        'col': 0,
        'values': data
    })

    # Output range starts at row 0 (header) through row num_rows (last data row).
    # Range uses 0-indexed coordinates internally: header is row 0, data spans rows 1..num_rows.
    # Range includes all columns from 0 to num_cols-1.
    output_range = Range(row=0, col=0, row_end=max(0, num_rows), col_end=num_cols - 1)

    return TranslationResult(operations, sheet_name, output_range)


def translate_select(op: Select, counter: int, input_sheet: str, input_range: Range,
                     input_schema: List[str]) -> TranslationResult:
    """Translate Select operation - column projection.

    Args:
        op: Select operation
        counter: Sheet counter
        input_sheet: Input sheet name
        input_range: Input data range
        input_schema: Input column names

    Returns:
        (operations, sheet_name, output_range)
    """
    sheet_name = _generate_sheet_name(op, counter)
    num_cols = len(op.columns)
    num_rows = input_range.row_end - input_range.row + 1  # Including header

    operations = []

    # Create sheet
    operations.append({
        'type': 'create_sheet',
        'name': sheet_name,
        'rows': num_rows,
        'cols': num_cols
    })

    # Write header
    operations.append({
        'type': 'set_values',
        'sheet': sheet_name,
        'row': 0,
        'col': 0,
        'values': [op.columns]
    })

    # Build column references for the selected columns
    col_refs = []
    for col_name in op.columns:
        if col_name not in input_schema:
            raise ValueError(f"Column '{col_name}' not found in input schema: {input_schema}")
        col_idx = input_schema.index(col_name)
        col_refs.append(_col_to_range_ref(input_sheet, input_range, col_name, col_idx, data_only=True))

    # Use a single FILTER formula that selects the columns and excludes
    # empty trailing rows (which arise when an upstream FILTER returned
    # fewer rows than the statically-allocated range).
    first_col_ref = _col_to_range_ref(input_sheet, input_range, input_schema[0], 0, data_only=True)

    conditions = []
    conditions.append(f'{first_col_ref}<>""')

    if op.predicate:
        pred_expr = _translate_predicate(op.predicate, input_sheet, input_range, input_schema)
        conditions.append(pred_expr)

    full_condition = "*".join(f"({c})" for c in conditions) if len(conditions) > 1 else conditions[0]

    if len(col_refs) == 1:
        array_expr = col_refs[0]
    else:
        array_expr = "{" + ", ".join(col_refs) + "}"
    formula = f"=FILTER({array_expr}, {full_condition})"
    operations.append({
        'type': 'set_formula',
        'sheet': sheet_name,
        'row': 1,
        'col': 0,
        'formula': formula
    })

    # Ensure row_end is at least row (handle single row case)
    output_range = Range(row=0, col=0, row_end=num_rows, col_end=num_cols - 1)

    return TranslationResult(operations, sheet_name, output_range)


def translate_filter(op: Filter, counter: int, input_sheet: str, input_range: Range,
                    input_schema: List[str]) -> TranslationResult:
    """Translate Filter operation using FILTER() formula.

    Args:
        op: Filter operation
        counter: Sheet counter
        input_sheet: Input sheet name
        input_range: Input data range
        input_schema: Input column names

    Returns:
        (operations, sheet_name, output_range)
    """
    sheet_name = _generate_sheet_name(op, counter)
    num_cols = len(input_schema)

    operations = []

    # Create sheet with sufficient space (FILTER will spill dynamically)
    operations.append({
        'type': 'create_sheet',
        'name': sheet_name,
        'rows': input_range.row_end - input_range.row + 1,
        'cols': num_cols
    })

    # Write header
    operations.append({
        'type': 'set_values',
        'sheet': sheet_name,
        'row': 0,
        'col': 0,
        'values': [input_schema]
    })

    # Translate predicate to spreadsheet condition
    condition = _translate_predicate(op.predicate, input_sheet, input_range, input_schema)

    # Full data range reference (no header)
    data_ref = _full_range_ref(input_sheet, input_range, data_only=True)

    # FILTER formula
    filter_formula = f"=FILTER({data_ref}, {condition})"

    operations.append({
        'type': 'set_formula',
        'sheet': sheet_name,
        'row': 1,
        'col': 0,
        'formula': filter_formula
    })

    # Output range is indeterminate (FILTER spills dynamically)
    output_range = Range(row=0, col=0, row_end=max(0, input_range.row_end - input_range.row), col_end=num_cols - 1)

    return TranslationResult(operations, sheet_name, output_range)


def _translate_expression_ast(node: Expression, input_sheet: str, input_range: Range,
                              input_schema: List[str]) -> str:
    """Recursively translate an Expression AST node to a spreadsheet formula fragment.

    Args:
        node: Expression AST node
        input_sheet: Input sheet name
        input_range: Input data range
        input_schema: Input column names

    Returns:
        Formula fragment string
    """
    if isinstance(node, Column):
        col_idx = input_schema.index(node.name)
        return _col_to_range_ref(input_sheet, input_range, node.name, col_idx, data_only=True)

    if isinstance(node, Literal):
        if isinstance(node.value, str):
            return f'"{node.value}"'
        return str(node.value)

    if isinstance(node, BinaryOp):
        left = _translate_expression_ast(node.left, input_sheet, input_range, input_schema)
        right = _translate_expression_ast(node.right, input_sheet, input_range, input_schema)

        op_map = {"==": "=", "!=": "<>", "and": "*", "or": "+"}
        op_str = op_map.get(node.op, node.op)

        if node.op in ("and", "or"):
            return f"({left}){op_str}({right})"
        return f"({left}{op_str}{right})"

    if isinstance(node, UnaryOp):
        operand = _translate_expression_ast(node.operand, input_sheet, input_range, input_schema)
        if node.op == "neg":
            return f"-({operand})"
        if node.op == "not":
            return f"NOT({operand})"
        return f"{node.op}({operand})"

    if isinstance(node, FunctionCall):
        args = ", ".join(
            _translate_expression_ast(a, input_sheet, input_range, input_schema)
            for a in node.args
        )
        return f"{node.func}({args})"

    # Base Expression with a plain string — fall through to string handling
    return str(node)


def _translate_predicate(predicate, input_sheet: str, input_range: Range,
                         input_schema: List[str]) -> str:
    """Translate a predicate Expression AST to spreadsheet condition.

    Args:
        predicate: Expression AST node (BinaryOp, Column, Literal, etc.)
        input_sheet: Input sheet name
        input_range: Input data range
        input_schema: Input column names

    Returns:
        Spreadsheet condition expression
    """
    if not isinstance(predicate, (BinaryOp, UnaryOp, Column, Literal, FunctionCall)):
        raise ValueError(
            f"Predicate must be an Expression AST node, got {type(predicate).__name__}. "
            f"Use col() helper to create predicates."
        )
    return _translate_expression_ast(predicate, input_sheet, input_range, input_schema)


def translate_join(op: Join, counter: int, left_sheet: str, left_range: Range, left_schema: List[str],
                  right_sheet: str, right_range: Range, right_schema: List[str]) -> TranslationResult:
    """Translate Join operation using XLOOKUP.

    Supports inner, left, right, and outer join types per the architecture spec.
    - left: R1 is the base, XLOOKUP brings in R2 columns
    - inner: like left, then FILTER removes unmatched rows
    - right: R2 is the base, XLOOKUP brings in R1 columns (symmetric with left)
    - outer: left join + anti-join of unmatched R2 rows, unioned together

    Args:
        op: Join operation
        counter: Sheet counter
        left_sheet: Left input sheet name
        left_range: Left input data range
        left_schema: Left input column names
        right_sheet: Right input sheet name
        right_range: Right input data range
        right_schema: Right input column names

    Returns:
        (operations, sheet_name, output_range)
    """
    ctx = JoinTranslationContext(
        op, counter, left_sheet, left_range, left_schema,
        right_sheet, right_range, right_schema
    )

    if op.join_type == 'right':
        return _translate_right_join(ctx)
    if op.join_type == 'outer':
        return _translate_outer_join(ctx)

    return _translate_left_or_inner_join(ctx)


def _translate_left_or_inner_join(ctx: JoinTranslationContext) -> TranslationResult:
    """Left or inner join: R1 is the base, XLOOKUP brings R2 columns."""
    sheet_name = _generate_sheet_name(ctx.op, ctx.counter)
    num_rows = ctx.left_range.row_end - ctx.left_range.row + 1

    operations = []
    operations.append({'type': 'create_sheet', 'name': sheet_name, 'rows': num_rows, 'cols': ctx.num_cols})
    operations.append({'type': 'set_values', 'sheet': sheet_name, 'row': 0, 'col': 0, 'values': [ctx.output_schema]})

    for j, col_name in enumerate(ctx.left_schema):
        col_ref = _col_to_range_ref(ctx.left_sheet, ctx.left_range, col_name, j, data_only=True)
        operations.append({'type': 'set_formula', 'sheet': sheet_name, 'row': 1, 'col': j, 'formula': f"=ARRAYFORMULA({col_ref})"})

    left_key_idx = ctx.left_schema.index(ctx.left_key)
    right_key_idx = ctx.right_schema.index(ctx.right_key)
    lookup_array_ref = _col_to_range_ref(ctx.left_sheet, ctx.left_range, ctx.left_key, left_key_idx, data_only=True)
    lookup_range_ref = _col_to_range_ref(ctx.right_sheet, ctx.right_range, ctx.right_key, right_key_idx, data_only=True)

    current_col = len(ctx.left_schema)
    for col_name in ctx.right_schema:
        if col_name in ctx.right_keys:
            continue
        col_idx = ctx.right_schema.index(col_name)
        return_array_ref = _col_to_range_ref(ctx.right_sheet, ctx.right_range, col_name, col_idx, data_only=True)
        xlookup_formula = f'=ARRAYFORMULA(XLOOKUP({lookup_array_ref}, {lookup_range_ref}, {return_array_ref}, ""))'
        operations.append({'type': 'set_formula', 'sheet': sheet_name, 'row': 1, 'col': current_col, 'formula': xlookup_formula})
        current_col += 1

    intermediate_range = Range(row=0, col=0, row_end=num_rows, col_end=ctx.num_cols - 1)

    if ctx.op.join_type == 'inner':
        helper_sheet = f"{sheet_name}_filtered"
        operations.append({'type': 'create_sheet', 'name': helper_sheet, 'rows': num_rows, 'cols': ctx.num_cols})
        operations.append({'type': 'set_values', 'sheet': helper_sheet, 'row': 0, 'col': 0, 'values': [ctx.output_schema]})

        right_non_key_cols = [c for c in ctx.right_schema if c not in ctx.right_keys]
        if right_non_key_cols:
            intermediate_data_ref = _full_range_ref(sheet_name, intermediate_range, data_only=True)
            checks = []
            for col in right_non_key_cols:
                col_idx = ctx.output_schema.index(col)
                col_ref = _col_to_range_ref(sheet_name, intermediate_range, col, col_idx, data_only=True)
                checks.append(f'({col_ref}<>"")')
            condition = "+".join(checks)
            filter_formula = f'=FILTER({intermediate_data_ref}, {condition})'
            operations.append({'type': 'set_formula', 'sheet': helper_sheet, 'row': 1, 'col': 0, 'formula': filter_formula})

        output_range = Range(row=0, col=0, row_end=num_rows, col_end=ctx.num_cols - 1)
        return TranslationResult(operations, helper_sheet, output_range)

    return TranslationResult(operations, sheet_name, intermediate_range)


def _translate_right_join(ctx: JoinTranslationContext) -> TranslationResult:
    """Right join: R2 is the base, XLOOKUP brings R1 columns (symmetric with left)."""
    sheet_name = _generate_sheet_name(ctx.op, ctx.counter)
    num_rows = ctx.right_range.row_end - ctx.right_range.row + 1

    operations = []
    operations.append({'type': 'create_sheet', 'name': sheet_name, 'rows': num_rows, 'cols': ctx.num_cols})
    operations.append({'type': 'set_values', 'sheet': sheet_name, 'row': 0, 'col': 0, 'values': [ctx.output_schema]})

    left_key_idx = ctx.left_schema.index(ctx.left_key)
    right_key_idx = ctx.right_schema.index(ctx.right_key)
    lookup_array_ref = _col_to_range_ref(ctx.right_sheet, ctx.right_range, ctx.right_key, right_key_idx, data_only=True)
    search_range_ref = _col_to_range_ref(ctx.left_sheet, ctx.left_range, ctx.left_key, left_key_idx, data_only=True)

    for j, col_name in enumerate(ctx.left_schema):
        col_idx = ctx.left_schema.index(col_name)
        return_ref = _col_to_range_ref(ctx.left_sheet, ctx.left_range, col_name, col_idx, data_only=True)
        xlookup = f'=ARRAYFORMULA(XLOOKUP({lookup_array_ref}, {search_range_ref}, {return_ref}, ""))'
        operations.append({'type': 'set_formula', 'sheet': sheet_name, 'row': 1, 'col': j, 'formula': xlookup})

    current_col = len(ctx.left_schema)
    for col_name in ctx.right_schema:
        if col_name in ctx.right_keys:
            continue
        col_idx = ctx.right_schema.index(col_name)
        col_ref = _col_to_range_ref(ctx.right_sheet, ctx.right_range, col_name, col_idx, data_only=True)
        operations.append({'type': 'set_formula', 'sheet': sheet_name, 'row': 1, 'col': current_col, 'formula': f"=ARRAYFORMULA({col_ref})"})
        current_col += 1

    output_range = Range(row=0, col=0, row_end=num_rows, col_end=ctx.num_cols - 1)
    return TranslationResult(operations, sheet_name, output_range)


def _translate_outer_join(ctx: JoinTranslationContext) -> TranslationResult:
    """Outer join: left join + anti-join of unmatched R2 rows, unioned.

    Sheet 1 (left part): left join result — all R1 rows with XLOOKUP for R2.
    Sheet 2 (anti-join): R2 rows where key not in R1, with R1 columns null.
    Sheet 3 (output):    vertical union of sheets 1 and 2.
    """
    sheet_name = _generate_sheet_name(ctx.op, ctx.counter)
    left_part = f"{sheet_name}_left"
    anti_part = f"{sheet_name}_anti"

    left_key_idx = ctx.left_schema.index(ctx.left_key)
    right_key_idx = ctx.right_schema.index(ctx.right_key)
    left_num_rows = ctx.left_range.row_end - ctx.left_range.row + 1
    right_num_rows = ctx.right_range.row_end - ctx.right_range.row + 1

    operations = []

    # Sheet 1: left join (reuse the left-join logic inline)
    operations.append({'type': 'create_sheet', 'name': left_part, 'rows': left_num_rows, 'cols': ctx.num_cols})
    operations.append({'type': 'set_values', 'sheet': left_part, 'row': 0, 'col': 0, 'values': [ctx.output_schema]})

    for j, col_name in enumerate(ctx.left_schema):
        col_ref = _col_to_range_ref(ctx.left_sheet, ctx.left_range, col_name, j, data_only=True)
        operations.append({'type': 'set_formula', 'sheet': left_part, 'row': 1, 'col': j, 'formula': f"=ARRAYFORMULA({col_ref})"})

    lookup_array = _col_to_range_ref(ctx.left_sheet, ctx.left_range, ctx.left_key, left_key_idx, data_only=True)
    search_range = _col_to_range_ref(ctx.right_sheet, ctx.right_range, ctx.right_key, right_key_idx, data_only=True)

    current_col = len(ctx.left_schema)
    for col_name in ctx.right_schema:
        if col_name in ctx.right_keys:
            continue
        col_idx = ctx.right_schema.index(col_name)
        ret_ref = _col_to_range_ref(ctx.right_sheet, ctx.right_range, col_name, col_idx, data_only=True)
        xlookup = f'=ARRAYFORMULA(XLOOKUP({lookup_array}, {search_range}, {ret_ref}, ""))'
        operations.append({'type': 'set_formula', 'sheet': left_part, 'row': 1, 'col': current_col, 'formula': xlookup})
        current_col += 1

    left_part_range = Range(row=0, col=0, row_end=left_num_rows, col_end=ctx.num_cols - 1)

    # Sheet 2: anti-join — R2 rows whose key is not in R1
    operations.append({'type': 'create_sheet', 'name': anti_part, 'rows': right_num_rows, 'cols': ctx.num_cols})
    operations.append({'type': 'set_values', 'sheet': anti_part, 'row': 0, 'col': 0, 'values': [ctx.output_schema]})

    r2_key_ref = _col_to_range_ref(ctx.right_sheet, ctx.right_range, ctx.right_key, right_key_idx, data_only=True)
    r1_key_ref = _col_to_range_ref(ctx.left_sheet, ctx.left_range, ctx.left_key, left_key_idx, data_only=True)
    anti_condition = f"ISNA(XMATCH({r2_key_ref}, {r1_key_ref}))"

    for j, col_name in enumerate(ctx.output_schema):
        if col_name in ctx.left_schema:
            # R1 columns are null for unmatched R2 rows — fill with empty strings
            # Use IF(condition, "", "") wrapped in ARRAYFORMULA so it spills to the
            # correct number of rows matching the FILTER output.
            # Simpler: leave R1 columns blank by not writing a formula.
            pass
        else:
            # R2 non-key column — FILTER to keep only unmatched rows
            r2_col_idx = ctx.right_schema.index(col_name)
            r2_col_ref = _col_to_range_ref(ctx.right_sheet, ctx.right_range, col_name, r2_col_idx, data_only=True)
            formula = f"=FILTER({r2_col_ref}, {anti_condition})"
            operations.append({'type': 'set_formula', 'sheet': anti_part, 'row': 1, 'col': j, 'formula': formula})

    anti_part_range = Range(row=0, col=0, row_end=right_num_rows, col_end=ctx.num_cols - 1)

    # Sheet 3: union of left part and anti-join part
    total_rows = left_num_rows + right_num_rows
    operations.append({'type': 'create_sheet', 'name': sheet_name, 'rows': total_rows, 'cols': ctx.num_cols})
    operations.append({'type': 'set_values', 'sheet': sheet_name, 'row': 0, 'col': 0, 'values': [ctx.output_schema]})

    left_data_ref = _full_range_ref(left_part, left_part_range, data_only=True)
    anti_data_ref = _full_range_ref(anti_part, anti_part_range, data_only=True)
    union_formula = f"={{{left_data_ref}; {anti_data_ref}}}"
    operations.append({'type': 'set_formula', 'sheet': sheet_name, 'row': 1, 'col': 0, 'formula': union_formula})

    output_range = Range(row=0, col=0, row_end=total_rows, col_end=ctx.num_cols - 1)
    return TranslationResult(operations, sheet_name, output_range)


def translate_groupby(op: GroupBy, counter: int, input_sheet: str, input_range: Range,
                     input_schema: List[str]) -> TranslationResult:
    """Translate GroupBy operation using UNIQUE + per-row SUMIFS pattern.

    Uses a single-sheet strategy that preserves first-appearance order:
    1. Output headers in row 0
    2. Group keys via UNIQUE formula (preserves first-appearance order)
    3. Aggregation columns via per-row SUMIFS/AVERAGEIFS/COUNTIFS/etc. formulas

    Args:
        op: GroupBy operation
        counter: Sheet counter
        input_sheet: Input sheet name
        input_range: Input data range
        input_schema: Input column names

    Returns:
        (operations, sheet_name, output_range)
    """
    sheet_name = _generate_sheet_name(op, counter)

    # Build output schema: keys first, then aggregation result columns
    output_schema = op.keys.copy()
    for agg_name, agg_func, agg_col in op.aggregations:
        output_schema.append(agg_name)
    num_cols = len(output_schema)

    operations = []

    # Mapping from aggregation function name to spreadsheet conditional function
    agg_func_map = {
        'sum': 'SUMIFS',
        'mean': 'AVERAGEIFS',
        'count': 'COUNTIFS',
        'min': 'MINIFS',
        'max': 'MAXIFS',
    }

    # Create the output sheet (allocate enough rows for potential groups)
    max_output_rows = 100
    operations.append({'type': 'create_sheet', 'name': sheet_name, 'rows': max_output_rows, 'cols': num_cols})

    # Set headers in row 0
    operations.append({'type': 'set_values', 'sheet': sheet_name, 'row': 0, 'col': 0, 'values': [output_schema]})

    # Generate UNIQUE formula for group keys
    # For single key: =UNIQUE(Source!A2:A)
    # For multi-key: =UNIQUE(Source!A2:C) returns multiple columns
    num_keys = len(op.keys)

    if num_keys == 1:
        key = op.keys[0]
        key_idx = input_schema.index(key)
        key_ref = _col_to_range_ref(input_sheet, input_range, key, key_idx, data_only=True)
        unique_formula = f"=UNIQUE({key_ref})"
    else:
        # Multi-column UNIQUE: reference all key columns together
        # Get the column indices for all keys
        key_indices = [input_schema.index(key) for key in op.keys]

        # Check if keys are contiguous
        sorted_indices = sorted(key_indices)
        is_contiguous = all(sorted_indices[i] + 1 == sorted_indices[i + 1]
                           for i in range(len(sorted_indices) - 1))

        if is_contiguous:
            # Keys are contiguous - use a single range reference
            min_key_idx = min(key_indices)
            max_key_idx = max(key_indices)
            start_col_letter = Range._col_to_letter(input_range.col + min_key_idx)
            end_col_letter = Range._col_to_letter(input_range.col + max_key_idx)
            start_row = input_range.row + 1  # data_only=True
            end_row = input_range.row_end

            keys_ref = Reference(f"{start_col_letter}{start_row}:{end_col_letter}{end_row}",
                                sheet_name=input_sheet)
            unique_formula = f"=UNIQUE({keys_ref.to_string()})"
        else:
            # Keys are non-contiguous - use array construction {col_a, col_d}
            key_refs = []
            for key in op.keys:
                key_idx = input_schema.index(key)
                key_ref = _col_to_range_ref(input_sheet, input_range, key, key_idx, data_only=True)
                key_refs.append(key_ref)

            # Build array construction formula
            array_construction = "{" + ", ".join(key_refs) + "}"
            unique_formula = f"=UNIQUE({array_construction})"

    operations.append({'type': 'set_formula', 'sheet': sheet_name, 'row': 1, 'col': 0, 'formula': unique_formula})

    # Generate per-row aggregation formulas
    # For each output row (up to max_output_rows), generate a formula that:
    # - Checks if the key cell is empty (no more groups)
    # - If not empty, calculates the aggregation using SUMIFS/etc matching the key value

    for agg_idx, (agg_name, agg_func, agg_col) in enumerate(op.aggregations):
        if agg_func not in agg_func_map:
            raise UnsupportedOperationError(f"Aggregation function '{agg_func}' not supported")

        spreadsheet_func = agg_func_map[agg_func]
        agg_col_idx = input_schema.index(agg_col)

        # Get source value column reference (for sum/mean/min/max, not count)
        if agg_func != 'count':
            value_ref = _col_to_range_ref(input_sheet, input_range, agg_col, agg_col_idx, data_only=True)

        # Generate formula for each potential output row
        for row_offset in range(max_output_rows - 1):  # -1 because row 0 is header
            output_row = 2 + row_offset  # 1-indexed: row 1 is header, data starts at row 2

            # Check if first key column is empty
            first_key_col_letter = Range._col_to_letter(0)
            key_cell_ref = f"{sheet_name}!{first_key_col_letter}{output_row}"

            # Build criteria pairs for each key column
            criteria_parts = []
            for key_idx, key in enumerate(op.keys):
                src_key_col_idx = input_schema.index(key)
                src_key_ref = _col_to_range_ref(input_sheet, input_range, key, src_key_col_idx, data_only=True)

                # Reference to this row's key value
                output_key_col_letter = Range._col_to_letter(key_idx)
                output_key_cell = f"{sheet_name}!{output_key_col_letter}{output_row}"

                criteria_parts.append(f"{src_key_ref}, {output_key_cell}")

            criteria_clause = ", ".join(criteria_parts)

            # Build the formula with empty check
            if agg_func == 'count':
                # COUNTIFS doesn't take a value range
                inner_formula = f"{spreadsheet_func}({criteria_clause})"
            else:
                # SUMIFS, AVERAGEIFS, MINIFS, MAXIFS take value range first
                inner_formula = f"{spreadsheet_func}({value_ref}, {criteria_clause})"

            formula = f'=IF({key_cell_ref}="", "", {inner_formula})'

            # Place formula in the appropriate column
            agg_output_col = num_keys + agg_idx
            operations.append({
                'type': 'set_formula',
                'sheet': sheet_name,
                'row': 1 + row_offset,  # 0-indexed: row 1 is first data row
                'col': agg_output_col,
                'formula': formula
            })

    output_range = Range(row=0, col=0, row_end=max_output_rows, col_end=num_cols - 1)
    return TranslationResult(operations, sheet_name, output_range)

def translate_aggregate(op: Aggregate, counter: int, input_sheet: str, input_range: Range,
                       input_schema: List[str]) -> TranslationResult:
    """Translate Aggregate operation using scalar formulas.

    Args:
        op: Aggregate operation
        counter: Sheet counter
        input_sheet: Input sheet name
        input_range: Input data range
        input_schema: Input column names

    Returns:
        (operations, sheet_name, output_range)
    """
    sheet_name = _generate_sheet_name(op, counter)

    # Output schema: aggregation output names
    output_schema = [agg_name for agg_name, _, _ in op.aggregations]
    num_cols = len(output_schema)

    operations = []

    # Create sheet (1 header row + 1 data row)
    operations.append({
        'type': 'create_sheet',
        'name': sheet_name,
        'rows': 2,
        'cols': num_cols
    })

    # Write header
    operations.append({
        'type': 'set_values',
        'sheet': sheet_name,
        'row': 0,
        'col': 0,
        'values': [output_schema]
    })

    # Function mapping
    func_map = {
        'sum': 'SUM',
        'mean': 'AVERAGE',
        'count': 'COUNTA',
        'min': 'MIN',
        'max': 'MAX'
    }

    # Create formula for each aggregation
    for j, (agg_name, agg_func, agg_col) in enumerate(op.aggregations):
        if agg_func not in func_map:
            raise UnsupportedOperationError(f"Aggregation function '{agg_func}' not supported")

        col_idx = input_schema.index(agg_col)
        col_ref = _col_to_range_ref(input_sheet, input_range, agg_col, col_idx, data_only=True)

        sheets_func = func_map[agg_func]
        formula = f"={sheets_func}({col_ref})"

        operations.append({
            'type': 'set_formula',
            'sheet': sheet_name,
            'row': 1,
            'col': j,
            'formula': formula
        })

    output_range = Range(row=0, col=0, row_end=2, col_end=num_cols - 1)

    return TranslationResult(operations, sheet_name, output_range)


def translate_sort(op: Sort, counter: int, input_sheet: str, input_range: Range,
                  input_schema: List[str]) -> TranslationResult:
    """Translate Sort operation using SORT formula.

    Args:
        op: Sort operation
        counter: Sheet counter
        input_sheet: Input sheet name
        input_range: Input data range
        input_schema: Input column names

    Returns:
        (operations, sheet_name, output_range)
    """
    sheet_name = _generate_sheet_name(op, counter)
    num_cols = len(input_schema)
    num_rows = input_range.row_end - input_range.row + 1

    if op.limit:
        # When limit is present, we need header + limit data rows
        num_rows = min(op.limit + 1, num_rows)

    operations = []

    # Create sheet
    operations.append({
        'type': 'create_sheet',
        'name': sheet_name,
        'rows': num_rows,
        'cols': num_cols
    })

    # Write header
    operations.append({
        'type': 'set_values',
        'sheet': sheet_name,
        'row': 0,
        'col': 0,
        'values': [input_schema]
    })

    # Build SORT formula
    data_ref = _full_range_ref(input_sheet, input_range, data_only=True)

    # SORT(range, sort_column, is_ascending, [sort_column2, is_ascending2, ...])
    sort_params = []
    for col_name, direction in op.keys:
        col_idx = input_schema.index(col_name) + 1  # 1-based for SORT
        is_asc = "TRUE" if direction == 'asc' else "FALSE"
        sort_params.append(f"{col_idx}, {is_asc}")

    sort_params_str = ", ".join(sort_params)

    # Build filter condition
    conditions = []

    # Always filter out empty rows from fixed-range upstream sheets
    first_col_ref = _col_to_range_ref(input_sheet, input_range, input_schema[0], 0, data_only=True)
    conditions.append(f'{first_col_ref}<>""')

    if op.predicate:
        # Translate the pushed-down predicate
        pred_expr = _translate_predicate(op.predicate, input_sheet, input_range, input_schema)
        conditions.append(pred_expr)

    # Combine conditions with * (AND)
    if len(conditions) > 1:
        full_condition = "*".join(f"({c})" for c in conditions)
    else:
        full_condition = conditions[0]

    filtered_ref = f"FILTER({data_ref}, {full_condition})"
    sort_formula = f"SORT({filtered_ref}, {sort_params_str})"

    if op.limit:
        # Apply limit via ARRAY_CONSTRAIN
        final_formula = f"=ARRAY_CONSTRAIN({sort_formula}, {op.limit}, {num_cols})"
        # Update output rows to match limit
        output_rows = min(op.limit, num_rows)
    else:
        final_formula = f"={sort_formula}"
        output_rows = num_rows

    operations.append({
        'type': 'set_formula',
        'sheet': sheet_name,
        'row': 1,
        'col': 0,
        'formula': final_formula
    })

    # Ensure row_end is at least row (handle single row case)
    output_range = Range(row=0, col=0, row_end=output_rows, col_end=num_cols - 1)

    return TranslationResult(operations, sheet_name, output_range)


def translate_limit(op: Limit, counter: int, input_sheet: str, input_range: Range,
                   input_schema: List[str]) -> TranslationResult:
    """Translate Limit operation using ARRAY_CONSTRAIN or INDEX.

    Args:
        op: Limit operation
        counter: Sheet counter
        input_sheet: Input sheet name
        input_range: Input data range
        input_schema: Input column names

    Returns:
        (operations, sheet_name, output_range)
    """
    sheet_name = _generate_sheet_name(op, counter)
    num_cols = len(input_schema)
    num_rows = min(op.count, input_range.row_end - input_range.row) + 1  # +1 for header

    operations = []

    # Create sheet
    operations.append({
        'type': 'create_sheet',
        'name': sheet_name,
        'rows': num_rows,
        'cols': num_cols
    })

    # Write header
    operations.append({
        'type': 'set_values',
        'sheet': sheet_name,
        'row': 0,
        'col': 0,
        'values': [input_schema]
    })

    data_ref = _full_range_ref(input_sheet, input_range, data_only=True)

    if op.end == 'head':
        limit_formula = f"=ARRAY_CONSTRAIN({data_ref}, {op.count}, {num_cols})"
    else:  # tail
        limit_formula = f"=OFFSET({data_ref}, ROWS({data_ref}) - {op.count}, 0, {op.count}, {num_cols})"

    operations.append({
        'type': 'set_formula',
        'sheet': sheet_name,
        'row': 1,
        'col': 0,
        'formula': limit_formula
    })

    # Ensure row_end is at least row (handle single row case)
    output_range = Range(row=0, col=0, row_end=num_rows, col_end=num_cols - 1)

    return TranslationResult(operations, sheet_name, output_range)


def translate_with_column(op: WithColumn, counter: int, input_sheet: str, input_range: Range,
                         input_schema: List[str]) -> TranslationResult:
    """Translate WithColumn operation.

    Args:
        op: WithColumn operation
        counter: Sheet counter
        input_sheet: Input sheet name
        input_range: Input data range
        input_schema: Input column names

    Returns:
        (operations, sheet_name, output_range)
    """
    sheet_name = _generate_sheet_name(op, counter)

    # Output schema: existing columns + new/replaced column
    if op.column in input_schema:
        output_schema = input_schema.copy()
        replace_idx = output_schema.index(op.column)
    else:
        output_schema = input_schema + [op.column]
        replace_idx = None

    num_cols = len(output_schema)
    num_rows = input_range.row_end - input_range.row + 1

    operations = []

    # Create sheet
    operations.append({
        'type': 'create_sheet',
        'name': sheet_name,
        'rows': num_rows,
        'cols': num_cols
    })

    # Write header
    operations.append({
        'type': 'set_values',
        'sheet': sheet_name,
        'row': 0,
        'col': 0,
        'values': [output_schema]
    })

    # Copy existing columns (except if being replaced)
    for j, col_name in enumerate(output_schema):
        if col_name == op.column:
            # This is the new/replaced column - will handle separately
            continue

        if col_name in input_schema:
            col_idx = input_schema.index(col_name)
            col_ref = _col_to_range_ref(input_sheet, input_range, col_name, col_idx, data_only=True)

            operations.append({
                'type': 'set_formula',
                'sheet': sheet_name,
                'row': 1,
                'col': j,
                'formula': f"=ARRAYFORMULA({col_ref})"
            })

    # Translate the expression to formula
    formula = _translate_expression(op.expression, input_sheet, input_range, input_schema)

    # Find where to put the new column
    output_col_idx = output_schema.index(op.column)

    operations.append({
        'type': 'set_formula',
        'sheet': sheet_name,
        'row': 1,
        'col': output_col_idx,
        'formula': f"=ARRAYFORMULA({formula})"
    })

    # Ensure row_end is at least row (handle single row case)
    output_range = Range(row=0, col=0, row_end=num_rows, col_end=num_cols - 1)

    return TranslationResult(operations, sheet_name, output_range)


def _translate_expression(expr, input_sheet: str, input_range: Range,
                         input_schema: List[str]) -> str:
    """Translate an expression to a spreadsheet formula.

    Args:
        expr: Expression (AST node or string)
        input_sheet: Input sheet name
        input_range: Input data range
        input_schema: Input column names

    Returns:
        Formula string (without leading =)
    """
    if isinstance(expr, (BinaryOp, UnaryOp, Column, Literal, FunctionCall)):
        return _translate_expression_ast(expr, input_sheet, input_range, input_schema)

    result = expr if isinstance(expr, str) else str(expr)

    for col_idx, col_name in enumerate(input_schema):
        if col_name in result:
            col_ref = _col_to_range_ref(input_sheet, input_range, col_name, col_idx, data_only=True)
            result = result.replace(f" {col_name} ", f" {col_ref} ")
            result = result.replace(f"({col_name} ", f"({col_ref} ")
            result = result.replace(f" {col_name})", f" {col_ref})")
            result = result.replace(f"({col_name})", f"({col_ref})")
            if result.startswith(col_name + " "):
                result = col_ref + result[len(col_name):]
            if result.startswith(col_name):
                result = col_ref + result[len(col_name):]

    return result


def translate_union(op: Union, counter: int, left_sheet: str, left_range: Range, left_schema: List[str],
                   right_sheet: str, right_range: Range, right_schema: List[str]) -> TranslationResult:
    """Translate Union operation - vertical concatenation.

    Args:
        op: Union operation
        counter: Sheet counter
        left_sheet: Left input sheet name
        left_range: Left input data range
        left_schema: Left input column names
        right_sheet: Right input sheet name
        right_range: Right input data range
        right_schema: Right input column names

    Returns:
        (operations, sheet_name, output_range)
    """
    if left_schema != right_schema:
        raise UnsupportedOperationError(f"Union requires identical schemas, got {left_schema} and {right_schema}")

    sheet_name = _generate_sheet_name(op, counter)
    num_cols = len(left_schema)
    left_rows = left_range.row_end - left_range.row
    right_rows = right_range.row_end - right_range.row
    num_rows = left_rows + right_rows + 1  # +1 for header

    operations = []

    # Create sheet
    operations.append({
        'type': 'create_sheet',
        'name': sheet_name,
        'rows': num_rows,
        'cols': num_cols
    })

    # Write header
    operations.append({
        'type': 'set_values',
        'sheet': sheet_name,
        'row': 0,
        'col': 0,
        'values': [left_schema]
    })

    # Vertical stack formula: ={range1; range2}
    left_ref = _full_range_ref(left_sheet, left_range, data_only=True)
    right_ref = _full_range_ref(right_sheet, right_range, data_only=True)

    union_formula = f"={{{left_ref}; {right_ref}}}"

    operations.append({
        'type': 'set_formula',
        'sheet': sheet_name,
        'row': 1,
        'col': 0,
        'formula': union_formula
    })

    # Ensure row_end is at least row (handle single row case)
    output_range = Range(row=0, col=0, row_end=num_rows, col_end=num_cols - 1)

    return TranslationResult(operations, sheet_name, output_range)


_MAX_PIVOT_COLS = 50


def translate_pivot(op: Pivot, counter: int, input_sheet: str, input_range: Range,
                   input_schema: List[str],
                   num_pivot_values: Optional[int] = None,
                   num_index_values: Optional[int] = None) -> TranslationResult:
    """Translate Pivot operation using a two-sheet strategy.

    Helper sheet extracts and transposes distinct pivot-column values into a header row.
    Output sheet uses UNIQUE for index values and individual formulas for each data cell.

    Args:
        op: Pivot operation
        counter: Sheet counter
        input_sheet: Input sheet name
        input_range: Input data range
        input_schema: Input column names
        num_pivot_values: Number of distinct pivot column values (when known from
            source data).  Falls back to ``_MAX_PIVOT_COLS`` when ``None``.
        num_index_values: Number of distinct index column values (when known from
            source data).  Falls back to 100 when ``None``.

    Returns:
        (operations, sheet_name, output_range)
    """
    sheet_name = _generate_sheet_name(op, counter)
    helper_sheet = f"{sheet_name}_distinct"

    if isinstance(op.index, list) and len(op.index) > 1:
        raise UnsupportedOperationError(
            f"Pivot with multiple index columns is not supported: {op.index}"
        )
    index_col = op.index[0] if isinstance(op.index, list) else op.index
    pivot_col = op.columns
    values_col = op.values

    index_col_idx = input_schema.index(index_col)
    pivot_col_idx = input_schema.index(pivot_col)
    values_col_idx = input_schema.index(values_col)

    index_col_ref = _col_to_range_ref(input_sheet, input_range, index_col, index_col_idx, data_only=True)
    pivot_col_ref = _col_to_range_ref(input_sheet, input_range, pivot_col, pivot_col_idx, data_only=True)
    values_col_ref = _col_to_range_ref(input_sheet, input_range, values_col, values_col_idx, data_only=True)

    operations = []

    n_cols = num_pivot_values if num_pivot_values is not None else _MAX_PIVOT_COLS
    n_rows = num_index_values if num_index_values is not None else 100

    # Helper sheet: distinct pivot values transposed into a header row
    operations.append({'type': 'create_sheet', 'name': helper_sheet, 'rows': 1, 'cols': n_cols})
    operations.append({
        'type': 'set_formula', 'sheet': helper_sheet, 'row': 0, 'col': 0,
        'formula': f"=TRANSPOSE(SORT(UNIQUE({pivot_col_ref}), 1, TRUE))",
    })

    # Output sheet
    operations.append({'type': 'create_sheet', 'name': sheet_name, 'rows': n_rows + 1, 'cols': n_cols + 1})

    # Header row: column 0 = index name, columns 1+ = distinct pivot values
    last_header_letter = Range._col_to_letter(n_cols)
    operations.append({'type': 'set_values', 'sheet': sheet_name, 'row': 0, 'col': 0, 'values': [[index_col]]})
    operations.append({
        'type': 'set_formula', 'sheet': sheet_name, 'row': 0, 'col': 1,
        'formula': f"='{helper_sheet}'!A1:{last_header_letter}1",
    })

    # Index column: distinct values (preserves first-appearance order via UNIQUE)
    operations.append({
        'type': 'set_formula', 'sheet': sheet_name, 'row': 1, 'col': 0,
        'formula': f"=UNIQUE({index_col_ref})",
    })

    # Determine aggregation function for per-cell formulas
    agg_func = op.aggfunc

    if agg_func not in ('first', None, 'sum', 'mean', 'count', 'min', 'max'):
        raise UnsupportedOperationError(f"Pivot aggregation function '{agg_func}' not supported")

    # Generate individual formulas for each data cell.
    # Each cell references the index value from column A and the pivot value from the helper sheet.
    for i in range(n_rows):
        row_num = i + 2  # Row 1 is headers, data starts at row 2
        index_cell = f"$A{row_num}"

        for j in range(n_cols):
            # Reference the pivot value directly from the helper sheet
            # Helper sheet columns are 0-indexed (A, B, C, ...)
            helper_col_letter = Range._col_to_letter(j)
            pivot_val_cell = f"'{helper_sheet}'!{helper_col_letter}$1"

            # Generate formula based on aggregation function
            if agg_func in (None, 'first'):
                formula = (
                    f'=IFERROR(INDEX(FILTER({values_col_ref}, '
                    f'({index_col_ref}={index_cell})*({pivot_col_ref}={pivot_val_cell})), 1), "")'
                )
            elif agg_func == 'sum':
                formula = (
                    f'=IFERROR(SUMIFS({values_col_ref}, '
                    f'{index_col_ref}, {index_cell}, {pivot_col_ref}, {pivot_val_cell}), "")'
                )
            elif agg_func == 'mean':
                formula = (
                    f'=IFERROR(AVERAGEIFS({values_col_ref}, '
                    f'{index_col_ref}, {index_cell}, {pivot_col_ref}, {pivot_val_cell}), "")'
                )
            elif agg_func == 'count':
                formula = (
                    f'=IFERROR(COUNTIFS({index_col_ref}, {index_cell}, '
                    f'{pivot_col_ref}, {pivot_val_cell}), "")'
                )
            elif agg_func == 'min':
                formula = (
                    f'=IFERROR(MIN(FILTER({values_col_ref}, '
                    f'({index_col_ref}={index_cell})*({pivot_col_ref}={pivot_val_cell}))), "")'
                )
            elif agg_func == 'max':
                formula = (
                    f'=IFERROR(MAX(FILTER({values_col_ref}, '
                    f'({index_col_ref}={index_cell})*({pivot_col_ref}={pivot_val_cell}))), "")'
                )

            operations.append({
                'type': 'set_formula', 'sheet': sheet_name,
                'row': row_num - 1, 'col': j + 1,  # Convert to 0-indexed
                'formula': formula,
            })

    output_range = Range(row=0, col=0, row_end=n_rows + 1, col_end=n_cols)
    return TranslationResult(operations, sheet_name, output_range)


def translate_melt(op: Melt, counter: int, input_sheet: str, input_range: Range,
                  input_schema: List[str]) -> TranslationResult:
    """Translate Melt operation using ARRAYFORMULA with INDEX/CHOOSE/MOD.

    Each input row fans out to |V| rows where V is the set of value columns.
    Identifier columns are repeated via INT((ROW-1)/k)+1 indexing.
    Variable and value columns cycle via CHOOSE with MOD.

    Args:
        op: Melt operation
        counter: Sheet counter
        input_sheet: Input sheet name
        input_range: Input data range
        input_schema: Input column names

    Returns:
        (operations, sheet_name, output_range)
    """
    sheet_name = _generate_sheet_name(op, counter)

    id_vars = op.id_vars
    value_vars = op.value_vars if op.value_vars else [c for c in input_schema if c not in id_vars]
    k = len(value_vars)

    if k == 0:
        raise UnsupportedOperationError("Melt requires at least one value column")

    output_schema = id_vars + [op.var_name, op.value_name]
    num_output_cols = len(output_schema)

    # Number of input data rows
    input_data_rows = input_range.row_end - input_range.row
    num_output_rows = input_data_rows * k

    operations = []

    operations.append({
        'type': 'create_sheet',
        'name': sheet_name,
        'rows': num_output_rows + 1,
        'cols': num_output_cols
    })

    # Write header
    operations.append({
        'type': 'set_values',
        'sheet': sheet_name,
        'row': 0,
        'col': 0,
        'values': [output_schema]
    })

    # Reference to first id column (used for row count in formulas)
    first_id_ref = _col_to_range_ref(
        input_sheet, input_range, id_vars[0], input_schema.index(id_vars[0]), data_only=True
    )

    # For each identifier column: repeat each value k times
    # =ARRAYFORMULA(INDEX(col, INT((ROW(INDIRECT("1:"&ROWS(col)*k))-1)/k)+1))
    for j, id_col in enumerate(id_vars):
        col_idx = input_schema.index(id_col)
        col_ref = _col_to_range_ref(input_sheet, input_range, id_col, col_idx, data_only=True)

        formula = (
            f'=ARRAYFORMULA(INDEX({col_ref}, '
            f'INT((ROW(INDIRECT("1:"&ROWS({col_ref})*{k}))-1)/{k})+1))'
        )

        operations.append({
            'type': 'set_formula',
            'sheet': sheet_name,
            'row': 1,
            'col': j,
            'formula': formula
        })

    # Variable column: cycling list of value column names
    # =ARRAYFORMULA(CHOOSE(MOD(ROW(INDIRECT("1:"&ROWS(col)*k))-1, k)+1, "v1", "v2", ...))
    var_names_str = ", ".join(f'"{v}"' for v in value_vars)
    var_formula = (
        f'=ARRAYFORMULA(CHOOSE(MOD(ROW(INDIRECT("1:"&ROWS({first_id_ref})*{k}))-1, {k})+1, '
        f'{var_names_str}))'
    )

    operations.append({
        'type': 'set_formula',
        'sheet': sheet_name,
        'row': 1,
        'col': len(id_vars),
        'formula': var_formula
    })

    # Value column: cycle through source value columns
    # =ARRAYFORMULA(CHOOSE(MOD(ROW(INDIRECT("1:"&ROWS(col)*k))-1, k)+1, col_v1, col_v2, ...))
    value_refs = []
    for v_col in value_vars:
        v_idx = input_schema.index(v_col)
        value_refs.append(_col_to_range_ref(input_sheet, input_range, v_col, v_idx, data_only=True))

    value_refs_str = ", ".join(value_refs)
    value_formula = (
        f'=ARRAYFORMULA(CHOOSE(MOD(ROW(INDIRECT("1:"&ROWS({first_id_ref})*{k}))-1, {k})+1, '
        f'{value_refs_str}))'
    )

    operations.append({
        'type': 'set_formula',
        'sheet': sheet_name,
        'row': 1,
        'col': len(id_vars) + 1,
        'formula': value_formula
    })

    output_range = Range(row=0, col=0, row_end=num_output_rows + 1, col_end=num_output_cols - 1)

    return TranslationResult(operations, sheet_name, output_range)


def translate_window(op: Window, counter: int, input_sheet: str, input_range: Range,
                    input_schema: List[str]) -> TranslationResult:
    """Translate Window operation using per-row formulas.

    Supports three categories of window functions:
    - Ranking (rank, row_number): COUNTIFS-based formulas
    - Running aggregates (sum, mean, min, max, count): conditional aggregate functions
      (SUMIFS, AVERAGEIFS, COUNTIFS, MINIFS, MAXIFS)
    - Lag/lead: IFERROR(OFFSET(...)) formulas

    Unsupported frame specs raise UnsupportedOperationError.

    Args:
        op: Window operation
        counter: Sheet counter
        input_sheet: Input sheet name
        input_range: Input data range
        input_schema: Input column names

    Returns:
        (operations, sheet_name, output_range)
    """
    sheet_name = _generate_sheet_name(op, counter)

    output_schema = input_schema + [op.output_column]
    num_cols = len(output_schema)
    num_rows = input_range.row_end - input_range.row + 1
    window_col_idx = len(input_schema)  # appended column position

    operations = []

    # Create sheet
    operations.append({
        'type': 'create_sheet',
        'name': sheet_name,
        'rows': num_rows,
        'cols': num_cols
    })

    # Write header
    operations.append({
        'type': 'set_values',
        'sheet': sheet_name,
        'row': 0,
        'col': 0,
        'values': [output_schema]
    })

    # Copy existing columns via array formulas
    for j, col_name in enumerate(input_schema):
        col_idx = input_schema.index(col_name)
        col_ref = _col_to_range_ref(input_sheet, input_range, col_name, col_idx, data_only=True)

        operations.append({
            'type': 'set_formula',
            'sheet': sheet_name,
            'row': 1,
            'col': j,
            'formula': f"=ARRAYFORMULA({col_ref})"
        })

    # Determine window function category and generate per-row formulas
    ranking_funcs = {'rank', 'row_number'}
    running_agg_funcs = {'sum', 'mean', 'min', 'max', 'count'}
    lag_lead_funcs = {'lag', 'lead'}

    data_rows = input_range.row_end - input_range.row  # number of data rows

    # Create context for window helper functions
    ctx = WindowTranslationContext(
        op, operations, sheet_name, input_sheet, input_range,
        input_schema, window_col_idx, data_rows
    )

    if op.function in ranking_funcs:
        _translate_window_ranking(ctx)
    elif op.function in running_agg_funcs:
        _translate_window_running_agg(ctx)
    elif op.function in lag_lead_funcs:
        _translate_window_lag_lead(ctx)
    else:
        raise UnsupportedOperationError(
            f"Window function '{op.function}' cannot be expressed as a spreadsheet formula. "
            "Supported functions: rank, row_number, sum, mean, min, max, count, lag, lead."
        )

    output_range = Range(row=0, col=0, row_end=num_rows, col_end=num_cols - 1)

    return TranslationResult(operations, sheet_name, output_range)


def _translate_window_ranking(ctx: WindowTranslationContext) -> None:
    """Generate COUNTIFS-based ranking formulas for each data row.

    For ``rank``, uses ``<=`` (or ``>=`` for descending) so tied values share
    the same rank.  For ``row_number``, uses strict ``<`` (or ``>``) and adds
    a tie-break COUNTIFS that compares ROW positions, guaranteeing unique
    sequential numbers.
    """
    is_row_number = ctx.op.function == 'row_number'

    for i in range(ctx.data_rows):
        row_in_sheet = 2 + i  # 1-indexed, row 1 is header, data starts at row 2

        countifs_args = []
        partition_args: List[str] = []

        for part_col in ctx.op.partition_by:
            p_idx = ctx.input_schema.index(part_col)
            part_range = _col_to_range_ref(ctx.input_sheet, ctx.input_range, part_col, p_idx, data_only=True)
            part_cell_letter = Range._col_to_letter(ctx.input_range.col + p_idx)
            part_pair = f"{part_range}, {ctx.sheet_name}!{part_cell_letter}{row_in_sheet}"
            countifs_args.append(part_pair)
            partition_args.append(part_pair)

        for order_col, order_dir in ctx.op.order_by:
            o_idx = ctx.input_schema.index(order_col)
            order_range = _col_to_range_ref(ctx.input_sheet, ctx.input_range, order_col, o_idx, data_only=True)
            order_cell_letter = Range._col_to_letter(ctx.input_range.col + o_idx)

            if is_row_number:
                op_str = "<" if order_dir == 'asc' else ">"
            else:
                op_str = "<=" if order_dir == 'asc' else ">="

            countifs_args.append(
                f'{order_range}, "{op_str}"&{ctx.sheet_name}!{order_cell_letter}{row_in_sheet}'
            )

        args_str = ", ".join(countifs_args)

        if is_row_number:
            tiebreak_args = list(partition_args)
            for order_col, _order_dir in ctx.op.order_by:
                o_idx = ctx.input_schema.index(order_col)
                order_range = _col_to_range_ref(
                    ctx.input_sheet, ctx.input_range, order_col, o_idx, data_only=True,
                )
                order_cell_letter = Range._col_to_letter(ctx.input_range.col + o_idx)
                cell_ref = f"{ctx.sheet_name}!{order_cell_letter}{row_in_sheet}"
                tiebreak_args.append(f"{order_range}, {cell_ref}")
                tiebreak_args.append(
                    f'ROW({order_range}), "<="&ROW({cell_ref})'
                )
            tiebreak_str = ", ".join(tiebreak_args)
            formula = f"=COUNTIFS({args_str})+COUNTIFS({tiebreak_str})"
        else:
            formula = f"=COUNTIFS({args_str})"

        ctx.operations.append({
            'type': 'set_formula',
            'sheet': ctx.sheet_name,
            'row': row_in_sheet - 1,  # 0-indexed for the operation dict
            'col': ctx.window_col_idx,
            'formula': formula
        })


def _translate_window_running_agg(ctx: WindowTranslationContext) -> None:
    """Generate conditional aggregate formulas (SUMIFS, AVERAGEIFS, etc.) for each data row."""
    # Only support unbounded preceding to current row frame
    if ctx.op.frame and ctx.op.frame not in ('unbounded preceding to current row', None):
        raise UnsupportedOperationError(
            f"Window frame '{ctx.op.frame}' not supported. "
            "Only 'unbounded preceding to current row' is supported."
        )

    func_map = {
        'sum': 'SUMIFS',
        'mean': 'AVERAGEIFS',
        'count': 'COUNTIFS',
        'min': 'MINIFS',
        'max': 'MAXIFS',
    }
    sheets_func = func_map[ctx.op.function]

    input_col = ctx.op.input_column
    if not input_col:
        raise UnsupportedOperationError(
            f"Running aggregate window function '{ctx.op.function}' requires an input column"
        )

    c_idx = ctx.input_schema.index(input_col)
    agg_range = _col_to_range_ref(ctx.input_sheet, ctx.input_range, input_col, c_idx, data_only=True)

    for i in range(ctx.data_rows):
        row_in_sheet = 2 + i

        criteria_args = []

        # For SUMIFS/etc., the first arg is the sum_range, then pairs of (criteria_range, criteria)
        # Partition key criteria
        for part_col in ctx.op.partition_by:
            p_idx = ctx.input_schema.index(part_col)
            part_range = _col_to_range_ref(ctx.input_sheet, ctx.input_range, part_col, p_idx, data_only=True)
            part_cell_letter = Range._col_to_letter(ctx.input_range.col + p_idx)
            criteria_args.append(
                f"{part_range}, {ctx.sheet_name}!{part_cell_letter}{row_in_sheet}"
            )

        # Order key criteria (include rows up to current row)
        for order_col, order_dir in ctx.op.order_by:
            o_idx = ctx.input_schema.index(order_col)
            order_range = _col_to_range_ref(ctx.input_sheet, ctx.input_range, order_col, o_idx, data_only=True)
            order_cell_letter = Range._col_to_letter(ctx.input_range.col + o_idx)
            op_str = "<=" if order_dir == 'asc' else ">="
            criteria_args.append(
                f'{order_range}, "{op_str}"&{ctx.sheet_name}!{order_cell_letter}{row_in_sheet}'
            )

        criteria_str = ", ".join(criteria_args)
        formula = f"={sheets_func}({agg_range}, {criteria_str})"

        ctx.operations.append({
            'type': 'set_formula',
            'sheet': ctx.sheet_name,
            'row': row_in_sheet - 1,
            'col': ctx.window_col_idx,
            'formula': formula
        })


def _translate_window_lag_lead(ctx: WindowTranslationContext) -> None:
    """Generate IFERROR(OFFSET(...)) formulas for lag/lead window functions."""
    if ctx.op.partition_by:
        raise UnsupportedOperationError(
            f"Partition-aware {ctx.op.function} cannot be expressed as a spreadsheet "
            "formula because OFFSET would cross partition boundaries. "
            "Remove partition_by or compute this column in pandas."
        )

    input_col = ctx.op.input_column
    if not input_col:
        raise UnsupportedOperationError(
            f"Window function '{ctx.op.function}' requires an input column"
        )

    # Extract offset from frame spec (default: 1)
    offset = 1
    if ctx.op.frame:
        try:
            offset = int(ctx.op.frame)
        except ValueError:
            offset = 1

    c_idx = ctx.input_schema.index(input_col)
    input_col_in_output = c_idx  # position in the output sheet

    for i in range(ctx.data_rows):
        row_in_sheet = 2 + i

        # Cell holding the input column value for the current row
        col_letter = Range._col_to_letter(1 + input_col_in_output)  # 1-indexed
        cell_ref = f"{ctx.sheet_name}!{col_letter}{row_in_sheet}"

        # delta: +offset for lead, -offset for lag
        delta = offset if ctx.op.function == 'lead' else -offset

        formula = f'=IFERROR(OFFSET({cell_ref}, {delta}, 0), "")'

        ctx.operations.append({
            'type': 'set_formula',
            'sheet': ctx.sheet_name,
            'row': row_in_sheet - 1,
            'col': ctx.window_col_idx,
            'formula': formula
        })
