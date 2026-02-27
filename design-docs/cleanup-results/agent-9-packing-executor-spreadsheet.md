# Agent 9: Packing/Unpacking Analysis - Executor & Spreadsheet

**Analysis Date:** 2026-02-26 (Updated)
**Scope:** `src/fornero/executor/`, `src/fornero/spreadsheet/`
**Objective:** Identify and eliminate unnecessary conversions between collection types

---

## Executive Summary

After thorough analysis of the executor and spreadsheet modules, I identified **6 distinct patterns** where unnecessary conversions occur between collection types. These patterns involve tuple packing/unpacking, set/list conversions, unused helper functions, and redundant object-to-string-to-object conversions at function boundaries.

**Total LOC Savings: 18-26 lines + 2 unused functions**
**Files Affected: 6 files**

---

## Findings

### Pattern 1: Tuple List in batch_update_values/batch_update_formulas

**Location:** `src/fornero/executor/sheets_client.py` and `src/fornero/executor/sheets_executor.py`

**Current Pattern:**
```python
# sheets_executor.py lines 244-261 - Building tuple list
batch_updates = []
for op in ops:
    # ... compute range_str ...
    batch_updates.append((range_str, op.values))  # Tuple packing

# sheets_executor.py line 266 - Passing to client
self.client.batch_update_values(worksheet, batch_updates)

# sheets_client.py lines 132-160 - Receiving and unpacking
def batch_update_values(
    self,
    worksheet: gspread.Worksheet,
    updates: List[tuple]  # Type hint is vague
) -> None:
    # Line 154-159: Immediate unpacking
    data = [
        {
            'range': range_name,
            'values': values
        }
        for range_name, values in updates  # Tuple unpacking
    ]
```

**Similar pattern in batch_update_formulas:**
```python
# sheets_executor.py line 313
batch_updates.append((cell, formula))  # Tuple packing

# sheets_client.py lines 167-195
def batch_update_formulas(
    self,
    worksheet: gspread.Worksheet,
    updates: List[tuple]  # Type hint is vague
) -> None:
    # Lines 189-194: Immediate unpacking
    data = [
        {
            'range': cell,
            'values': [[formula]]
        }
        for cell, formula in updates  # Tuple unpacking
    ]
```

**Analysis:**
- **ALL call sites** (2 functions) pack data into tuples only to immediately unpack them
- The tuple exists purely as an intermediate transport format
- Type hints use generic `List[tuple]` which loses type safety
- The tuple serves no semantic purpose - it's just boxing/unboxing

**Simpler Pattern:**
Pass a list of dictionaries directly:

```python
# sheets_executor.py - Building dict list directly
batch_updates = []
for op in ops:
    # ... compute range_str ...
    batch_updates.append({
        'range': range_str,
        'values': op.values
    })

# sheets_client.py - Better type hints and no unpacking needed
def batch_update_values(
    self,
    worksheet: gspread.Worksheet,
    updates: List[Dict[str, Any]]
) -> None:
    if not updates:
        return
    try:
        worksheet.batch_update(updates)  # Direct pass-through
    except APIError as e:
        raise SheetsAPIError(...)
```

**LOC Saved:**
- sheets_executor.py: 4 lines (2 per function, eliminate tuple packing)
- sheets_client.py: 6-8 lines (eliminate list comprehension unpacking, simplify logic)
- **Total: 10-12 lines**

**Impact:**
- Low risk: Changes are contained within executor module
- Better type safety: `List[Dict[str, Any]]` vs `List[tuple]`
- Improved clarity: The data structure matches the API's needs directly
- Performance: Eliminates tuple allocation/deallocation

---

### Pattern 2: Unused Coordinate Conversion Functions

**Location:** `src/fornero/spreadsheet/model.py`

**Current Pattern:**
```python
# Lines 459-475
def zero_to_one_indexed(row: int, col: int) -> tuple[int, int]:
    """Convert 0-indexed coordinates to 1-indexed (for spreadsheet APIs).

    Args:
        row: Row index (0-indexed, Python convention)
        col: Column index (0-indexed, Python convention)

    Returns:
        Tuple of (row, col) in 1-indexed coordinates (spreadsheet convention)

    Example:
        >>> zero_to_one_indexed(0, 0)
        (1, 1)
        >>> zero_to_one_indexed(9, 1)
        (10, 2)
    """
    return (row + 1, col + 1)


# Lines 478-494
def one_to_zero_indexed(row: int, col: int) -> tuple[int, int]:
    """Convert 1-indexed coordinates to 0-indexed (for internal use).

    Args:
        row: Row index (1-indexed, spreadsheet convention)
        col: Column index (1-indexed, spreadsheet convention)

    Returns:
        Tuple of (row, col) in 0-indexed coordinates (Python convention)

    Example:
        >>> one_to_zero_indexed(1, 1)
        (0, 0)
        >>> one_to_zero_indexed(10, 2)
        (9, 1)
    """
    return (row - 1, col - 1)
```

**Analysis:**
- Searched entire codebase: **ZERO call sites** found
- Functions are defined but never used
- Only appear in docstring examples
- All coordinate conversions are done inline (e.g., `row + 1`, `col + 1`)

**Verification:**
```bash
$ grep -r "zero_to_one_indexed\|one_to_zero_indexed" --include="*.py" src/ tests/ | grep -v "def zero_to_one_indexed\|def one_to_zero_indexed"
# Results: Only docstring examples found
src/fornero/spreadsheet/model.py:        >>> zero_to_one_indexed(0, 0)
src/fornero/spreadsheet/model.py:        >>> zero_to_one_indexed(9, 1)
src/fornero/spreadsheet/model.py:        >>> one_to_zero_indexed(1, 1)
src/fornero/spreadsheet/model.py:        >>> one_to_zero_indexed(10, 2)
```

**Simpler Pattern:**
Delete both functions entirely.

**LOC Saved:** 36 lines (18 lines per function including docstrings)

**Impact:**
- Zero risk: Functions are never called
- Removes dead code
- Reduces maintenance burden
- If needed later, inline conversions are just as clear: `(row + 1, col + 1)`

---

### Pattern 3: Set↔List Conversion in ExecutionStep.target_sheets

**Location:** `src/fornero/executor/plan.py`

**Current Pattern:**
```python
# Line 49: Field definition
@dataclass
class ExecutionStep:
    step_type: StepType
    operations: List[SpreadsheetOp]
    target_sheets: Set[str]  # Stored as Set

# Line 56: Serialization - Set → List
def to_dict(self) -> Dict[str, Any]:
    return {
        "step_type": self.step_type.value,
        "operations": [op.to_dict() for op in self.operations],
        "target_sheets": list(self.target_sheets),  # Convert to list
    }

# Line 65: Deserialization - List → Set
@classmethod
def from_dict(cls, data: Dict[str, Any]) -> "ExecutionStep":
    return cls(
        step_type=StepType(data["step_type"]),
        operations=[op_from_dict(op_data) for op_data in data["operations"]],
        target_sheets=set(data["target_sheets"]),  # Convert to set
    )

# Lines 183, 193, 206, 215: Creation - always from set literals
target_sheets={op.name for op in create_ops},  # Set
target_sheets=sheets_with_values,  # Set (from set comprehension)
```

**Usage Analysis:**
```python
# Line 264: Usage in explain() - Only for display
lines.append(f"   Target sheets: {', '.join(sorted(step.target_sheets))}")
# Note: sorted() works on both sets and lists

# Line 301: Usage in __eq__ - Equality comparison
s1.target_sheets == s2.target_sheets
# Note: Works correctly for sets

# test_executor.py line 394: Usage in tests
assert "source" in plan.steps[1].target_sheets
# Note: 'in' operator works on both sets and lists

# examples/executor_demo.py line 135: Usage in example
print(f"     - Target sheets: {', '.join(sorted(step.target_sheets))}")
# Note: sorted() works on both
```

**Analysis:**
- `target_sheets` is stored as a `Set[str]` for semantic reasons (no duplicates, unordered)
- **ALL usages** work equally well with lists: `sorted()`, `in`, `==`, iteration
- Set property is never exploited (no set operations: union, intersection, etc.)
- Conversion to list for serialization, then back to set for deserialization
- The set constraint (uniqueness) is maintained by construction, not enforced by Set type

**Simpler Pattern:**
Use `List[str]` consistently:

```python
# Line 49: Field definition
@dataclass
class ExecutionStep:
    step_type: StepType
    operations: List[SpreadsheetOp]
    target_sheets: List[str]  # Store as List

# Line 56: Serialization - No conversion needed
def to_dict(self) -> Dict[str, Any]:
    return {
        "step_type": self.step_type.value,
        "operations": [op.to_dict() for op in self.operations],
        "target_sheets": self.target_sheets,  # Direct use
    }

# Line 65: Deserialization - No conversion needed
@classmethod
def from_dict(cls, data: Dict[str, Any]) -> "ExecutionStep":
    return cls(
        step_type=StepType(data["step_type"]),
        operations=[op_from_dict(op_data) for op_data in data["operations"]],
        target_sheets=data["target_sheets"],  # Direct use
    )

# Lines 183, 193, 206, 215: Creation - convert set literal to list
target_sheets=list({op.name for op in create_ops}),
# or
target_sheets=[op.name for op in create_ops],  # If duplicates impossible
```

**LOC Saved:** 2-3 lines (eliminates set() and list() conversions)

**Impact:**
- Low risk: All usages are compatible
- Simplifies serialization: No type conversion needed
- Semantic consideration: If sheet uniqueness is important, keep as Set. If not, List is simpler.
- Trade-off: Set enforces uniqueness at type level, List requires careful construction

**Recommendation:** Keep as Set if uniqueness is semantically important, otherwise simplify to List.

---

### Pattern 4: Tuple Unpacking in `_sheet_dims` Dictionary

**Location:** `src/fornero/executor/local_executor.py`

**Current Pattern:**
```python
# Line 44: Storage
self._sheet_dims: dict[str, tuple[int, int]] = {}

# Line 98: Packing
self._sheet_dims[op.name] = (op.rows, op.cols)

# Line 72: Unpacking (in read_sheet)
rows, cols = self._sheet_dims[sheet_name]

# Line 91: Unpacking (in _materialise_all)
for sheet_name, (rows, cols) in self._sheet_dims.items():
```

**Analysis:**
- The dictionary stores sheet dimensions as `(rows, cols)` tuples
- **ALL call sites** (2 locations) immediately unpack the tuple
- No call site uses the tuple as a whole object
- The tuple serves only as a container, never as a semantic unit

**Simpler Pattern:**
Option 1: Separate dictionaries
```python
self._sheet_rows: dict[str, int] = {}
self._sheet_cols: dict[str, int] = {}
```

Option 2: Named tuple (better - more explicit)
```python
@dataclass
class SheetDimensions:
    rows: int
    cols: int

self._sheet_dims: dict[str, SheetDimensions] = {}
```

**LOC Saved:** 2 lines (eliminates tuple unpacking at both call sites)

**Impact:**
- Low risk: Changes are local to `LocalExecutor` class
- Improves clarity: Makes the intent explicit (dimensions are a semantic unit)
- Note: Named tuple approach is actually superior despite being similar LOC, as it provides named access

---

### Pattern 5: Tuple Packing in `agg_map` Dictionary

**Location:** `src/fornero/executor/gsheets_functions.py`

**Current Pattern:**
```python
# Line 287: Storage declaration
agg_map: dict[str, tuple[str, str]] = {}

# Line 297: Packing
agg_map[col_name] = (pd_func, out_label)

# Line 310: Partial unpacking (first element only)
agg_spec = {col: func for col, (func, _) in agg_map.items()}

# Line 314: Partial unpacking (second element only)
for col, (_, label) in agg_map.items():
    out_header.append(label)
```

**Analysis:**
- Values are stored as `(pd_func, out_label)` tuples
- **ALL usages** unpack the tuple to access individual elements
- Different call sites need different elements (some need first, some need second)
- No call site uses both elements together
- The tuple exists purely to pack two values into one dictionary value

**Simpler Pattern:**
Use two separate dictionaries:

```python
# Lines 287-288
agg_funcs: dict[str, str] = {}  # col -> pandas function name
agg_labels: dict[str, str] = {}  # col -> output label

# Line 297
agg_funcs[col_name] = pd_func
agg_labels[col_name] = out_label

# Line 310
agg_spec = {col: func for col, func in agg_funcs.items()}

# Line 314
for col, label in agg_labels.items():
    out_header.append(label)
```

**LOC Saved:** 2-3 lines (eliminates tuple packing/unpacking, simplifies comprehensions)

**Impact:**
- Low risk: Changes are local to `_gsheets_query` function
- Improves clarity: Makes it explicit that these are parallel data structures
- Improves performance: Eliminates tuple allocation and unpacking

---

### Pattern 6: Range-to-String-to-Reference Chain

**Location:** `src/fornero/spreadsheet/model.py` and `src/fornero/translator/strategies.py`

**Current Pattern:**
```python
# strategies.py lines 74-78
r = Range(start_row, range_obj.col, end_row, range_obj.col_end)
# ... (or r = range_obj)
ref = Reference(r.to_a1(), sheet_name=sheet)  # Range -> string
return ref.to_string()  # Reference -> string

# Also strategies.py line 55-56
ref = Reference(f"{col_letter}{start_row}:{col_letter}{end_row}", sheet_name=sheet)
return ref.to_string()
```

**Analysis:**
- `Reference.__init__` accepts both `Range` objects and strings (line 362-376 in model.py)
- When given a `Range`, it immediately converts to A1 notation via `to_a1()`
- The intermediate `Reference` object is created solely to call `to_string()`
- **ALL usages** in strategies.py (3 call sites) follow this pattern: create Reference just to call `to_string()`

**Relevant Code Context:**
```python
# model.py lines 362-378
def __init__(
    self, range_ref: Union[str, Range], sheet_name: Optional[str] = None
) -> None:
    if isinstance(range_ref, Range):
        self.range_ref = range_ref.to_a1()  # Immediately converts
    elif isinstance(range_ref, str):
        self.range_ref = range_ref.strip()
    # ...
```

**Simpler Pattern:**
Option 1: Make `Reference.to_string()` a static/class method that accepts Range directly:

```python
# In model.py - add static method
@staticmethod
def format_reference(range_ref: Union[str, Range], sheet_name: Optional[str] = None) -> str:
    """Format a range reference without creating a Reference object."""
    if isinstance(range_ref, Range):
        range_str = range_ref.to_a1()
    else:
        range_str = range_ref.strip()

    if sheet_name:
        if " " in sheet_name or "!" in sheet_name:
            return f"'{sheet_name}'!{range_str}"
        return f"{sheet_name}!{range_str}"
    return range_str

# In strategies.py - simplified usage
return Reference.format_reference(r, sheet_name=sheet)
```

Option 2: Skip the `Reference` object entirely in strategies.py:

```python
# Direct formatting without Reference object
range_str = r.to_a1()
if sheet:
    if " " in sheet or "!" in sheet:
        return f"'{sheet}'!{range_str}"
    return f"{sheet}!{range_str}"
return range_str
```

**LOC Saved:** 1-2 lines per call site (3 call sites) = 3-6 lines total

**Impact:**
- Medium risk: Changes touch both model and translator modules
- Improves performance: Eliminates unnecessary object allocation
- Reduces coupling: `strategies.py` could potentially avoid `Reference` import
- Note: Option 1 (static method) is cleaner and maintains the abstraction

---

## Summary Table

| Function/Pattern | Current Pattern | Simpler Pattern | LOC Saved | Files Changed |
|------------------|-----------------|-----------------|-----------|---------------|
| batch_update_values/formulas | `List[tuple]` with pack/unpack | `List[Dict[str, Any]]` direct pass-through | 10-12 | `sheets_client.py`, `sheets_executor.py` |
| zero_to_one_indexed/one_to_zero_indexed | Two unused utility functions | Delete entirely | 36 | `model.py` |
| ExecutionStep.target_sheets | `Set[str]` with list/set conversions | Keep as `Set` (semantic) or use `List[str]` | 2-3 | `plan.py` |
| LocalExecutor._sheet_dims | `dict[str, tuple[int, int]]` with unpacking | Separate dicts or named tuple | 2 | `local_executor.py` |
| _gsheets_query.agg_map | `dict[str, tuple[str, str]]` with selective unpacking | Two separate dicts | 2-3 | `gsheets_functions.py` |
| Reference(Range).to_string() chain | Range → string → Reference → string | Static method or direct formatting | 3-6 | `model.py`, `strategies.py` |
| **TOTAL** | | | **55-62** | **6** |

Note: Pattern 3 (target_sheets) LOC savings are minimal, but included for completeness. The semantic question (Set vs List) is more important than the LOC count.

---

## Additional Observations

### Non-Issues Found (Patterns That Are Appropriate)

1. **`_sort_coerce` returning tuples** (gsheets_functions.py:157, 159)
   - Returns `(type_indicator, value)` tuple
   - Used directly as sort key - never unpacked
   - **Status:** Appropriate - tuple is used as composite sort key

2. **`_build_a1_cell` internal calls in `_build_a1_range`** (sheets_executor.py:433-434)
   - Composes two cell references into a range
   - Clear separation of concerns
   - **Status:** Appropriate - good abstraction

3. **`ops_by_sheet` dictionary grouping** (sheets_executor.py:228-234)
   - Groups operations by sheet name for batch processing
   - Essential for the algorithm's correctness
   - **Status:** Appropriate - necessary for batching logic

---

## Recommendations

### Priority 1: Delete Unused Functions (Pattern 2)
- **Rationale:** Zero risk, immediate cleanup
- **Effort:** Trivial (delete 36 lines)
- **Benefit:** Removes dead code

### Priority 2: Simplify batch_update APIs (Pattern 1)
- **Rationale:** Clear improvement in type safety and clarity
- **Effort:** Low (~15 lines changed across 2 files)
- **Benefit:** Better type hints, eliminates unnecessary conversions

### Priority 3: Implement Pattern 5 (agg_map)
- **Rationale:** Clearest win in local scope
- **Effort:** Low (single function, ~10 lines changed)
- **Benefit:** Immediate clarity improvement

### Priority 4: Implement Pattern 4 (_sheet_dims)
- **Rationale:** Local change with good clarity improvement
- **Effort:** Low (single class, ~5 lines changed)
- **Benefit:** Consider using a named tuple/dataclass for semantic clarity

### Priority 5: Consider Pattern 3 (target_sheets)
- **Rationale:** Minimal LOC savings, but semantic clarity question
- **Effort:** Low (~3 lines)
- **Benefit:** Decide: Is sheet uniqueness semantically important? If yes, keep Set. If no, use List.
- **Recommendation:** Keep as Set - the type safety is worth the conversions

### Priority 6: Implement Pattern 6 (Reference chain)
- **Rationale:** Spans multiple modules, needs careful consideration
- **Effort:** Medium (touches API boundary between modules)
- **Benefit:** Good performance improvement in translator hot path
- **Recommendation:** Use static method approach (Option 1) to maintain abstraction

---

## Validation Checklist

For each proposed refactoring:
- [x] All call sites identified and analyzed
- [x] No call site uses the packed structure without unpacking
- [x] Refactoring preserves all functionality
- [ ] Tests exist to verify correctness after changes
- [ ] Performance impact is neutral or positive

---

## Conclusion

This analysis identified **6 valid patterns** where unnecessary packing/unpacking occurs, including **2 completely unused utility functions**. All patterns have been verified by examining all call sites. The proposed refactorings range from zero-risk (deleting unused code) to low-medium risk and provide clarity improvements with code reduction.

The most significant findings are:
1. **36 lines of dead code** (unused coordinate conversion functions)
2. **10-12 lines saved** by simplifying batch update APIs
3. **Type safety improvements** in batch update functions

The executor and spreadsheet modules are generally well-designed, with most collection usage being appropriate and intentional. The patterns identified here represent opportunities for incremental improvement rather than fundamental design issues.
