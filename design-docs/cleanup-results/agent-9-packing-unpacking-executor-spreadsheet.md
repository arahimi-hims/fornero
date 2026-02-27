# Agent 9: Packing/Unpacking Analysis - Executor & Spreadsheet

**Analysis Date:** 2026-02-26
**Scope:** `src/fornero/executor/`, `src/fornero/spreadsheet/`
**Objective:** Identify and eliminate unnecessary conversions between collection types

---

## Executive Summary

After thorough analysis of the executor and spreadsheet modules, I identified **3 distinct patterns** where unnecessary conversions occur between collection types. These patterns involve tuple packing/unpacking, intermediate dictionary conversions, and redundant object-to-string-to-object conversions at function boundaries.

**Total LOC Savings: 6-8 lines**
**Files Affected: 3 files**

---

## Findings

### Pattern 1: Tuple Unpacking in `_sheet_dims` Dictionary

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
Store rows and cols in separate dictionaries or use a simple dataclass/named tuple:

```python
# Option 1: Separate dictionaries
self._sheet_rows: dict[str, int] = {}
self._sheet_cols: dict[str, int] = {}

# Option 2: Named tuple (better - more explicit)
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

### Pattern 2: Tuple Packing in `agg_map` Dictionary

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

### Pattern 3: Range-to-String-to-Reference Chain

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
- `Reference.__init__` accepts both `Range` objects and strings (line 345-359 in model.py)
- When given a `Range`, it immediately converts to A1 notation via `to_a1()`
- The intermediate `Reference` object is created solely to call `to_string()`
- **ALL usages** in strategies.py (3 call sites) follow this pattern: create Reference just to call `to_string()`

**Relevant Code Context:**
```python
# model.py lines 345-362
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
| `LocalExecutor._sheet_dims` | `dict[str, tuple[int, int]]` with unpacking at all sites | Separate dicts or named tuple with direct access | 2 | `local_executor.py` |
| `_gsheets_query.agg_map` | `dict[str, tuple[str, str]]` with selective unpacking | Two separate dicts: `agg_funcs` and `agg_labels` | 2-3 | `gsheets_functions.py` |
| `Reference(Range).to_string()` chain | Range → string → Reference → string | Static method or direct formatting | 3-6 | `model.py`, `strategies.py` |
| **TOTAL** | | | **6-8** | **3** |

---

## Additional Observations

### Non-Issues Found (Patterns That Are Appropriate)

1. **`_sort_coerce` returning tuples** (gsheets_functions.py:157, 159)
   - Returns `(type_indicator, value)` tuple
   - Used directly as sort key - never unpacked
   - **Status:** Appropriate - tuple is used as composite sort key

2. **`ExecutionStep.to_dict()` / `from_dict()` pattern** (plan.py)
   - Serialization/deserialization for JSON compatibility
   - Not used in hot paths
   - **Status:** Appropriate - needed for serialization

3. **`_build_a1_cell` internal calls in `_build_a1_range`** (sheets_executor.py:453-454)
   - Composes two cell references into a range
   - Clear separation of concerns
   - **Status:** Appropriate - good abstraction

4. **`ops_by_sheet` dictionary grouping** (sheets_executor.py:232-241)
   - Groups operations by sheet name for batch processing
   - Essential for the algorithm's correctness
   - **Status:** Appropriate - necessary for batching logic

---

## Recommendations

### Priority 1: Implement Pattern 2 (agg_map)
- **Rationale:** Clearest win with minimal risk
- **Effort:** Low (single function, ~10 lines changed)
- **Benefit:** Immediate clarity improvement

### Priority 2: Implement Pattern 1 (_sheet_dims)
- **Rationale:** Local change with good clarity improvement
- **Effort:** Low (single class, ~5 lines changed)
- **Benefit:** Consider using a named tuple/dataclass for semantic clarity

### Priority 3: Implement Pattern 3 (Reference chain)
- **Rationale:** Spans multiple modules, needs careful consideration
- **Effort:** Medium (touches API boundary between modules)
- **Benefit:** Good performance improvement in translator hot path
- **Recommendation:** Use static method approach (Option 1) to maintain abstraction

---

## Validation Checklist

For each proposed refactoring:
- [ ] All call sites identified and analyzed
- [ ] No call site uses the packed structure without unpacking
- [ ] Refactoring preserves all functionality
- [ ] Tests exist to verify correctness after changes
- [ ] Performance impact is neutral or positive

---

## Conclusion

This analysis identified **3 valid patterns** where unnecessary packing/unpacking occurs. All patterns have been verified by examining all call sites. The proposed refactorings are low-to-medium risk and provide clarity improvements with minimal code changes.

The executor and spreadsheet modules are generally well-designed, with most collection usage being appropriate and intentional. The patterns identified here represent opportunities for incremental improvement rather than fundamental design issues.
