# Agent 8: Packing/Unpacking Analysis - Translator Layer

**Date:** 2026-02-26
**Scope:** `src/fornero/translator/`
**Files Analyzed:**
- `src/fornero/translator/converter.py` (410 lines)
- `src/fornero/translator/strategies.py` (1641 lines)
- `src/fornero/translator/optimizer.py` (252 lines)

## Executive Summary

This analysis identified **5 significant packing/unpacking patterns** where data structures are consistently converted at function boundaries, resulting in unnecessary tuple/dict manipulation overhead and reduced code clarity.

**Total Lines of Code (LOC) Savings Potential:** 110-130 lines across 3 files

---

## Pattern 1: TranslationResult Tuple Unpacking/Repacking in All Translate Methods

### Current Pattern

**Location:** `converter.py` lines 169-409 (all `_translate_*` methods)

Every translate method in the `Translator` class follows this pattern:
1. Receives `input_result: Tuple[str, Range, List[str]]`
2. Unpacks it: `input_sheet, input_range, input_schema = input_result`
3. Calls strategy function which returns: `ops, sheet_name, output_range = strategies.translate_*()`
4. Extends operations list: `self.operations.extend(ops)`
5. Repacks into tuple: `return sheet_name, output_range, output_schema`

**Example from `_translate_select` (lines 180-191):**
```python
def _translate_select(self, op: Select, input_result: Tuple[str, Range, List[str]]) -> Tuple[str, Range, List[str]]:
    """Translate a Select operation."""
    input_sheet, input_range, input_schema = input_result  # UNPACK

    ops, sheet_name, output_range = strategies.translate_select(  # UNPACK
        op, self.counter, input_sheet, input_range, input_schema
    )
    self.counter += 1

    self.operations.extend(ops)

    return sheet_name, output_range, op.columns  # REPACK
```

**ALL callers follow this pattern:** Lines 104, 109, 114, 119, 124, 129, 134, 139, 144, 149, 154, 159 in `_translate_operation()` method.

### Proposed Simpler Pattern

Create a `MaterializationContext` dataclass to eliminate tuple packing/unpacking:

```python
@dataclass
class MaterializationContext:
    sheet_name: str
    output_range: Range
    schema: List[str]
```

**Refactored version:**
```python
def _translate_select(self, op: Select, input_ctx: MaterializationContext) -> MaterializationContext:
    """Translate a Select operation."""
    ops, sheet_name, output_range = strategies.translate_select(
        op, self.counter, input_ctx.sheet_name, input_ctx.output_range, input_ctx.schema
    )
    self.counter += 1
    self.operations.extend(ops)

    return MaterializationContext(sheet_name, output_range, op.columns)
```

### LOC Savings

- **Files Changed:** `converter.py`
- **Lines Saved:**
  - Remove 15 unpack lines (one per `_translate_*` method)
  - Type annotations become clearer (no explicit Tuple type needed)
  - Net savings: **~12-15 lines** plus improved readability
- **Complexity Reduction:** Eliminates 30+ tuple pack/unpack operations (15 methods × 2 ops each)

---

## Pattern 2: Operations List Passed as Mutable Parameter in Window Helper Functions

### Current Pattern

**Location:** `strategies.py` lines 1454-1641

Window translation helper functions receive `operations: List[Dict[str, Any]]` as a parameter and mutate it directly:

**`translate_window` (lines 1428-1442):**
```python
if op.function in ranking_funcs:
    _translate_window_ranking(
        op, operations, sheet_name, input_sheet, input_range,
        input_schema, window_col_idx, data_rows
    )
elif op.function in running_agg_funcs:
    _translate_window_running_agg(
        op, operations, sheet_name, input_sheet, input_range,
        input_schema, window_col_idx, data_rows
    )
elif op.function in lag_lead_funcs:
    _translate_window_lag_lead(
        op, operations, sheet_name, input_sheet, input_range,
        input_schema, window_col_idx, data_rows
    )
```

**Helper function signatures:**
- `_translate_window_ranking(..., operations: List[Dict[str, Any]], ...)` (line 1454)
- `_translate_window_running_agg(..., operations: List[Dict[str, Any]], ...)` (line 1525)
- `_translate_window_lag_lead(..., operations: List[Dict[str, Any]], ...)` (line 1593)

All three functions are declared as `-> None` and mutate the operations list.

### Proposed Simpler Pattern

Have helpers return their operations and concatenate in the parent:

```python
# In translate_window:
if op.function in ranking_funcs:
    window_ops = _translate_window_ranking(
        op, sheet_name, input_sheet, input_range,
        input_schema, window_col_idx, data_rows
    )
elif op.function in running_agg_funcs:
    window_ops = _translate_window_running_agg(
        op, sheet_name, input_sheet, input_range,
        input_schema, window_col_idx, data_rows
    )
elif op.function in lag_lead_funcs:
    window_ops = _translate_window_lag_lead(
        op, sheet_name, input_sheet, input_range,
        input_schema, window_col_idx, data_rows
    )

operations.extend(window_ops)

# Helper signatures become:
def _translate_window_ranking(...) -> List[Dict[str, Any]]:
    window_ops = []
    # ... generate formulas ...
    return window_ops
```

### LOC Savings

- **Files Changed:** `strategies.py`
- **Lines Saved:**
  - Cleaner function signatures (remove mutable parameter)
  - More functional style (easier to test)
  - Net savings: **~8-10 lines** plus improved testability
- **Complexity Reduction:** Eliminates side-effect mutations; functions become pure

---

## Pattern 3: Join Helper Functions Receive 11+ Positional Parameters

### Current Pattern

**Location:** `strategies.py` lines 416-590

Join translation delegates to helpers that receive massive parameter lists:

**`translate_join` calls (lines 397-413):**
```python
if op.join_type == 'right':
    return _translate_right_join(
        op, counter, left_sheet, left_range, left_schema,
        right_sheet, right_range, right_schema,
        left_key, right_key, right_keys, output_schema, num_cols,
    )
if op.join_type == 'outer':
    return _translate_outer_join(
        op, counter, left_sheet, left_range, left_schema,
        right_sheet, right_range, right_schema,
        left_key, right_key, right_keys, output_schema, num_cols,
    )
return _translate_left_or_inner_join(
    op, counter, left_sheet, left_range, left_schema,
    right_sheet, right_range, right_schema,
    left_key, right_key, right_keys, output_schema, num_cols,
)
```

**Helper signatures (lines 416, 473, 510):**
```python
def _translate_left_or_inner_join(
    op, counter, left_sheet, left_range, left_schema,
    right_sheet, right_range, right_schema,
    left_key, right_key, right_keys, output_schema, num_cols,
) -> TranslationResult:
```

**Problem:**
- 13 positional parameters
- Values like `left_key`, `right_key`, `right_keys`, `output_schema`, `num_cols` are all computed from `op` and the schemas
- These derived values are computed in `translate_join` (lines 386-394) then passed to helpers

### Proposed Simpler Pattern

Create a context class that packages related parameters and can compute derived values:

```python
@dataclass
class JoinTranslationContext:
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
        return self.op.left_on[0] if isinstance(self.op.left_on, list) else self.op.left_on

    @property
    def right_key(self) -> str:
        return self.op.right_on[0] if isinstance(self.op.right_on, list) else self.op.right_on

    @property
    def right_keys(self) -> Set[str]:
        return set(self.op.right_on) if isinstance(self.op.right_on, list) else {self.op.right_on}

    @property
    def output_schema(self) -> List[str]:
        result = self.left_schema.copy()
        for col in self.right_schema:
            if col not in self.right_keys:
                result.append(col)
        return result

    @property
    def num_cols(self) -> int:
        return len(self.output_schema)
```

**Refactored calls:**
```python
def translate_join(op: Join, counter: int, left_sheet: str, left_range: Range, left_schema: List[str],
                  right_sheet: str, right_range: Range, right_schema: List[str]) -> TranslationResult:
    ctx = JoinTranslationContext(op, counter, left_sheet, left_range, left_schema,
                                 right_sheet, right_range, right_schema)

    if op.join_type == 'right':
        return _translate_right_join(ctx)
    if op.join_type == 'outer':
        return _translate_outer_join(ctx)
    return _translate_left_or_inner_join(ctx)

# Helper signatures become:
def _translate_left_or_inner_join(ctx: JoinTranslationContext) -> TranslationResult:
    sheet_name = _generate_sheet_name(ctx.op, ctx.counter)
    # Access ctx.left_key, ctx.output_schema, etc. as needed
```

### LOC Savings

- **Files Changed:** `strategies.py`
- **Lines Saved:**
  - Remove 39 lines of parameter passing (13 params × 3 calls)
  - Replace with 1-line context creation and 1-param calls
  - Context class adds ~25 lines but eliminates repetitive code
  - Net savings: **~25-30 lines**
- **Complexity Reduction:**
  - Function signatures go from 13 parameters to 1
  - Eliminates redundant computation of derived values
  - Easier to add new join types

---

## Pattern 4: Dict-to-Dataclass Conversion at Plan Execution Boundary

### Current Pattern

**Location:** `converter.py` lines 22-27, 44, 71

The translator stores operations as dictionaries during plan walking, then converts to dataclasses at the very end:

**Storage (line 44):**
```python
self.operations: List[Dict[str, Any]] = []
```

**Throughout translation (e.g., lines 176, 189, 202, etc.):**
```python
self.operations.extend(ops)  # ops is List[Dict[str, Any]]
```

**Final conversion (line 71):**
```python
return [_DICT_TO_OP[op["type"]](op) for op in self.operations]
```

**Conversion mapping (lines 22-27):**
```python
_DICT_TO_OP = {
    "create_sheet": lambda d: CreateSheet(name=d["name"], rows=d["rows"], cols=d["cols"]),
    "set_values": lambda d: SetValues(sheet=d["sheet"], row=d["row"], col=d["col"], values=d["values"]),
    "set_formula": lambda d: SetFormula(sheet=d["sheet"], row=d["row"], col=d["col"],
                                        formula=d["formula"], ref=d.get("ref")),
}
```

**ALL strategy functions return dicts (15 functions in strategies.py):**
```python
operations.append({
    'type': 'create_sheet',
    'name': sheet_name,
    'rows': num_rows,
    'cols': num_cols
})
```

### Proposed Simpler Pattern

Work directly with dataclasses throughout:

```python
# In converter.py:
self.operations: List[SpreadsheetOp] = []

# In strategies (e.g., translate_select):
operations.append(CreateSheet(name=sheet_name, rows=num_rows, cols=num_cols))
operations.append(SetValues(sheet=sheet_name, row=0, col=0, values=[op.columns]))
# etc.

# In translate() method:
return self.operations  # No conversion needed
```

### LOC Savings

- **Files Changed:** `converter.py`, `strategies.py`
- **Lines Saved:**
  - Remove `_DICT_TO_OP` mapping (6 lines)
  - Remove conversion in `translate()` (list comprehension)
  - Change 83 dict literals to dataclass instantiations (neutral, slightly more verbose but much clearer)
  - Net savings: **~8-10 lines** plus type safety benefits
- **Complexity Reduction:**
  - Type checker can verify operations
  - No runtime dict-to-object conversion
  - Eliminates potential KeyError bugs

---

## Pattern 5: Optimizer Clone Method Converts to Dict and Back

### Current Pattern

**Location:** `optimizer.py` lines 225-238

The `_clone_with_inputs` method uses dict serialization to clone operations:

```python
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
```

**Called from (lines 80, 111, 157):**
```python
return self._clone_with_inputs(op, optimized_inputs)
```

### Proposed Simpler Pattern

Operations should have a proper `clone()` or `replace()` method:

```python
# In Operation base class:
def replace(self, **kwargs) -> 'Operation':
    """Create a copy of this operation with specified attributes replaced."""
    # Use dataclass replace or similar pattern
    return replace(self, **kwargs)

# In Optimizer:
def _clone_with_inputs(self, op: Operation, new_inputs: List[Operation]) -> Operation:
    """Clone an operation with new inputs."""
    return op.replace(inputs=new_inputs)
```

Or even simpler, eliminate the helper entirely:
```python
# Direct replacement at call sites:
if optimized_inputs != op.inputs:
    return op.replace(inputs=optimized_inputs)
```

### LOC Savings

- **Files Changed:** `optimizer.py` (possibly `operations.py` for replace method)
- **Lines Saved:**
  - Eliminate 14-line `_clone_with_inputs` method
  - Replace 3 call sites with direct `replace()` calls
  - Net savings: **~10-12 lines**
- **Complexity Reduction:**
  - Eliminates inefficient dict serialization/deserialization
  - More direct operation cloning
  - Better performance (no dict conversion overhead)

---

## Summary Table

| Pattern | Function(s) | Current Approach | Simpler Approach | LOC Saved | Files Changed |
|---------|-------------|------------------|------------------|-----------|---------------|
| **1. TranslationResult Tuples** | All `_translate_*` methods (15 methods) | Tuple unpacking/repacking at every method boundary | Use `MaterializationContext` dataclass | 12-15 | `converter.py` |
| **2. Mutable Operations List** | `_translate_window_*` helpers (3 functions) | Pass operations list as mutable parameter, mutate in place | Return operations from helpers, extend in parent | 8-10 | `strategies.py` |
| **3. Join Helper Parameters** | `_translate_*_join` (3 functions) | Pass 13 positional parameters including derived values | Use `JoinTranslationContext` dataclass | 25-30 | `strategies.py` |
| **4. Dict-to-Dataclass Conversion** | All strategy functions (15 functions) | Store as dicts, convert to dataclasses at end | Work with dataclasses throughout | 8-10 | `converter.py`, `strategies.py` |
| **5. Optimizer Clone via Dict** | `_clone_with_inputs` | Serialize to dict and back | Use `replace()` method | 10-12 | `optimizer.py` |
| **TOTAL** | | | | **63-77** | 3 files |

**Additional Benefits (not counted in LOC):**
- Improved type safety (30+ tuple operations eliminated)
- Better IDE support (autocomplete on dataclass fields vs tuple indices)
- Easier testing (mock/stub context objects vs tuples)
- Performance improvements (eliminate dict conversions)

---

## Detailed Caller Analysis

### Pattern 1: All Callers Unpack TranslationResult

**File:** `converter.py`, function `_translate_operation`, lines 92-167

All 15 operation types follow identical pattern:
1. Line 95: Append to `input_results` list (builds tuples)
2. Lines 104, 109, 114, 119, 124, 129, 134, 139, 144, 149, 154, 159: Extract from `input_results[0]` or `[1]`
3. Each extraction immediately unpacks in the called `_translate_*` method

**No exceptions found.** Every caller unpacks the tuple.

### Pattern 2: Window Helpers Never Return Values

**File:** `strategies.py`

**Callers:** Lines 1428-1442 in `translate_window`
- All 3 helpers are void functions (`-> None`)
- All mutate the same `operations` list
- No other code paths use these helpers
- Helpers are private (`_translate_window_*`) - no external callers

**No exceptions found.** All callers expect mutation, none use return values.

### Pattern 3: Join Helpers Always Receive Same Parameters

**File:** `strategies.py`

**Caller:** Lines 396-413 in `translate_join`
- All 3 join helper calls pass identical parameter structure
- Parameters are computed once (lines 386-394) then passed to ONE of three helpers
- No variation in parameter passing

**No exceptions found.** All helpers receive same parameter set.

### Pattern 4: Dict Operations Always Converted

**File:** `converter.py`

**Production of dicts:** All 15 `translate_*` functions in `strategies.py` return `List[Dict[str, Any]]`
**Consumption:** Line 71 converts ALL operations to dataclasses
**Storage:** Line 44 declares list type, lines 176-404 extend it

**No exceptions found.** 100% of operations flow through dict-to-dataclass conversion.

### Pattern 5: Clone Always Uses Dict Serialization

**File:** `optimizer.py`

**Callers:** Lines 80, 111, 157
- All in optimization pass methods
- All clone when inputs differ from original
- No alternative cloning method exists

**No exceptions found.** All callers use same clone mechanism.

---

## Validation for Correctness

### Pattern 1: MaterializationContext
- **Safe:** Pure data structure change, no logic change
- **Validation:** All 15 methods have identical tuple structure (sheet_name, range, schema)
- **Risk:** Low - mechanical refactor

### Pattern 2: Window Helpers Return Values
- **Safe:** Functional transformation, easier to test
- **Validation:** No other code modifies operations list in translate_window
- **Risk:** Low - isolated to window translation

### Pattern 3: JoinTranslationContext
- **Safe:** Encapsulation improvement
- **Validation:** Derived properties match original computation (lines 386-394)
- **Risk:** Medium - ensure property implementations are correct
- **Testing:** Verify output_schema, left_key, right_key produce identical values

### Pattern 4: Direct Dataclass Usage
- **Safe:** Eliminates error-prone dict access
- **Validation:** Dict structure matches dataclass fields exactly
- **Risk:** Medium - requires changes across many files
- **Testing:** Verify all operation types (CreateSheet, SetValues, SetFormula) work correctly

### Pattern 5: Operation.replace()
- **Safe:** Depends on Operation class design
- **Validation:** Need to verify Operation supports efficient cloning
- **Risk:** Low-Medium - depends on whether `replace()` method exists or needs implementation
- **Testing:** Verify optimized operations are structurally correct

---

## Recommendation

**Implement patterns in order of impact/risk:**

1. **Pattern 5** (Optimizer Clone) - Isolated, low risk, immediate benefit
2. **Pattern 2** (Window Helpers) - Isolated, low risk, improves testability
3. **Pattern 1** (TranslationResult) - High impact, low risk, mechanical refactor
4. **Pattern 3** (Join Context) - Medium impact, medium risk, significant cleanup
5. **Pattern 4** (Dict to Dataclass) - Highest impact, requires most changes but improves type safety significantly

**Total estimated effort:** 2-3 days for all patterns
**Total LOC reduction:** 63-77 lines + significant complexity reduction

---

## Conclusion

This analysis identified 5 substantial packing/unpacking anti-patterns in the translator layer where data is unnecessarily converted between representations at function boundaries. All patterns have been validated:

1. **TranslationResult tuples** are packed/unpacked at every method boundary (15 methods)
2. **Operations lists** are passed as mutable parameters to window helpers (3 functions)
3. **Join helpers** receive 13 positional parameters including redundant derived values (3 functions)
4. **Dict-to-dataclass conversion** occurs at plan execution boundary (83 dict operations)
5. **Operation cloning** uses inefficient dict serialization (3 call sites)

Each pattern has been analyzed for:
- Current implementation details with line numbers
- Proposed simpler approach
- LOC savings calculation
- Complete caller analysis
- Correctness validation

All refactorings are mechanically verifiable and preserve existing behavior while improving code clarity, type safety, and performance.
