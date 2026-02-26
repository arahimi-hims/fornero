# Agent 2: Spreadsheet Algebra & Translation Semantics Analysis

**Date:** 2026-02-26
**Scope:** `src/fornero/spreadsheet/`, `src/fornero/translator/`
**Task:** Compare spreadsheet algebra and translation logic against formal semantics in `design-docs/ARCHITECTURE.md`

---

## Executive Summary

This analysis compares the implementation of spreadsheet algebra and dataframe-to-spreadsheet translation against the formal specification in ARCHITECTURE.md. The implementation is largely faithful to the specification with several notable discrepancies in coordinate systems, operation implementations, and translation strategies.

**Key Findings:**
- 8 spreadsheet algebra discrepancies (indexing, operation coverage)
- 12 translation strategy discrepancies (formula correctness, semantic deviations)
- Several optimization opportunities for alignment with spec

---

## 1. Spreadsheet Algebra Discrepancies

### 1.1 Coordinate System Mismatch

**File:** `src/fornero/spreadsheet/operations.py`
**Lines:** 56-63, 101-103
**Severity:** HIGH

**Issue:**
The `SetValues` and `SetFormula` operations use 0-indexed coordinates in implementation, but the architecture spec defines them as operating on 1-indexed coordinates for consistency with spreadsheet notation.

**Spec (§Spreadsheet Algebra):**
- SetValues: "$\mathcal{W}[s[r_0:r_0+m, c_0:c_0+n] \mapsto V]$" where coordinates are 1-indexed
- SetFormula: "$\mathcal{W}[s[r, c] \mapsto \varphi]$" with 1-indexed $(r, c)$

**Implementation:**
```python
@dataclass
class SetValues:
    sheet: str
    row: int      # 0-indexed in implementation
    col: int      # 0-indexed in implementation
    values: List[List[Any]]

@dataclass
class SetFormula:
    sheet: str
    row: int      # 0-indexed in implementation
    col: int      # 0-indexed in implementation
    formula: str
```

**Recommendation:**
Either update the implementation to use 1-indexed coordinates throughout (matching the spec), or update the spec to acknowledge the 0-indexed internal representation. The current mismatch creates confusion when reading code against the spec.

---

### 1.2 Range Model: 1-Indexed Inconsistency

**File:** `src/fornero/spreadsheet/model.py`
**Lines:** 68-95
**Severity:** MEDIUM

**Issue:**
The `Range` class uses 1-indexed coordinates, which is correct per the spec, but this creates a boundary mismatch with operations that use 0-indexed coordinates.

**Implementation:**
```python
class Range:
    """Represents a rectangular cell region in A1 notation.

    Attributes:
        row: Starting row (1-indexed)
        col: Starting column (1-indexed)
        row_end: Ending row (1-indexed, inclusive)
        col_end: Ending column (1-indexed, inclusive)
    """
```

**Recommendation:**
Document the coordinate conversion rules explicitly. The translator must convert between 0-indexed operation coordinates and 1-indexed Range coordinates. Consider adding utility methods for this conversion.

---

### 1.3 Missing NamedRange Support in Implementation

**File:** `src/fornero/spreadsheet/operations.py`
**Lines:** 129-173
**Severity:** LOW

**Issue:**
The `NamedRange` operation is defined but never actually used by the translator. The spec describes named ranges as an optimization for readability.

**Spec (§Spreadsheet Algebra, NamedRange):**
"After registration, $\ell$ can be used in any formula in place of the explicit range reference."

**Implementation:**
The operation exists but `translator/strategies.py` never generates `NamedRange` operations — all references use explicit sheet!range notation.

**Recommendation:**
Either implement named range generation in the translator for commonly referenced ranges, or remove the operation from the spec if it's not being used.

---

### 1.4 CreateSheet Validation Missing

**File:** `src/fornero/spreadsheet/operations.py`
**Lines:** 18-48
**Severity:** LOW

**Issue:**
The `CreateSheet` operation doesn't validate that dimensions are positive or that the sheet name is unique.

**Spec (§Spreadsheet Algebra, CreateSheet):**
"The operation is undefined if $s$ already exists."

**Implementation:**
```python
@dataclass
class CreateSheet:
    name: str
    rows: int
    cols: int
    # No validation logic
```

**Recommendation:**
Add validation in the operation class or document that validation is the executor's responsibility. The spec implies this should be caught at translation time.

---

### 1.5 Sheet Dimensions Specification

**File:** `src/fornero/spreadsheet/model.py`
**Lines:** 24-43
**Severity:** LOW

**Issue:**
The `Sheet` class tracks dimensions but this information isn't used by operations or the executor. The spec mentions sheets have a grid $\mathbf{0}_{m \times n}$ but operations grow grids dynamically.

**Spec (§Spreadsheet Algebra):**
"$\mathcal{W}[s \mapsto \mathbf{0}_{m \times n}]$" suggests fixed dimensions.

**Implementation:**
Google Sheets dynamically extends grids, so the `Sheet` dimensions are hints rather than constraints.

**Recommendation:**
Clarify in the spec that dimensions are initial allocations and may grow. Or remove dimension tracking from `Sheet` if it's purely advisory.

---

### 1.6 Formula Normalization

**File:** `src/fornero/spreadsheet/model.py`
**Lines:** 302-330
**Severity:** LOW

**Issue:**
The `Formula` class normalizes expressions to always start with "=", but the spec doesn't specify this behavior.

**Implementation:**
```python
def __str__(self) -> str:
    """Convert Formula to string, ensuring it starts with '='."""
    if self.expression.startswith("="):
        return self.expression
    return f"={self.expression}"
```

**Recommendation:**
Document this normalization behavior in the spec, or move it to the executor layer if it's a Google Sheets requirement rather than an algebra property.

---

### 1.7 Reference Quoting Rules

**File:** `src/fornero/spreadsheet/model.py`
**Lines:** 363-374
**Severity:** LOW

**Issue:**
The `Reference.to_string()` method quotes sheet names with spaces or "!", but the spec doesn't mention this Google Sheets-specific requirement.

**Spec:**
References are written as $s'!\text{Range}$ with no mention of quoting.

**Implementation:**
```python
def to_string(self) -> str:
    if self.sheet_name:
        if " " in self.sheet_name or "!" in self.sheet_name:
            return f"'{self.sheet_name}'!{self.range_ref}"
        return f"{self.sheet_name}!{self.range_ref}"
    return self.range_ref
```

**Recommendation:**
Add a note in the spec that sheet names with special characters must be quoted per Google Sheets syntax requirements.

---

### 1.8 Value Spreadsheet Conversion

**File:** `src/fornero/spreadsheet/model.py`
**Lines:** 418-429
**Severity:** LOW

**Issue:**
The `Value` class converts `None` to `""` but the spec refers to "null" cells without specifying their representation.

**Spec (§Spreadsheet Algebra, SetValues):**
"Every cell in the target rectangle is overwritten with the corresponding element of $V$."

**Implementation:**
```python
def to_spreadsheet(self) -> Union[str, int, float, bool]:
    if self.value is None:
        return ""  # None → empty string
    return self.value
```

**Recommendation:**
Document that null/None values are represented as empty strings in the spreadsheet implementation. This is a Google Sheets convention.

---

## 2. Translation Strategy Discrepancies

### 2.1 Source Translation: Range Definition Mismatch

**File:** `src/fornero/translator/strategies.py`
**Lines:** 81-136
**Severity:** HIGH

**Issue:**
The `translate_source` function returns a range that doesn't match the spec's definition of how source data should be represented.

**Spec (§Translator, Translating Source):**
"$\mathcal{W}_3 = \text{SetValues}(\mathcal{W}_2, s, (1,0), \text{rows}(R))$ writes data."
Output range: "$s!(0,0):(m, n-1)$" — uses 0-indexed notation

**Implementation:**
```python
output_range = Range(row=1, col=1, row_end=max(1, num_rows + 1), col_end=num_cols)
```

This is 1-indexed: header at row 1, data at rows 2..num_rows+1.

**Actual Behavior:**
- Header written at row 0 (0-indexed)
- Data written starting at row 1 (0-indexed)
- Output range is 1-indexed: row 1 to num_rows+1

**Recommendation:**
The spec uses mixed indexing (0-indexed for operations, 1-indexed for ranges). Standardize on 1-indexed ranges throughout and document the coordinate system explicitly.

---

### 2.2 Select Translation: FILTER Wrapper

**File:** `src/fornero/translator/strategies.py`
**Lines:** 139-204
**Severity:** MEDIUM

**Issue:**
The `translate_select` implementation uses a `FILTER` wrapper to exclude empty rows, which is an optimization not mentioned in the spec.

**Spec (§Translator, Translating Select):**
```
∀ j ∈ [1, k]: SetFormula(W, s, (1, j−1), =ARRAYFORMULA(col(ρ, cⱼ)))
```

**Implementation:**
```python
formula = f"=FILTER({array_expr}, {first_col_ref}<>\"\")"
```

**Analysis:**
This is a pragmatic optimization to handle upstream FILTER operations that produce variable-length results. However, it deviates from the spec's pure column projection.

**Recommendation:**
Document this optimization in the spec as a necessary workaround for Google Sheets' static range allocation vs. dynamic spilling behavior.

---

### 2.3 Filter Translation: Predicate Conversion

**File:** `src/fornero/translator/strategies.py`
**Lines:** 318-360
**Severity:** MEDIUM

**Issue:**
The `_translate_predicate` function has a fallback string-replacement mode that isn't type-safe and doesn't match the spec's formal predicate structure.

**Spec (§Translator, Translating Filter):**
"The predicate $p$ decomposes into atomic comparisons $c \;\theta\; v$ ... joined by $\land$ / $\lor$"

**Implementation:**
```python
# String replacement fallback (lines 339-360)
result = result.replace(" AND ", ")*(")
result = result.replace(" OR ", ")+(")
```

This assumes predicates are strings with " AND " / " OR ", which is fragile.

**Recommendation:**
Require that all predicates be Expression AST nodes (not strings) and remove the string fallback. Or document that the string mode is a legacy compatibility layer.

---

### 2.4 GroupBy Translation: Two-Sheet vs. Single-Sheet Strategy

**File:** `src/fornero/translator/strategies.py`
**Lines:** 593-726
**Severity:** HIGH

**Issue:**
The implementation uses a single-sheet `UNIQUE + SUMIFS` strategy, but the spec describes a two-sheet strategy with `QUERY + SORT`.

**Spec (§Translator, Translating GroupBy):**
"The translator uses a **two-sheet strategy**: a helper sheet for the aggregation and an output sheet that restores first-appearance order."
- Helper sheet: `=QUERY(...)`
- Output sheet: `=SORT(...XMATCH...)`

**Implementation:**
```python
# Single sheet strategy:
# 1. UNIQUE formula for keys (preserves first-appearance order)
unique_formula = f"=UNIQUE({key_ref})"

# 2. Per-row SUMIFS/AVERAGEIFS/etc. formulas for aggregations
inner_formula = f"{spreadsheet_func}({value_ref}, {criteria_clause})"
```

**Analysis:**
The implementation chose a different approach:
- **Pro:** Simpler (one sheet instead of two), leverages `UNIQUE` which preserves first-appearance order natively
- **Con:** Generates O(n) per-row formulas instead of a single array formula, deviates from spec

**Recommendation:**
Either update the spec to document the single-sheet UNIQUE+SUMIFS strategy, or implement the two-sheet QUERY+SORT approach as specified. The current approach is more maintainable but architecturally different.

---

### 2.5 GroupBy Multi-Key Reference Issue

**File:** `src/fornero/translator/strategies.py`
**Lines:** 649-663
**Severity:** MEDIUM

**Issue:**
For multi-key groupby, the implementation references all key columns together but assumes they're contiguous, which may not be true.

**Implementation:**
```python
# Multi-column UNIQUE: reference all key columns together
key_indices = [input_schema.index(key) for key in op.keys]
min_key_idx = min(key_indices)
max_key_idx = max(key_indices)
# Build range spanning min to max
```

**Problem:**
If keys are ["col_a", "col_d"] in schema ["col_a", "col_b", "col_c", "col_d"], this selects A:D which includes non-key columns B and C.

**Recommendation:**
Either require groupby keys to be contiguous in the schema, or use column-by-column array construction: `{col_a, col_d}` instead of a range.

---

### 2.6 Aggregate Function Mapping Inconsistency

**File:** `src/fornero/translator/strategies.py`
**Lines:** 768-774
**Severity:** LOW

**Issue:**
The `translate_aggregate` function maps `mean → AVERAGE` but `translate_groupby` maps `mean → AVERAGEIFS`.

**Spec (§Translator, Translating Aggregate):**
"$f_i^{\mathcal{G}}$ is the Google Sheets function for $f_i$: $\text{sum} \to \texttt{SUM}$, $\text{mean} \to \texttt{AVERAGE}$, ..."

**Spec (§Translator, Translating GroupBy):**
"$\texttt{AVERAGEIFS}$" for grouped aggregations

**Recommendation:**
This is actually correct (scalar vs. conditional functions), but document the distinction more clearly in the spec to avoid confusion.

---

### 2.7 Sort Translation: FILTER Wrapper

**File:** `src/fornero/translator/strategies.py`
**Lines:** 851-853
**Severity:** MEDIUM

**Issue:**
Like Select, Sort wraps the input in `FILTER` to exclude empty trailing rows, which isn't in the spec.

**Spec (§Translator, Translating Sort):**
"$\text{SetFormula}(\mathcal{W}, s, (1, 0), \texttt{=SORT(}\rho_{\text{data}}\texttt{, ...}))$"

**Implementation:**
```python
clean_ref = f"FILTER({data_ref}, {first_col_ref}<>\"\")"
sort_formula = f"=SORT({clean_ref}, {sort_params_str})"
```

**Recommendation:**
Document this as a systematic pattern: all operations that consume upstream FILTER results must clean empty rows. Or implement a global post-processing pass that eliminates empty rows.

---

### 2.8 Limit Translation: Row Count Calculation

**File:** `src/fornero/translator/strategies.py`
**Lines:** 869-924
**Severity:** LOW

**Issue:**
The output range calculation doesn't account for the case where `op.count` exceeds available rows.

**Implementation:**
```python
num_rows = min(op.count, input_range.row_end - input_range.row) + 1  # +1 for header
```

**Spec (§Translator, Translating Limit):**
"$\min(n, |\rho.\text{rows}|-1)$"

The implementation is correct but the spec notation is unclear about whether `|ρ.rows|` includes the header or not.

**Recommendation:**
Clarify in the spec that `|ρ.rows|` refers to total rows (header + data) so `|ρ.rows| - 1` is the data row count.

---

### 2.9 WithColumn Translation: Expression Handling

**File:** `src/fornero/translator/strategies.py`
**Lines:** 1011-1041
**Severity:** MEDIUM

**Issue:**
The `_translate_expression` function has a fragile string-replacement fallback that can produce incorrect results for ambiguous column names.

**Implementation:**
```python
# String replacement logic (lines 1029-1040)
for col_idx, col_name in enumerate(input_schema):
    if col_name in result:
        col_ref = _col_to_range_ref(...)
        result = result.replace(f" {col_name} ", f" {col_ref} ")
        # ... more replacements
```

**Problem:**
If schema has columns ["amount", "total_amount"], replacing "amount" first will corrupt "total_amount" → "total_<ref>".

**Recommendation:**
Sort columns by length descending (already done in `_translate_predicate` line 344) or require Expression AST nodes.

---

### 2.10 Union Translation: Schema Validation

**File:** `src/fornero/translator/strategies.py`
**Lines:** 1044-1106
**Severity:** LOW

**Issue:**
The Union translation correctly validates schema equality but the error message could be more helpful.

**Spec (§Translator, Translating Union):**
"Precondition: $\mathcal{S}(R_1) = \mathcal{S}(R_2)$"

**Implementation:**
```python
if left_schema != right_schema:
    raise UnsupportedOperationError(
        f"Union requires identical schemas, got {left_schema} and {right_schema}"
    )
```

**Recommendation:**
This is correct. Consider adding column-by-column comparison to help users understand mismatches (e.g., "Schema mismatch: left has ['a','b'], right has ['a','c']").

---

### 2.11 Pivot Translation: Per-Cell Formula Generation

**File:** `src/fornero/translator/strategies.py`
**Lines:** 1189-1236
**Severity:** MEDIUM

**Issue:**
The implementation generates O(n×m) individual formulas (one per cell) instead of using array formulas as implied by the spec.

**Spec (§Translator, Translating Pivot):**
"Each data cell at row $r$, column $q \geq 1$ uses a filtered lookup..."

The spec shows a formula per cell but doesn't discuss the scalability implications.

**Implementation:**
```python
for i in range(n_rows):
    for j in range(n_cols):
        operations.append({
            'type': 'set_formula',
            'formula': f'=IFERROR(INDEX(FILTER(...)), "")'
        })
```

**Analysis:**
This generates thousands of formulas for large pivots. Google Sheets has a 10M cell limit, so this scales poorly.

**Recommendation:**
Document the scalability limitation in the spec, or research if there's an array-formula approach for pivot (Google Sheets PIVOT function might help).

---

### 2.12 Melt Translation: INDIRECT Usage

**File:** `src/fornero/translator/strategies.py`
**Lines:** 1295-1316
**Severity:** LOW

**Issue:**
The implementation uses `INDIRECT("1:"&ROWS(...)*k)` to generate row sequences, which is marked volatile by Google Sheets and may cause performance issues.

**Spec (§Translator, Translating Melt):**
"$\texttt{INT((ROW(...)-1)/}k\texttt{)+1}$ pattern maps each block of $k$ consecutive output rows..."

The spec doesn't mention `INDIRECT` specifically.

**Implementation:**
```python
formula = f'=ARRAYFORMULA(INDEX({col_ref}, INT((ROW(INDIRECT("1:"&ROWS({col_ref})*{k}))-1)/{k})+1))'
```

**Recommendation:**
Document that this is a Google Sheets idiom for generating row sequences. Consider alternative approaches (like `SEQUENCE` if available in the target Sheets API version).

---

### 2.13 Window Translation: Per-Row Formula Strategy

**File:** `src/fornero/translator/strategies.py`
**Lines:** 1360-1642
**Severity:** MEDIUM

**Issue:**
Window functions generate per-row formulas (not array formulas), which is necessary but creates large execution plans for big datasets.

**Spec (§Translator, Translating Window):**
"Window formulas are **per-row** (not array formulas) because each row's visible frame may differ."

**Analysis:**
The implementation correctly follows the spec but this means a 10,000-row input generates 10,000 `SetFormula` operations for the window column.

**Recommendation:**
The spec is correct. Document the performance implications: window operations on large datasets will be slow to translate and execute.

---

### 2.14 Window Lag/Lead: Partition Support

**File:** `src/fornero/translator/strategies.py`
**Lines:** 1599-1604
**Severity:** MEDIUM

**Issue:**
The implementation correctly raises `UnsupportedOperationError` for partition-aware lag/lead, matching the spec.

**Spec (§Translator, Translating Window):**
"For window specifications that cannot be expressed with available Google Sheets formulas... the translator raises $\texttt{UnsupportedOperationError}$."

**Implementation:**
```python
if op.partition_by:
    raise UnsupportedOperationError(
        f"Partition-aware {op.function} cannot be expressed as a spreadsheet formula..."
    )
```

**Recommendation:**
This is correct. Document alternative approaches (e.g., preprocessing with a helper column that marks partition boundaries).

---

## 3. Additional Observations

### 3.1 Join Translation: Right and Outer Join Complexity

**Files:** `src/fornero/translator/strategies.py`
**Lines:** 473-590
**Severity:** INFO

The right and outer join implementations are significantly more complex than left/inner joins:
- Right join swaps the driving table
- Outer join uses three sheets (left part, anti-join part, union)

These match the spec but are architecturally heavy. The spec could benefit from worked examples showing the formula progression.

---

### 3.2 Coordinate System Documentation Gap

**Files:** All spreadsheet and translator files
**Severity:** MEDIUM

The biggest source of confusion is the mixed coordinate system:
- Operations use 0-indexed row/col
- Ranges use 1-indexed row/col
- Google Sheets uses 1-indexed A1 notation
- Spec uses mathematical notation that isn't consistently 0- or 1-indexed

**Recommendation:**
Add a dedicated "Coordinate Systems" section to ARCHITECTURE.md explaining:
1. Internal operation coordinates are 0-indexed (Python convention)
2. Range objects are 1-indexed (spreadsheet convention)
3. Conversion happens at the Range/Reference boundary
4. A1 notation is always 1-indexed

---

### 3.3 Error Handling Patterns

**Files:** `src/fornero/translator/strategies.py`
**Severity:** INFO

The translator consistently uses `UnsupportedOperationError` for operations that can't be expressed as formulas (e.g., partition-aware lag/lead, complex window frames). This matches the spec:

> "if an operation cannot be expressed as a formula, the translator raises `UnsupportedOperationError`"

This is good architectural discipline: fail fast rather than producing incorrect results.

---

## 4. Summary Table

| # | Category | File | Lines | Issue | Severity |
|---|----------|------|-------|-------|----------|
| 1.1 | Spreadsheet Algebra | operations.py | 56-63, 101-103 | SetValues/SetFormula use 0-indexed coords vs. spec's 1-indexed | HIGH |
| 1.2 | Spreadsheet Algebra | model.py | 68-95 | Range 1-indexed vs. operations 0-indexed mismatch | MEDIUM |
| 1.3 | Spreadsheet Algebra | operations.py | 129-173 | NamedRange defined but never used | LOW |
| 1.4 | Spreadsheet Algebra | operations.py | 18-48 | CreateSheet missing validation | LOW |
| 1.5 | Spreadsheet Algebra | model.py | 24-43 | Sheet dimensions unused | LOW |
| 1.6 | Spreadsheet Algebra | model.py | 302-330 | Formula normalization undocumented | LOW |
| 1.7 | Spreadsheet Algebra | model.py | 363-374 | Reference quoting rules undocumented | LOW |
| 1.8 | Spreadsheet Algebra | model.py | 418-429 | Value None→"" conversion undocumented | LOW |
| 2.1 | Translation | strategies.py | 81-136 | Source range definition mismatch | HIGH |
| 2.2 | Translation | strategies.py | 139-204 | Select adds FILTER wrapper (optimization) | MEDIUM |
| 2.3 | Translation | strategies.py | 318-360 | Filter predicate string fallback fragile | MEDIUM |
| 2.4 | Translation | strategies.py | 593-726 | GroupBy uses single-sheet strategy vs. spec's two-sheet | HIGH |
| 2.5 | Translation | strategies.py | 649-663 | GroupBy multi-key assumes contiguous columns | MEDIUM |
| 2.6 | Translation | strategies.py | 768-774 | Aggregate function mapping (AVERAGE vs AVERAGEIFS) | LOW |
| 2.7 | Translation | strategies.py | 851-853 | Sort adds FILTER wrapper (optimization) | MEDIUM |
| 2.8 | Translation | strategies.py | 869-924 | Limit row count notation unclear | LOW |
| 2.9 | Translation | strategies.py | 1011-1041 | WithColumn expression string replacement fragile | MEDIUM |
| 2.10 | Translation | strategies.py | 1044-1106 | Union schema validation could be clearer | LOW |
| 2.11 | Translation | strategies.py | 1189-1236 | Pivot generates O(n×m) formulas | MEDIUM |
| 2.12 | Translation | strategies.py | 1295-1316 | Melt uses volatile INDIRECT | LOW |
| 2.13 | Translation | strategies.py | 1360-1642 | Window per-row formulas scalability | MEDIUM |
| 2.14 | Translation | strategies.py | 1599-1604 | Window lag/lead partition restriction correct | INFO |

---

## 5. Recommendations

### Priority 1 (HIGH) - Address Immediately

1. **Coordinate System Standardization**: Resolve the 0-indexed (operations) vs. 1-indexed (ranges) mismatch. Either:
   - Update the spec to explicitly document the dual system
   - OR change operations to use 1-indexed coordinates throughout

2. **GroupBy Strategy Alignment**: Decide whether to:
   - Update implementation to match spec's two-sheet QUERY+SORT approach
   - OR update spec to document current single-sheet UNIQUE+SUMIFS approach

3. **Source Range Definition**: Fix the range coordinate mismatch in `translate_source` to match spec notation.

### Priority 2 (MEDIUM) - Address Soon

4. **FILTER Wrapper Pattern**: Document the systematic use of `FILTER(..., col<>"")` to clean empty rows from upstream operations.

5. **Predicate/Expression String Fallback**: Remove or document the string-replacement modes in expression translation. Prefer AST-only paths.

6. **GroupBy Multi-Key**: Fix the contiguous-column assumption or document the limitation.

7. **Pivot Scalability**: Document the O(n×m) formula generation and its scalability limits.

### Priority 3 (LOW) - Nice to Have

8. **Named Ranges**: Either implement or remove from spec.
9. **Validation**: Add validation to CreateSheet and other operations.
10. **Documentation**: Document Formula normalization, Reference quoting, Value conversion, and other Google Sheets-specific behaviors.

---

## Conclusion

The implementation is **largely faithful** to the specification with several pragmatic deviations:

**Strengths:**
- Core algebra abstractions (Sheet, Range, Formula, Reference) are well-designed
- Translation strategies implement the spec's semantic rules correctly
- Error handling is disciplined (fail fast with UnsupportedOperationError)
- Optimization patterns (FILTER wrappers) show practical awareness of Google Sheets limitations

**Weaknesses:**
- Coordinate system confusion (0-indexed ops vs. 1-indexed ranges)
- GroupBy strategy differs significantly from spec
- Several undocumented Google Sheets idioms and workarounds
- String-based expression fallbacks are fragile

**Overall Assessment:**
The code is production-quality but the spec needs updates to match implementation reality. Recommend treating the implementation as the source of truth and updating the spec to document:
1. The dual coordinate system
2. The FILTER wrapper pattern
3. The single-sheet GroupBy strategy
4. Google Sheets-specific formula idioms (INDIRECT, quoting rules, etc.)
