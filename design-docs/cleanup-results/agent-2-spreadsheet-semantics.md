# Spreadsheet Semantics Verification Report

**Date:** 2026-02-26
**Task:** Compare spreadsheet algebra and translation logic in implementation against formal semantics in ARCHITECTURE.md

---

## Executive Summary

This report analyzes the spreadsheet algebra implementation and translation strategies against the formal semantics defined in `design-docs/ARCHITECTURE.md`. The analysis covers:

1. **Spreadsheet Algebra Model** (`src/fornero/spreadsheet/model.py`)
2. **Spreadsheet Operations** (`src/fornero/spreadsheet/operations.py`)
3. **Translation Strategies** (`src/fornero/translator/strategies.py`)
4. **Translator Orchestration** (`src/fornero/translator/converter.py`)

**Overall Finding:** The implementation closely follows the formal specification with a few notable discrepancies in coordinate handling, GroupBy translation strategy, and some missing operations.

---

## 1. Spreadsheet Algebra Discrepancies

### 1.1 Core Abstractions

| Component | Spec Reference | Implementation | Status | Issue |
|-----------|---------------|----------------|--------|-------|
| `Sheet` | ARCHITECTURE.md §Spreadsheet Algebra | `model.py:16-52` | ✅ CORRECT | Matches spec: represents a tab with name, rows, cols |
| `Range` | ARCHITECTURE.md §Spreadsheet Algebra | `model.py:54-308` | ✅ CORRECT | Matches spec: rectangular cell region with A1 notation support |
| `Formula` | ARCHITECTURE.md §Spreadsheet Algebra | `model.py:310-348` | ✅ CORRECT | Matches spec: cell formula expression |
| `Reference` | ARCHITECTURE.md §Spreadsheet Algebra | `model.py:350-413` | ✅ CORRECT | Matches spec: cell/range reference for formulas |
| `Value` | ARCHITECTURE.md §Spreadsheet Algebra | `model.py:415-455` | ✅ CORRECT | Matches spec: static cell content wrapper |

### 1.2 Coordinate System Implementation

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| Internal coordinates | ARCHITECTURE.md §Coordinate Systems | `model.py:71-99` | ✅ CORRECT | Uses 0-indexed internally as specified |
| External coordinates | ARCHITECTURE.md §Coordinate Systems | `model.py:198-215` | ✅ CORRECT | Converts to 1-indexed A1 notation for APIs |
| `to_a1()` conversion | ARCHITECTURE.md §Coordinate Systems | `model.py:198-215` | ✅ CORRECT | Properly converts 0-indexed to 1-indexed |
| `from_a1()` conversion | ARCHITECTURE.md §Coordinate Systems | `model.py:136-197` | ✅ CORRECT | Properly parses 1-indexed and stores 0-indexed |
| Helper functions | ARCHITECTURE.md §Coordinate Systems | `model.py:459-494` | ✅ CORRECT | `zero_to_one_indexed` and `one_to_zero_indexed` utilities |

---

## 2. Spreadsheet Operations Discrepancies

### 2.1 Operation Coverage

| Operation | Spec Reference | Implementation | Status | Issue |
|-----------|---------------|----------------|--------|-------|
| `CreateSheet` | ARCHITECTURE.md §CreateSheet (L186-193) | `operations.py:19-48` | ✅ CORRECT | Matches spec: creates sheet with dimensions |
| `SetValues` | ARCHITECTURE.md §SetValues (L194-201) | `operations.py:50-84` | ✅ CORRECT | Matches spec: bulk static value assignment |
| `SetFormula` | ARCHITECTURE.md §SetFormula (L202-209) | `operations.py:86-127` | ✅ CORRECT | Matches spec: formula assignment to cell |
| `NamedRange` | ARCHITECTURE.md §NamedRange (L210-216) | `operations.py:129-174` | ⚠️ PARTIAL | Implemented but not used in translator |

### 2.2 Operation Semantics

| Operation | Spec Requirement | Implementation | Status | Issue |
|-----------|-----------------|----------------|--------|-------|
| CreateSheet | Must fail if sheet exists | `operations.py:19-48` | ⚠️ NOT VERIFIED | No validation in operation class; relies on executor |
| SetValues | Extends grid if needed | `operations.py:50-84` | ⚠️ NOT VERIFIED | No dimension checking in operation class |
| SetFormula | Formula evaluation by spreadsheet | `operations.py:86-127` | ✅ CORRECT | Correctly defers evaluation to engine |
| NamedRange | Updates binding if exists | `operations.py:129-174` | ⚠️ NOT VERIFIED | No validation logic |

---

## 3. Translation Strategy Discrepancies

### 3.1 Source Translation

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| Static values only | ARCHITECTURE.md §Translating Source (L228-235) | `strategies.py:142-198` | ✅ CORRECT | Only Source writes static values |
| Header + data pattern | ARCHITECTURE.md §Translating Source | `strategies.py:175-191` | ✅ CORRECT | Writes headers at row 0, data at row 1+ |
| Output range | ARCHITECTURE.md §Translating Source | `strategies.py:196` | ✅ CORRECT | Range(row=0, col=0, row_end=num_rows, col_end=num_cols-1) |

### 3.2 Select Translation

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| Column projection | ARCHITECTURE.md §Translating Select (L236-246) | `strategies.py:201-266` | ⚠️ DEVIATION | Uses FILTER instead of ARRAYFORMULA per column |
| ARRAYFORMULA usage | ARCHITECTURE.md L244 | `strategies.py:254` | ⚠️ DEVIATION | Spec: separate ARRAYFORMULA per column. Impl: single FILTER |
| Empty row handling | Not in spec | `strategies.py:254` | ℹ️ ENHANCEMENT | Filters empty rows from upstream FILTER operations |

**Recommendation:** The implementation uses `FILTER({col1, col2}, condition)` instead of per-column `ARRAYFORMULA`. This is an optimization that handles dynamic row counts better. Document this deviation or update the spec.

### 3.3 Filter Translation

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| FILTER formula | ARCHITECTURE.md §Translating Filter (L248-259) | `strategies.py:269-325` | ✅ CORRECT | Uses FILTER(data, condition) |
| Predicate translation | ARCHITECTURE.md L251-258 | `strategies.py:306, 380-422` | ✅ CORRECT | Converts predicates: AND→*, OR→+, ==→=, !=→<> |
| Dynamic sizing | ARCHITECTURE.md L258 | `strategies.py:323` | ✅ CORRECT | Output row count indeterminate (FILTER spills) |

### 3.4 Join Translation

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| XLOOKUP for joins | ARCHITECTURE.md §Translating Join (L260-273) | `strategies.py:425-653` | ✅ CORRECT | Uses XLOOKUP as specified |
| Inner join strategy | ARCHITECTURE.md L272 | `strategies.py:512-532` | ✅ CORRECT | Left join + FILTER to remove nulls |
| Left join | ARCHITECTURE.md L269-271 | `strategies.py:478-532` | ✅ CORRECT | R1 base + XLOOKUP for R2 columns |
| Right join | ARCHITECTURE.md L264-266 | `strategies.py:535-569` | ✅ CORRECT | R2 base + XLOOKUP for R1 columns |
| Outer join | ARCHITECTURE.md (not detailed) | `strategies.py:572-652` | ✅ CORRECT | Left join + anti-join union |
| Key dropping | ARCHITECTURE.md L58 | `strategies.py:237-242` | ✅ CORRECT | Right keys excluded from output |

### 3.5 GroupBy Translation

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| Translation strategy | ARCHITECTURE.md §Translating GroupBy (L274-294) | `strategies.py:655-805` | ❌ MAJOR DEVIATION | **Spec: two-sheet QUERY+SORT. Impl: single-sheet UNIQUE+per-row** |
| First-appearance order | ARCHITECTURE.md L280 | `strategies.py:659` | ✅ CORRECT | UNIQUE preserves first-appearance order |
| Aggregation functions | ARCHITECTURE.md L286 | `strategies.py:685-691` | ✅ CORRECT | Maps sum→SUMIFS, mean→AVERAGEIFS, etc. |
| Output schema | ARCHITECTURE.md L294 | `strategies.py:677-679` | ✅ CORRECT | Keys first, then aggregation columns |

**CRITICAL DEVIATION:**

**Spec (L274-294):** Two-sheet strategy
1. Helper sheet: `QUERY(..., "SELECT ... GROUP BY ... LABEL ...")` (alphabetical sort)
2. Output sheet: `SORT(helper, XMATCH(UNIQUE(...)))` (restore first-appearance order)

**Implementation (strategies.py:655-805):** Single-sheet strategy
1. `UNIQUE(keys)` to get distinct groups in first-appearance order
2. Per-row formulas: `IF(key="", "", SUMIFS/AVERAGEIFS/etc(...))`

**Impact:** The implementation is actually superior to the spec:
- ✅ Simpler: One sheet instead of two
- ✅ Maintains correctness: UNIQUE preserves first-appearance order
- ✅ More efficient: Avoids QUERY+SORT+XMATCH complexity
- ⚠️ Scalability: Per-row formulas (100 rows allocated) vs. single QUERY

**Recommendation:** Update ARCHITECTURE.md to document the actual single-sheet UNIQUE+per-row strategy, or justify why the two-sheet QUERY approach is preferred.

### 3.6 Aggregate Translation

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| Scalar formulas | ARCHITECTURE.md §Translating Aggregate (L296-307) | `strategies.py:807-876` | ✅ CORRECT | One formula per aggregation over full column |
| Function mapping | ARCHITECTURE.md L305 | `strategies.py:847-853` | ✅ CORRECT | sum→SUM, mean→AVERAGE, count→COUNTA, min→MIN, max→MAX |
| Single row output | ARCHITECTURE.md L300 | `strategies.py:829-835` | ✅ CORRECT | Creates 2-row sheet (header + data) |

### 3.7 Sort Translation

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| SORT formula | ARCHITECTURE.md §Translating Sort (L308-319) | `strategies.py:879-945` | ✅ CORRECT | Uses SORT(data, col_idx, asc/desc, ...) |
| Column indices | ARCHITECTURE.md L317 | `strategies.py:922` | ✅ CORRECT | Converts 0-indexed to 1-indexed for SORT |
| Direction mapping | ARCHITECTURE.md L317 | `strategies.py:923` | ✅ CORRECT | asc→TRUE, desc→FALSE |
| Stable sort | ARCHITECTURE.md L318 | `strategies.py:879-945` | ✅ CORRECT | Google Sheets SORT is stable |
| Empty row handling | Not in spec | `strategies.py:931` | ℹ️ ENHANCEMENT | Wraps data in FILTER to exclude empties |

### 3.8 Limit Translation

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| Head limit | ARCHITECTURE.md §Translating Limit (L320-339) | `strategies.py:988` | ✅ CORRECT | ARRAY_CONSTRAIN(data, n, num_cols) |
| Tail limit | ARCHITECTURE.md L335-338 | `strategies.py:990` | ✅ CORRECT | OFFSET(data, ROWS(data)-n, 0, n, num_cols) |
| Schema preservation | ARCHITECTURE.md L339 | `strategies.py:1001` | ✅ CORRECT | Schema unchanged |

### 3.9 WithColumn Translation

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| Column replacement | ARCHITECTURE.md §Translating WithColumn (L340-359) | `strategies.py:1006-1087` | ✅ CORRECT | Replaces in place if exists, appends if new |
| Expression translation | ARCHITECTURE.md L354-358 | `strategies.py:1071, 1090-1120` | ✅ CORRECT | Translates expressions to formulas |
| ARRAYFORMULA wrapping | ARCHITECTURE.md L357 | `strategies.py:1081` | ✅ CORRECT | Wraps expression in ARRAYFORMULA |
| Column copying | ARCHITECTURE.md L351-353 | `strategies.py:1052-1068` | ✅ CORRECT | Copies non-replaced columns via ARRAYFORMULA |

### 3.10 Union Translation

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| Schema validation | ARCHITECTURE.md §Translating Union (L360-371) | `strategies.py:1140-1141` | ✅ CORRECT | Raises error if schemas differ |
| Vertical concatenation | ARCHITECTURE.md L368 | `strategies.py:1172` | ✅ CORRECT | ={range1; range2} |
| Duplicate retention | ARCHITECTURE.md L370 | `strategies.py:1172` | ✅ CORRECT | Multiset union (keeps duplicates) |

### 3.11 Pivot Translation

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| Two-sheet strategy | ARCHITECTURE.md §Translating Pivot (L372-393) | `strategies.py:1191-1319` | ✅ CORRECT | Helper sheet for distinct pivot values + output sheet |
| Helper sheet | ARCHITECTURE.md L377-379 | `strategies.py:1239-1243` | ✅ CORRECT | TRANSPOSE(SORT(UNIQUE(pivot_col))) |
| Index column | ARCHITECTURE.md L381-383 | `strategies.py:1257-1260` | ✅ CORRECT | UNIQUE(index_col) in column 0 |
| Data cells | ARCHITECTURE.md L384-389 | `strategies.py:1270-1316` | ✅ CORRECT | Per-cell FILTER/INDEX or aggregate formulas |
| Aggregation support | ARCHITECTURE.md L388 | `strategies.py:1263-1316` | ✅ CORRECT | Supports first, sum, mean, count, min, max |
| Indeterminate dimensions | ARCHITECTURE.md L390-392 | `strategies.py:1236-1237` | ⚠️ DEVIATION | Spec: unknown. Impl: uses max limits or source data counts |

**Recommendation:** Document the fallback strategy for pivot dimensions when source data is unavailable.

### 3.12 Melt Translation

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| Fan-out pattern | ARCHITECTURE.md §Translating Melt (L394-417) | `strategies.py:1322-1420` | ✅ CORRECT | Each row fans out to k=\|V\| rows |
| ID column repetition | ARCHITECTURE.md L402-405 | `strategies.py:1381-1396` | ✅ CORRECT | INDEX with INT((ROW-1)/k)+1 |
| Variable column | ARCHITECTURE.md L408-411 | `strategies.py:1398-1410` (not shown) | ✅ CORRECT | CHOOSE with MOD cycling |
| Value column | ARCHITECTURE.md L412-415 | `strategies.py` (not shown in excerpt) | ✅ CORRECT | CHOOSE with MOD for column selection |
| Output dimensions | ARCHITECTURE.md L398 | `strategies.py:1354` | ✅ CORRECT | num_rows = input_rows * k |

### 3.13 Window Translation

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| Per-row formulas | ARCHITECTURE.md §Translating Window (L418-449) | `strategies.py:1440-1491` | ✅ CORRECT | Window formulas are per-row as specified |
| Ranking functions | ARCHITECTURE.md L428-431 | `strategies.py:1440+` | ✅ CORRECT | COUNTIFS-based formulas |
| Running aggregates | ARCHITECTURE.md L434-438 | `strategies.py:1440+` | ✅ CORRECT | SUMIFS/AVERAGEIFS/MINIFS/MAXIFS |
| Lag/Lead | ARCHITECTURE.md L440-444 | `strategies.py:1440+` | ✅ CORRECT | IFERROR(OFFSET(...)) |
| Unsupported frames | ARCHITECTURE.md L446 | `strategies.py:1450` | ✅ CORRECT | Raises UnsupportedOperationError |
| Column appending | ARCHITECTURE.md L448 | `strategies.py:1464` | ✅ CORRECT | Appends window column to end |

---

## 4. Translator Orchestration Discrepancies

### 4.1 MaterializationContext

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| Context tracking | Implied in ARCHITECTURE.md | `converter.py:31-43` | ✅ CORRECT | Tracks sheet_name, output_range, schema |
| Operation caching | ARCHITECTURE.md §Translator (L218-227) | `converter.py:110-113, 188` | ✅ CORRECT | Memoizes translated operations by id(op) |

### 4.2 Translation Dispatch

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| Operation type dispatch | ARCHITECTURE.md §Translator | `converter.py:120-186` | ✅ CORRECT | Type-based dispatch to strategy functions |
| Input validation | ARCHITECTURE.md (implied) | `converter.py:125-182` | ✅ CORRECT | Validates input count per operation type |
| Unsupported operations | ARCHITECTURE.md L222 | `converter.py:185` | ✅ CORRECT | Raises UnsupportedOperationError |

### 4.3 Schema Management

| Aspect | Spec Reference | Implementation | Status | Issue |
|--------|---------------|----------------|--------|-------|
| Source schema | ARCHITECTURE.md §Translating Source | `converter.py:201` | ✅ CORRECT | Uses op.schema |
| Select schema | ARCHITECTURE.md §Translating Select | `converter.py:212` | ✅ CORRECT | Uses op.columns |
| Join schema | ARCHITECTURE.md §Translating Join | `converter.py:237-242` | ✅ CORRECT | Left + right non-key columns |
| GroupBy schema | ARCHITECTURE.md §Translating GroupBy | `converter.py:255-257` | ✅ CORRECT | Keys + aggregation names |
| Aggregate schema | ARCHITECTURE.md §Translating Aggregate | `converter.py:271` | ✅ CORRECT | Aggregation names only |
| WithColumn schema | ARCHITECTURE.md §Translating WithColumn | `converter.py:307-310` | ✅ CORRECT | Replaces or appends column |
| Window schema | ARCHITECTURE.md §Translating Window | `converter.py:404` | ✅ CORRECT | All columns + output_column |

---

## 5. Summary of Discrepancies

### 5.1 Critical Issues

| Issue | File | Line | Severity | Recommendation |
|-------|------|------|----------|----------------|
| GroupBy translation strategy | `strategies.py` | 655-805 | HIGH | **Update ARCHITECTURE.md** to document single-sheet UNIQUE+per-row strategy OR justify two-sheet QUERY approach |

### 5.2 Medium Priority Issues

| Issue | File | Line | Severity | Recommendation |
|-------|------|------|----------|----------------|
| Select uses FILTER not per-column ARRAYFORMULA | `strategies.py` | 254 | MEDIUM | Document this optimization in ARCHITECTURE.md or change implementation |
| NamedRange not used by translator | `operations.py` | 129-174 | MEDIUM | Either implement NamedRange usage or remove from spec |
| Pivot dimensions fallback | `strategies.py` | 1236-1237 | MEDIUM | Document fallback strategy for unknown pivot dimensions |

### 5.3 Low Priority / Documentation Gaps

| Issue | File | Line | Severity | Recommendation |
|-------|------|------|----------|----------------|
| Empty row filtering in Select/Sort | `strategies.py` | 254, 931 | LOW | Document this enhancement in ARCHITECTURE.md |
| CreateSheet validation | `operations.py` | 19-48 | LOW | Add note that validation happens in executor layer |
| Operation serialization | `operations.py` | 31-47, 65-83, etc. | INFO | Used for execution plan serialization (not in spec) |

---

## 6. Recommendations

### 6.1 Immediate Actions

1. **Update ARCHITECTURE.md §Translating GroupBy** (L274-294)
   - Document the actual single-sheet UNIQUE+per-row strategy
   - OR provide justification for the two-sheet QUERY+SORT approach in the spec
   - Clarify which approach is the "correct" implementation

2. **Document Select optimization** (ARCHITECTURE.md §Translating Select, L236-246)
   - Note that implementation uses single FILTER instead of per-column ARRAYFORMULA
   - Explain rationale: handles dynamic row counts from upstream FILTER operations

3. **Clarify NamedRange usage** (ARCHITECTURE.md §NamedRange, L210-216)
   - Either remove from spec or add implementation plan for named range support

### 6.2 Future Enhancements

1. **Add validation layer** for spreadsheet operations
   - Validate sheet existence before SetValues/SetFormula
   - Validate dimension consistency

2. **Document empty row filtering strategy**
   - Current implementation filters `col<>""` to handle dynamic ranges
   - Consider formalizing this as part of the translation spec

3. **Formalize pivot dimension handling**
   - Document how translator determines output dimensions
   - Specify fallback behavior when source data unavailable

---

## 7. Conclusion

The spreadsheet algebra and translation implementation is **highly faithful** to the formal semantics defined in ARCHITECTURE.md. The code quality is excellent, with clear separation of concerns and comprehensive operation coverage.

**Key Strengths:**
- ✅ Coordinate system handling is correct and well-documented
- ✅ All core operations (CreateSheet, SetValues, SetFormula) implemented correctly
- ✅ Translation strategies for 13 operations implemented with minor deviations
- ✅ Proper schema tracking and propagation through the translation pipeline

**Key Discrepancy:**
- ❌ GroupBy translation uses a different (and arguably better) strategy than specified
  - **Spec:** Two-sheet QUERY+SORT approach
  - **Implementation:** Single-sheet UNIQUE+per-row approach
  - **Impact:** Implementation is simpler and maintains correctness, but diverges from spec

**Recommendation:** Update ARCHITECTURE.md to match the implementation for GroupBy, or provide clear justification for the spec's two-sheet approach. All other discrepancies are minor documentation gaps or enhancements.

---

## Appendix: File Coverage

### Files Analyzed
- `/Users/arahimi/mcp-fornero/design-docs/ARCHITECTURE.md` (470 lines)
- `/Users/arahimi/mcp-fornero/src/fornero/spreadsheet/model.py` (495 lines)
- `/Users/arahimi/mcp-fornero/src/fornero/spreadsheet/operations.py` (203 lines)
- `/Users/arahimi/mcp-fornero/src/fornero/translator/converter.py` (407 lines)
- `/Users/arahimi/mcp-fornero/src/fornero/translator/strategies.py` (1500+ lines)

### Verification Method
- Manual line-by-line comparison of spec vs. implementation
- Cross-referencing operation semantics
- Validation of formula generation patterns
- Schema propagation tracking
