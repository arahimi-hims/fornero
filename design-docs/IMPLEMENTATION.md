# Implementation

## File Structure

The project is organized into clear layers that mirror the architectural components. The `core` module contains the user-facing API — a `DataFrame` subclass and the module-level pandas re-exports that make `import fornero as pd` work. The `algebra` module defines the intermediate representation, the `translator` bridges between algebras, and the `executor` handles Google Sheets integration. This separation makes testing easier: we can test the algebra without touching APIs, and we can test translation logic without executing anything.

Examples live in a top-level `examples/` directory so users can see real usage patterns. Tests mirror the source structure, with fixtures for common test data. The `utils` module contains cross-cutting concerns like serialization (saving plans to JSON) and visualization (rendering plans as diagrams).

```
mcp-fornero/
├── pyproject.toml                    # Project metadata & dependencies
├── README.md                         # Documentation
├── PLAN.md                           # This file
├── examples/                         # Example usage
│   ├── basic_operations.py
│   ├── joins_and_groupby.py
│   └── end_to_end_demo.py
├── src/
│   └── fornero/                      # Main package
│       ├── __init__.py               # Re-exports pandas API + fornero DataFrame
│       ├── core/                     # Core abstractions
│       │   ├── __init__.py
│       │   ├── dataframe.py         # fornero.DataFrame (pd.DataFrame subclass)
│       │   └── tracer.py            # Operation capture logic
│       ├── algebra/                  # Dataframe algebra
│       │   ├── __init__.py
│       │   ├── operations.py        # Operation classes
│       │   ├── logical_plan.py      # LogicalPlan structure
│       │   └── expressions.py       # Expression nodes
│       ├── spreadsheet/              # Spreadsheet algebra
│       │   ├── __init__.py
│       │   ├── model.py             # Sheet, Range, Formula classes
│       │   └── formulas.py          # Formula generation utilities
│       ├── translator/               # Translation layer
│       │   ├── __init__.py
│       │   ├── converter.py         # Main translator
│       │   ├── strategies.py        # Translation strategies
│       │   └── optimizer.py         # Optimization passes
│       ├── executor/                 # Execution layer
│       │   ├── __init__.py
│       │   ├── plan.py              # ExecutionPlan class
│       │   ├── sheets_executor.py   # Google Sheets integration
│       │   └── sheets_client.py     # gspread API wrapper
│       └── utils/                    # Utilities
│           ├── __init__.py
│           ├── serialization.py     # Plan serialization
│           └── visualization.py     # Plan visualization
└── tests/                            # Test suite
    ├── __init__.py
    ├── test_dataframe.py
    ├── test_algebra.py
    ├── test_translator.py
    ├── test_executor.py
    ├── test_correctness.py              # End-to-end matrix comparison runner
    ├── programs/                        # E2E test program corpus (see TESTING.md)
    ├── helpers/                         # E2E comparison & mock utilities
    └── fixtures/
        └── sample_data.csv
```

## Agent Parallelism Map

Tasks are assigned to seven agents. After **Scaffolding** completes, three agents run in parallel (wave 2), unlocking three more in wave 3.

| Wave | Agent | Tasks | Depends on | Parallel with |
|------|-------|-------|------------|---------------|
| 1 | **Scaffolding** | 1 | — | — |
| 2 | **Algebra** | 5, 6 | Scaffolding | Spreadsheet, Executor(14) |
| 2 | **Spreadsheet** | 7 → 13 | Scaffolding | Algebra, Executor(14) |
| 2 | **Executor** | 14 | Scaffolding | Algebra, Spreadsheet |
| 3 | **Core** | 2, 3, 4 | Algebra | Translator, Utilities |
| 3 | **Translator** | 8, 9, 10, 11, 12 | Algebra + Spreadsheet(7) | Core, Utilities, Executor(15) |
| 3 | **Executor** | 15 | Spreadsheet(13) + Executor(14) | Core, Translator, Utilities |
| 3 | **Utilities** | 16, 17 | Algebra | Core, Translator, Executor(15) |

Each agent also implements the unit tests described in this document for its own tasks. The **Error Handling** exception classes and **Dependencies** section are part of **Scaffolding**. **Extensions** (Tasks 18–20) and cross-cutting **Testing & Documentation** (Tasks 21–22) run after all primary agents complete.

## Formal Semantics Compliance

Every implementation task must be verified against the formal definitions in ARCHITECTURE.md. The architecture specifies precise semantics for each dataframe algebra operation (§Dataframe Algebra) and each spreadsheet algebra operation (§Spreadsheet Algebra), as well as translation rules (§Translator) that map between them. Each task section below includes a **Formal semantics compliance** block that the implementing agent must satisfy. Where the implementation previously deviated from the formal semantics, corrections are noted inline.

Key invariants from the architecture that cut across all tasks:

- **Dual-mode execution**: every operation executes eagerly against pandas *and* simultaneously records itself into the logical plan.
- **Data-blindness of translation**: the translator is a pure function of the operation node — it never inspects actual data values. Source nodes write static values; every other node produces formulas exclusively.
- **Formula-only derived computation**: only Source translation may write data rows via `SetValues`. All other translation rules write headers via `SetValues` and data via `SetFormula`.
- **Row-order significance**: every relation carries an implicit positional index. Operations preserve row order unless stated otherwise (only Sort and GroupBy reorder).

## Core — `core/`

The `fornero.DataFrame` subclass carries a `_plan` attribute that accumulates operations as they're called:

```python
class DataFrame(pd.DataFrame):
    _metadata = ['_plan']

    def __init__(self, *args, plan=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._plan = plan or LogicalPlan(source=self)

    def filter(self, condition):
        result = self.loc[condition]  # Row selection (ARCHITECTURE.md §Filter)
        result = DataFrame(result)
        new_plan = self._plan.add_operation(Filter(condition))
        result._plan = new_plan
        return result

    def to_spreadsheet_plan(self):
        return translate(self._plan)
```

### Tasks

> **Agent: Scaffolding** (wave 1)

1. Set up project structure with `pyproject.toml`

> **Agent: Core** (wave 3) — depends on Algebra; parallel with Translator, Utilities

2. Implement `fornero.DataFrame` as a `pd.DataFrame` subclass with plan tracking
3. Implement `fornero.__init__` to re-export pandas API with tracked variants
4. Build operation tracer that captures operations

> **Formal semantics compliance** (ARCHITECTURE.md §High-Level Architecture, §Filter):
> - The code sketch above corrects the `filter` method: `pd.DataFrame.filter()` is column-label filtering, but ARCHITECTURE.md §Filter defines Filter as row selection ($\text{Filter}(R, p) = [r \mid r \in R, p(r) = 1]$). The implementation uses `self.loc[condition]` for eager execution instead of `super().filter()`.
> - Dual-mode invariant: every traced operation must both execute eagerly via pandas (producing correct results, exceptions, debugging output) *and* record the corresponding algebra node into `_plan`.
> - No untracked DataFrames: the architecture states "there's no concept of untracked dataframes." Every DataFrame created by fornero (including results of `pd.read_csv`, `pd.merge`, `pd.concat`) must carry a `_plan`.

## Dataframe Algebra — `algebra/`

Use Python type hints extensively and `dataclass` for clean operation definitions:

```python
from typing import List, Dict, Optional, Union, Any
from dataclasses import dataclass

@dataclass
class Select(Operation):
    columns: List[str]
    inputs: List[Operation]

    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': 'select',
            'columns': self.columns,
            'inputs': [inp.to_dict() for inp in self.inputs]
        }
```

### Tasks

> **Agent: Algebra** (wave 2) — depends on Scaffolding; parallel with Spreadsheet, Executor

5. Implement `LogicalPlan` and all operation nodes:
   - `Source` (data loading)
   - `Select` (column selection)
   - `Filter` (row filtering)
   - `Join` (inner/left/right/outer joins)
   - `GroupBy` and `Aggregate` (grouping with aggregations)
   - `Sort` (ordering)
   - `Limit` (head/tail)
   - `WithColumn` (computed columns)
   - `Union` (vertical combination)
   - `Pivot` / `Melt` (reshaping)
   - `Window` (window functions)
6. Implement `plan.explain()` for debugging (similar to Polars)

> **Formal semantics compliance** (ARCHITECTURE.md §Dataframe Algebra):
> Each operation node's fields and validation rules must match its formal definition exactly:
> - **Select**: validate `columns ⊆ S(R)`. Output schema is `C`. Preserve row order and multiplicity.
> - **Filter**: predicate `p : Row → {0, 1}`. Output schema is `S(R)`. Preserve order among surviving rows.
> - **Join**: equi-join on `(k₁, k₂, τ)` where `τ ∈ {inner, left, right, outer}`. Output schema is `S(R₁) ∪ S(R₂) \ {k₂}` — the join key from R₂ must be dropped.
> - **GroupBy**: keys `K ⊆ S(R)`, aggregations `[(aᵢ, fᵢ, cᵢ)]`. Output schema is `K ∥ [a₁, …, aₙ]`. Group order must match the order of first appearance in R.
> - **Sort**: key-direction pairs `[(cᵢ, dᵢ)]` where `dᵢ ∈ {asc, desc}`. Must be a stable sort — ties preserve original relative order. Schema unchanged.
> - **Limit**: count `n ∈ ℕ` and end selector `e ∈ {head, tail}`. Head returns `R[0 : min(n, |R|)]`; tail returns `R[max(0, |R| − n) : |R|]`. Schema unchanged.
> - **WithColumn**: if `c ∈ S(R)`, replace the existing column in place (preserving column position); if `c ∉ S(R)`, append the column. Row order preserved.
> - **Aggregate**: equivalent to `GroupBy` with an empty key set. Always produces exactly one row. Output schema is `[a₁, …, aₙ]`.
> - **Union**: requires `S(R₁) = S(R₂)` — raise `ValueError` if schemas differ. Rows of R₁ first, then R₂. Duplicates retained (multiset union).
> - **Pivot**: must handle null for missing cells and accept an optional aggregation function for duplicate matches (default: `first`).
> - **Melt**: identifier columns `I`, value columns `V = S(R) \ I`. Each input row fans out to `|V|` rows. Output schema is `I ∥ [variable, value]`.
> - **Window**: partition keys, order keys, and frame spec must all be captured. Column `a` is appended; row order preserved.

## Spreadsheet Algebra — `spreadsheet/`

### Tasks

> **Agent: Spreadsheet** (wave 2) — depends on Scaffolding; parallel with Algebra, Executor

7. Implement spreadsheet model classes (`Sheet`, `Range`, `Formula`, `Value`, `Reference`)

> **Formal semantics compliance** (ARCHITECTURE.md §Spreadsheet Algebra):
> - **CreateSheet**: must enforce `s ∉ dom(W)` — the operation is undefined if the sheet name already exists. Raise `PlanValidationError` on duplicate names.
> - **SetValues**: overwrites the target rectangle only; cells outside the rectangle are unchanged. The grid of sheet `s` must be extended if the rectangle exceeds current dimensions.
> - **SetFormula**: formula `φ` is a tree of function applications, literal values, and references. References take the form `s'!Range` (cross-sheet) or `Range` (same-sheet).
> - **NamedRange**: if label `ℓ` already exists in the registry, the binding is updated to the new range (not rejected).
> - **Value**: must convert Python `None` → empty string for spreadsheet-safe representation.

## Translator — `translator/`

### Handling Complex Operations

**Custom functions.** Pandas allows arbitrary Python functions (e.g., `df.apply(lambda x: custom_logic(x))`). Solutions, layered:

1. **Detect and Warn** — Raise `UnsupportedOperationError` for custom functions with clear error messages
2. **Limited Lambda Support** — Support simple lambda functions that map to spreadsheet formulas (e.g., `lambda x: x * 2` → `=A2 * 2`) using symbolic execution to analyze expressions
3. **Apps Script Integration** — Generate Google Apps Script for complex functions and deploy as custom functions in the spreadsheet

**Formula complexity.** Start with simple operations. Use helper sheets to decompose complex translations into chains of simpler formulas. Raise `UnsupportedOperationError` for operations that cannot be expressed as formulas (never fall back to static values — the translator must not inspect data). Maintain clear documentation of supported vs. unsupported operations.

**Limited spreadsheet capabilities.** Provide warnings for unsupported features and suggest alternatives (e.g., "use pandas for this operation").

### Tasks

> **Agent: Translator** (wave 3) — depends on Algebra + Spreadsheet(7); parallel with Core, Utilities, Executor(15)

8. Implement translator with strategies for all operations:
   - Select → column projection
   - Filter → FILTER() formula
   - Join → XLOOKUP/VLOOKUP formulas
   - GroupBy → QUERY formulas or pivot tables
   - Aggregate → SUM/AVERAGE/COUNTA formulas
9. Handle multi-sheet plans for complex operations (helper sheets)
10. Implement optimization passes:
    - Predicate pushdown (filter early)
    - Projection pushdown (select only needed columns)
    - Formula simplification
11. Support simple lambda functions that map to spreadsheet formulas (e.g., `lambda x: x * 2` → `=A2 * 2`)
12. Custom function support via Google Apps Script for complex functions

> **Formal semantics compliance** (ARCHITECTURE.md §Translator):
> - **Source is the only translation rule that writes data rows as static values.** All other rules write headers via `SetValues` and data rows via `SetFormula` exclusively. The translator must never fall back to static values for derived computation.
> - **Translation is a pure function** of the operation node's type and structural properties (column names, predicates, join keys, aggregation functions). It never inspects actual data values in the DataFrames.
> - **Select translation**: one array formula per column referencing the source column range; headers via `SetValues`. Correctness invariant: `eval(ρ') = π_{c₁,…,cₖ}(eval(ρ))`.
> - **Filter translation**: single `FILTER` array formula at cell `(1, 0)`. Predicate translation: conjunction `∧` → `*` (multiplication), disjunction `∨` → `+` (addition). Headers for the filter output must be written via `SetValues` at row 0 (the architecture places the formula at `(1, 0)`, leaving row 0 for headers — this is an implementation detail the architecture leaves implicit).
> - **Join translation**: `XLOOKUP` for each non-key column from R₂. Output schema drops `k₂`. For inner joins, a follow-up helper sheet with `FILTER` removes unmatched rows. For left joins, `XLOOKUP`'s if-not-found argument produces null (empty string).
> - **GroupBy translation**: `QUERY` formula with `SELECT…GROUP BY` clause. `QUERY` emits its own header row — **do not generate a separate header `SetValues`**. Function mapping uses QUERY-dialect names: `sum→SUM`, `mean→AVG`, `count→COUNT`, `min→MIN`, `max→MAX`.
> - **Aggregate translation**: direct Google Sheets scalar formulas (not QUERY), one per aggregation, with headers via `SetValues`. Function mapping uses Google Sheets function names, which differ from GroupBy's QUERY dialect: `sum→SUM`, `mean→AVERAGE`, `count→COUNTA`, `min→MIN`, `max→MAX`. (Note: `COUNTA` not `COUNT`; `AVERAGE` not `AVG`.)
> - **Sort translation**: `SORT` formula with 1-indexed column number and direction flag (`TRUE` for asc, `FALSE` for desc).
> - **Union translation**: must validate `S(R₁) = S(R₂)` and raise `UnsupportedOperationError` if schemas differ.
> - **Indeterminate row counts**: `FILTER`, `QUERY`, and `SORT` produce dynamically-sized spill ranges. The translator must represent output row counts as indeterminate (`?`) and downstream operations must reference open-ended ranges when consuming these outputs.
> - **Unsupported operations**: if an operation cannot be expressed as a formula, raise `UnsupportedOperationError` — never fall back to static values.

## Execution Plan — `executor/plan.py`

The translator produces a flat list of spreadsheet algebra operations (`CreateSheet`, `SetValues`, `SetFormula`, `NamedRange`). The `ExecutionPlan` takes that list and organises it into ordered, batchable steps that respect dependency constraints — sheets must exist before anything is written to them, source data must land before formulas that reference it, and named ranges must be registered before formulas that use them.

**Data model.** An `ExecutionPlan` contains an ordered list of `ExecutionStep`s. Each step groups operations of the same kind that can be sent to the Sheets API in a single batch call. The step types, in fixed execution order, are:
- `CreateSheets` — all `CreateSheet` operations, batched into one `spreadsheet.batch_update` call
- `WriteSourceData` — `SetValues` operations for source nodes (static data), batched into one `worksheet.update` call per sheet
- `WriteFormulas` — `SetFormula` operations, topologically sorted so that a formula's dependencies are written before the formula itself. Within a single sheet, formulas with no cross-sheet dependencies can be batched; cross-sheet formulas must follow the sheet they reference.
- `RegisterNamedRanges` — `NamedRange` operations, batched into one `spreadsheet.batch_update` call

Each `ExecutionStep` is a dataclass holding the step type (an enum), the list of spreadsheet algebra operations in that batch, and the target sheet name(s).

**Construction.** `ExecutionPlan.from_operations(ops: List[SpreadsheetOp]) -> ExecutionPlan` is a class method that partitions the flat operation list by type, topologically sorts formula operations by their sheet-reference dependencies, and assembles the step sequence. Construction raises `PlanValidationError` if a formula references a sheet that no `CreateSheet` operation produces, or if a `SetValues` targets a nonexistent sheet.

**Inspection.** `plan.explain() -> str` returns a human-readable summary: how many sheets, how many formulas, how many API calls the plan will require, and the step-by-step order. `plan.to_dict() -> dict` serialises the plan to a JSON-compatible dictionary (used by `utils/serialization.py`). These two methods make the plan auditable before execution — users can call `df.to_spreadsheet_plan()`, inspect the result, and only then pass it to the executor.

**Main sheet.** The plan tracks which sheet holds the final output of the pipeline (the sheet produced by the root node of the dataframe algebra tree). The executor uses this to position the main sheet as the first tab in the spreadsheet.

**Testing.** Unit tests for `ExecutionPlan` do not touch the Sheets API. They construct a list of spreadsheet algebra operations by hand — e.g., a `CreateSheet("source")`, a `SetValues("source", ...)`, a `CreateSheet("filtered")`, a `SetFormula("filtered", ..., ref="source!A:A")` — and verify that `from_operations` produces the correct step sequence. Specific cases to cover:
- A single-source plan (one `CreateSheet`, one `SetValues`, no formulas) produces two steps in the right order.
- A plan with cross-sheet formulas places `WriteSourceData` for the referenced sheet before `WriteFormulas` for the referencing sheet.
- A plan with a formula referencing a nonexistent sheet raises `PlanValidationError`.
- `explain()` output includes sheet count, formula count, and step count.
- `to_dict()` round-trips: `ExecutionPlan.from_dict(plan.to_dict())` produces an equivalent plan.

### Tasks

> **Agent: Spreadsheet** (wave 2, continued) — Task 13 follows Task 7 within the same agent

13. Implement `ExecutionPlan` class

> **Formal semantics compliance** (ARCHITECTURE.md §Execution Plan):
> - Step execution order is fixed: `CreateSheets → WriteSourceData → WriteFormulas → RegisterNamedRanges`. No reordering is permitted.
> - `WriteFormulas` must be topologically sorted by sheet-reference dependencies — a formula's dependencies are written before the formula itself.
> - The plan must track which sheet holds the final output (the root node of the algebra tree) so the executor can position it as the first tab.
> - `CreateSheet` operations with duplicate sheet names must raise `PlanValidationError` (the formal semantics define `CreateSheet` as undefined when `s ∈ dom(W)`).

## Google Sheets Executor — `executor/sheets_executor.py`, `executor/sheets_client.py`

**Google Sheets API limits.** Mitigations:

- Implement rate limiting
- Use batch operations where possible
- Add retry logic with exponential backoff
- Provide offline plan generation (users can inspect before execution)

**Large datasets.** Google Sheets has limits (10M cells, ~5M formula cells). Refuse the request with a clear error message indicating the dataset is too large for Google Sheets. Users should reduce the data size before exporting.

### Tasks

> **Agent: Executor** — Task 14 starts in wave 2 (parallel with Algebra, Spreadsheet); Task 15 starts in wave 3 (depends on Spreadsheet(13))

14. Implement `gspread`-based client wrapper for Google Sheets API
15. Implement `SheetsExecutor` class with batch operations, error handling, and retry logic

> **Formal semantics compliance** (ARCHITECTURE.md §Google Sheets Executor):
> - The executor is the only component that communicates with Google Sheets. All other components (algebra, translator, execution plan) are pure and API-free.
> - Must use the `gspread` operations specified in the architecture: `gc.create`, `spreadsheet.add_worksheet`, `worksheet.update`, `spreadsheet.batch_update`.
> - Transient failures (rate limits, network blips, service errors) must be handled with exponential backoff and post-operation validation.
> - Large datasets exceeding Google Sheets limits (10M cells, ~5M formula cells) must be refused with a clear error *before* making any API calls.

## Utilities — `utils/`

### Tasks

> **Agent: Utilities** (wave 3) — depends on Algebra; parallel with Core, Translator, Executor(15)

16. Add plan visualization (text or graphical)
17. Implement plan serialization (JSON)

## Error Handling

> **Agent: Scaffolding** (wave 1)

```python
class UnsupportedOperationError(Exception):
    """Raised when operation cannot be translated to spreadsheet"""

class SheetsAPIError(Exception):
    """Raised when Google Sheets API call fails"""

class PlanValidationError(Exception):
    """Raised when plan is invalid"""
```

## Dependencies

> **Agent: Scaffolding** (wave 1)

**Core:**

- `pandas` - DataFrame compatibility
- `gspread` - Google Sheets API client (high-level wrapper around Sheets API v4)
- `google-auth` - Google authentication (service accounts and OAuth2)
- `google-auth-oauthlib` - OAuth2 flow for end-user credentials
- `typing-extensions` - Advanced type hints
- `dataclasses` - Clean data structures (Python 3.7+)

## Extensions

> Post-implementation — after all primary agents complete

18. Incremental updates (modify existing sheets)
19. Bidirectional sync (read operations from sheets)
20. Performance optimization for large datasets

## Testing & Documentation

> Per-task unit tests are owned by each agent (see subsections below). Tasks 21–22 cover cross-cutting integration/E2E tests and run after all primary agents complete.

21. Write unit, integration, end-to-end, and property-based tests
22. Add comprehensive documentation and example notebooks

### Unit Tests by Task

Every test below is a pure unit test — no Google Sheets API, no network, no filesystem (except reading fixture CSVs). Mock or stub external dependencies wherever they appear.

#### Core — Tasks 1–4 *(Agents: Scaffolding + Core)*

**Task 2: `fornero.DataFrame` subclass**

- Constructing a `fornero.DataFrame` from raw data attaches a `LogicalPlan` with a `Source` root node.
- Constructing from an existing pandas `DataFrame` (e.g., `fornero.DataFrame(pd_df)`) preserves all data and attaches a fresh plan.
- The `_plan` attribute survives pandas operations that return new frames (slicing, copying). Verify via `_metadata` propagation.
- `to_spreadsheet_plan()` calls the translator on the attached plan and returns the result.

**Task 3: `fornero.__init__` re-exports**

- `import fornero as pd; pd.DataFrame(...)` produces a `fornero.DataFrame`, not a bare `pd.DataFrame`.
- `pd.read_csv(...)` (stubbed to avoid filesystem) returns a `fornero.DataFrame` with a `Source` node.
- All re-exported pandas functions (`pd.merge`, `pd.concat`, etc.) are callable and return tracked frames.

**Task 4: Operation tracer**

- Each traced operation (`filter`, `select`, `sort`, `groupby`, `merge`, `head`, `assign`) appends the correct algebra node to `_plan`.
- Chaining multiple operations produces a plan whose nodes are nested in the right order (e.g., `Select(Filter(Source))` for `df.filter(...).select(...)`).
- The tracer captures operation arguments faithfully: column names, predicate expressions, sort directions, aggregation functions.
- Operations that pandas executes eagerly still produce correct pandas results alongside the plan (dual-mode invariant).

#### Dataframe Algebra — Tasks 5–6 *(Agent: Algebra)*

**Task 5: Operation nodes**

For each operation class (`Source`, `Select`, `Filter`, `Join`, `GroupBy`, `Aggregate`, `Sort`, `Limit`, `WithColumn`, `Union`, `Pivot`, `Melt`, `Window`):

- Construction with valid arguments succeeds and stores all fields.
- Construction with invalid arguments (e.g., empty column list for `Select`, unknown join type for `Join`) raises `ValueError` or a typed validation error.
- `to_dict()` returns a JSON-serializable dictionary with a `type` discriminator and all fields.
- Round-trip: `Operation.from_dict(op.to_dict())` produces an equivalent node (structural equality).
- `inputs` is always a list; unary nodes have one input, binary nodes have two, `Source` has zero.

Operation-specific checks:

- `Join`: the `join_type` field accepts exactly `inner`, `left`, `right`, `outer`.
- `GroupBy`: aggregation tuples `(output_name, function, input_column)` are stored and round-trip correctly.
- `Sort`: direction flags (`asc`/`desc`) are preserved per key.
- `Limit`: `head` vs `tail` selector is stored.
- `Window`: partition keys, order keys, and frame spec are all captured.

Additional checks required by formal semantics (ARCHITECTURE.md §Dataframe Algebra):

- `WithColumn`: test replace-in-place behavior — if column `c` already exists in the schema, the operation replaces it at the same column position; if `c` is new, it is appended after all existing columns.
- `Union`: construction must validate `S(R₁) = S(R₂)` and raise `ValueError` if schemas differ.
- `Pivot`: construction must accept an optional aggregation function for duplicate matches (default: `first`). Missing cells produce null.
- `Aggregate`: must be structurally equivalent to `GroupBy` with an empty key set (`K = []`). Always produces exactly one row.
- `GroupBy`: the order of groups in the output must match the order of first appearance in `R`.
- `Join`: output schema is `S(R₁) ∪ S(R₂) \ {k₂}` — verify the join key from R₂ is dropped from the output schema, not duplicated.
- `Sort`: verify stable sort — rows with equal keys must preserve their original relative order.

**Task 6: `plan.explain()`**

- A single-node plan (just `Source`) produces output mentioning the source and zero operations.
- A multi-step plan produces output listing each operation in tree order (root last, leaves first — or the reverse, as long as it's consistent).
- The output is a string (not a data structure) suitable for printing.
- Operation-specific details appear: column names for `Select`, predicate text for `Filter`, join keys and type for `Join`.

#### Spreadsheet Algebra — Task 7 *(Agent: Spreadsheet)*

**Task 7: Model classes**

- `Sheet(name, rows, cols)` stores dimensions; name must be non-empty.
- `Range` from string: `Range.from_a1("A2:C100")` parses correctly; `Range.from_a1("ZZ1")` handles multi-letter columns.
- `Range` to string: `range.to_a1()` round-trips with `from_a1`.
- `Range` arithmetic: expanding, intersecting, and offsetting ranges produce correct results.
- `Formula` stores its expression string; `str(formula)` prepends `=` if missing.
- `Reference` distinguishes same-sheet (`A1:B10`) from cross-sheet (`Sheet2!A1:B10`) references.
- `Value` wraps Python scalars (str, int, float, bool, None) and converts them to spreadsheet-safe representations (e.g., `None` → empty string).

#### Translator — Tasks 8–12 *(Agent: Translator)*

All translator tests operate on hand-built algebra trees (no real DataFrames needed). The output is a list of spreadsheet algebra operations; assert on their types, target sheets, and formula strings.

**Task 8: Translation strategies**

For each strategy, build a minimal algebra tree and translate it:

- **Select** → produces `CreateSheet`, header `SetValues`, and one `SetFormula` per selected column referencing the source column range.
- **Filter** → produces `CreateSheet`, header `SetValues`, and a single `SetFormula` containing a `FILTER(...)` expression. Verify the predicate translates correctly: `>`, `<`, `=`, `>=`, `<=`, `!=`, `AND` (multiplication), `OR` (addition).
- **Join (inner)** → produces `CreateSheet`, header `SetValues`, array reference for left-side columns, `XLOOKUP` formulas for right-side columns. A follow-up helper sheet with `FILTER` removes unmatched rows.
- **Join (left)** → same as inner but no helper-sheet filter; `XLOOKUP` if-not-found argument is empty string.
- **GroupBy** → produces `CreateSheet` and a `QUERY` formula with correct `SELECT ... GROUP BY ...` clause. Verify function mapping: `sum→SUM`, `mean→AVG`, `count→COUNT`, `min→MIN`, `max→MAX`.
- **Aggregate** → produces `CreateSheet`, header `SetValues`, and one scalar formula per aggregation. Function mapping uses Google Sheets names (not QUERY dialect): `sum→SUM(...)`, `mean→AVERAGE(...)`, `count→COUNTA(...)`, `min→MIN(...)`, `max→MAX(...)`. Verify `COUNTA` (not `COUNT`) and `AVERAGE` (not `AVG`) — these differ from the GroupBy QUERY-dialect mapping.
- **Sort** → produces a `SORT(...)` formula with correct column index and direction (1 for asc, 0 for desc).
- **Limit** → produces an `INDEX`/`ARRAY_CONSTRAIN` or equivalent formula capping the row count. Verify both `head` and `tail` variants.
- **WithColumn** → produces formulas that reference existing columns and apply the expression.
- **Union** → produces a sheet whose formula vertically stacks two source ranges (e.g., `={source1!A2:D; source2!A2:D}`).
- **Pivot / Melt** → verify the translator either produces a valid formula decomposition or raises `UnsupportedOperationError` with a clear message.
- **Window** → verify partition-aware formulas or `UnsupportedOperationError`.

Cross-cutting translation checks:

- Translating any non-`Source` node that has no input raises an error.
- Sheet names generated by the translator are unique and deterministic for the same plan.
- Headers written by `SetValues` match the output schema of the algebra node.
- Source is the only translation rule that writes data rows via `SetValues`. Verify that every other strategy produces `SetFormula` operations for data cells — never `SetValues` for derived data.
- Translation is data-blind: given the same algebra tree structure, the translator produces identical spreadsheet operations regardless of what data values the source DataFrames contain. Test by translating the same plan tree with two different source datasets and asserting the formula strings are identical.
- GroupBy translation does not produce a header `SetValues` (QUERY emits its own header row). All other non-Source strategies that produce headers must use `SetValues`.
- Join translation drops `k₂` from the output schema — the header `SetValues` must not include the right-side join key.

**Task 9: Multi-sheet plans**

- A plan with a chain of three operations (e.g., `Filter → GroupBy → Sort`) produces at least three sheets (one per intermediate result plus sources).
- Cross-sheet references in formulas use the correct sheet names.
- Helper sheets for decomposed operations (e.g., inner join's filter step) are created and referenced correctly.

**Task 10: Optimization passes**

- **Predicate pushdown**: a plan `Select(Filter(Source))` is rewritten to `Select(Filter(Source))` with the filter closer to the source (or fused). If the filter references only columns that survive the select, verify the optimized plan is structurally different from the naïve one and produces fewer sheets.
- **Projection pushdown**: a plan `Select([a, b], Join(Source1, Source2))` only carries columns `a` and `b` through the join, dropping unused columns early.
- **Formula simplification**: a `Select` of all columns (identity projection) is elided; a `Filter` with a tautological predicate is elided.
- Each optimization is idempotent: applying it twice produces the same plan as applying it once.
- Optimizations preserve the plan's logical semantics: for a fixed input, the optimized plan's `explain()` output describes an equivalent computation (test by comparing unoptimized and optimized translation output for a known simple case).
- Optimizations operate only on the plan tree, never on data values — consistent with the data-blindness invariant (ARCHITECTURE.md §Translator).

**Task 11: Lambda support**

- `lambda x: x * 2` translates to `=A2 * 2` (with correct cell reference).
- `lambda x: x + 1` translates to `=A2 + 1`.
- `lambda x: x.upper()` (string method) raises `UnsupportedOperationError`.
- Lambdas referencing multiple columns (e.g., `lambda row: row['a'] + row['b']`) translate to `=A2 + B2`.
- Nested arithmetic (`lambda x: (x * 2) + 3`) translates to `=(A2 * 2) + 3`.

**Task 12: Apps Script integration**

- A complex function triggers Apps Script generation rather than a formula.
- The generated script contains a valid function signature and body.
- The formula cell references the custom function by name (e.g., `=CUSTOM_FN(A2)`).
- The translator attaches the script to the execution plan metadata so the executor can deploy it.

#### Execution Plan — Task 13 *(Agent: Spreadsheet)*

(Already detailed in the Execution Plan section above; reproduced here for completeness.)

- A single-source plan (one `CreateSheet`, one `SetValues`, no formulas) produces two steps in the right order.
- A plan with cross-sheet formulas places `WriteSourceData` for the referenced sheet before `WriteFormulas` for the referencing sheet.
- A plan with a formula referencing a nonexistent sheet raises `PlanValidationError`.
- `explain()` output includes sheet count, formula count, and step count.
- `to_dict()` round-trips: `ExecutionPlan.from_dict(plan.to_dict())` produces an equivalent plan.

Additional:

- An empty operation list produces an empty plan (zero steps) without error.
- `CreateSheet` operations with duplicate sheet names raise `PlanValidationError`.
- `WriteFormulas` step respects topological order: if formula on sheet B references sheet A, sheet A's data step precedes sheet B's formula step.
- `RegisterNamedRanges` step always comes after all `WriteFormulas` steps.
- The main-sheet tracker correctly identifies the root output sheet.

Formal semantics compliance (ARCHITECTURE.md §Execution Plan, §Spreadsheet Algebra):

- Step execution order must be verified as exactly: `CreateSheets → WriteSourceData → WriteFormulas → RegisterNamedRanges`. No other ordering is valid.
- `CreateSheet` with a duplicate sheet name (`s ∈ dom(W)`) must raise `PlanValidationError` — the formal semantics define this case as undefined.
- Topological sort of formulas: if formula on sheet B contains a reference `A!Range`, then sheet A's `WriteSourceData` or `WriteFormulas` step must precede sheet B's `WriteFormulas` step.
- `RegisterNamedRanges` always comes after all `WriteFormulas` steps, since named ranges may reference formula-populated ranges.

#### Google Sheets Executor — Tasks 14–15 *(Agent: Executor)*

All tests mock `gspread` — no real API calls.

**Task 14: Client wrapper**

- `create_spreadsheet(title)` calls `gc.create(title)` and returns a wrapper around the result.
- `add_sheet(name, rows, cols)` calls `spreadsheet.add_worksheet(...)` with correct arguments.
- `write_values(sheet, range, values)` calls `worksheet.update(range, values)`.
- `write_formula(sheet, cell, formula)` calls `worksheet.update(cell, formula, raw=False)`.
- Authentication: the wrapper initializes `gspread` with the provided credentials object (mock the auth flow).
- Error wrapping: a `gspread.exceptions.APIError` is caught and re-raised as `SheetsAPIError` with the original message.

**Task 15: `SheetsExecutor`**

- `execute(plan)` iterates the plan's steps in order, calling the client wrapper for each.
- Batch grouping: multiple `CreateSheet` operations in a single step result in one `batch_update` call (not N individual calls).
- Rate limiting: if the mock raises a 429 error on the first call and succeeds on the second, the executor retries and succeeds.
- Exponential backoff: verify the delay between retries increases (mock `time.sleep` and inspect call args).
- Max retries: after N consecutive failures the executor raises `SheetsAPIError`.
- Large dataset guard: if the plan's total cell count exceeds the 10M limit, the executor raises before making any API calls.
- The executor returns metadata about the created spreadsheet (URL, sheet names, cell counts).

Formal semantics compliance (ARCHITECTURE.md §Google Sheets Executor):

- The executor must be the only component that touches the Google Sheets API. Verify that no algebra, translator, or plan module imports `gspread` or makes network calls.
- The executor uses only the `gspread` operations listed in the architecture: `gc.create`, `spreadsheet.add_worksheet`, `worksheet.update`, `worksheet.get_all_values`/`worksheet.get`, `worksheet.format`, `spreadsheet.batch_update`.
- Post-operation validation: after writing to a sheet, the executor should confirm the sheet exists before proceeding to dependent writes (e.g., verifying a sheet was created before writing formulas that reference it).

#### Utilities — Tasks 16–17 *(Agent: Utilities)*

**Task 16: Visualization**

- `visualize(plan)` for a single-node plan returns a string containing the node type.
- `visualize(plan)` for a multi-step plan returns a tree-shaped string with indentation or connectors showing parent–child relationships.
- The output is deterministic (same plan → same string).

**Task 17: Serialization**

- `serialize(plan)` returns a JSON-serializable `dict`.
- `deserialize(serialize(plan))` produces a plan structurally equal to the original (round-trip property).
- Serialized output includes a version key for forward compatibility.
- Serializing a plan with all operation types succeeds without error.
- Deserializing a dict with an unknown operation type raises a clear error.
- Deserializing a dict with a missing required field raises a clear error.

#### Extensions — Tasks 18–20 *(post-implementation)*

**Task 18: Incremental updates**

- Diffing two plans (old and new) produces a minimal set of spreadsheet operations (only the changed sheets/cells).
- If only a filter predicate changes, the diff contains a single `SetFormula` update, not a full re-creation.
- If a new column is added, the diff contains a `SetFormula` for the new column and updated headers, but does not re-write unchanged columns.

**Task 19: Bidirectional sync**

- Reading a spreadsheet's structure back produces a `LogicalPlan` that, when translated, would regenerate the same spreadsheet (round-trip at the plan level).
- Formulas are parsed back into operation nodes with correct types and arguments.

**Task 20: Performance optimization**

- The batch size calculator partitions N operations into ceil(N / batch_limit) groups.
- Formula dependency analysis correctly identifies independent formulas that can be written in parallel.
- Cell-count estimation for a plan matches the sum of (rows × cols) across all sheets.
