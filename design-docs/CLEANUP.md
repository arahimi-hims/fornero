# Cleanup Strategy - Parallel Execution Plan

This document defines independent cleanup tasks that can be executed in parallel by multiple agents. Each task is scoped to specific files/modules to avoid conflicts.

---

## Agent 1: Algebra Semantics Verification

**Scope:** `src/fornero/algebra/`

**Task:** Compare the implementation of dataframe algebra operations against the formal semantics in `design-docs/ARCHITECTURE.md` (sections on Select, Filter, Join, GroupBy, Sort, Limit, WithColumn, Aggregate, Union, Pivot, Melt, Window).

**Files:**

- `src/fornero/algebra/operations.py`
- `src/fornero/algebra/logical_plan.py`
- `src/fornero/algebra/expressions.py`

**Deliverables:**

- List of discrepancies between formal semantics and implementation
- For each discrepancy: file path, line number, what the spec says vs. what the code does
- Recommendation: fix implementation or update spec

**Acceptance Criteria:**

- Every operation class in `operations.py` is checked against its formal definition
- Expression handling in `expressions.py` is verified for correctness
- Output is a markdown table: `| Operation | Location | Issue | Spec Says | Code Does | Recommendation |`

---

## Agent 2: Spreadsheet Algebra & Translation Semantics

**Scope:** `src/fornero/spreadsheet/`, `src/fornero/translator/`

**Task:** Compare spreadsheet algebra and translation logic against the formal semantics in `design-docs/ARCHITECTURE.md` (spreadsheet algebra section and translation rules).

**Files:**

- `src/fornero/spreadsheet/model.py`
- `src/fornero/spreadsheet/operations.py`
- `src/fornero/translator/converter.py`
- `src/fornero/translator/strategies.py`

**Deliverables:**

- List of discrepancies in spreadsheet algebra implementation
- List of discrepancies in dataframe→spreadsheet translation rules
- For each: file path, line number, issue description, recommendation

**Acceptance Criteria:**

- All spreadsheet operation classes verified against spec
- Translation strategies checked for correctness
- Output is a markdown table with discrepancies and recommendations

---

## Agent 3: Executor Semantics Verification

**Scope:** `src/fornero/executor/`

**Task:** Verify executor implementation matches the execution semantics described in `design-docs/ARCHITECTURE.md` (execution plan and Google Sheets API interaction).

**Files:**

- `src/fornero/executor/plan.py`
- `src/fornero/executor/sheets_executor.py`
- `src/fornero/executor/sheets_client.py`
- `src/fornero/executor/base.py`

**Deliverables:**

- List of discrepancies between spec and implementation
- Verification that batching, error handling, and API call patterns match design
- For each issue: file path, line number, description, recommendation

**Acceptance Criteria:**

- Execution plan structure verified
- Sheets API call patterns checked
- Output is a markdown table with findings

---

## Agent 4: Unused Imports - Core & Algebra

**Scope:** `src/fornero/core/`, `src/fornero/algebra/`

**Task:** Remove unused imports using automated tools (ruff) and manual verification.

**Files:**

- `src/fornero/core/dataframe.py`
- `src/fornero/core/tracer.py`
- `src/fornero/core/__init__.py`
- `src/fornero/algebra/operations.py`
- `src/fornero/algebra/logical_plan.py`
- `src/fornero/algebra/expressions.py`
- `src/fornero/algebra/eager.py`
- `src/fornero/algebra/__init__.py`

**Approach:**

1. Run `ruff check --select F401 <file>` to detect unused imports
2. Run `ruff check --fix --select F401 <file>` to auto-remove
3. Verify tests still pass: `pytest tests/`

**Deliverables:**

- Cleaned files with unused imports removed
- Summary report: number of imports removed per file

**Acceptance Criteria:**

- No unused imports remain (verified by ruff)
- All tests pass after cleanup

---

## Agent 5: Unused Imports - Translator & Executor

**Scope:** `src/fornero/translator/`, `src/fornero/executor/`

**Task:** Remove unused imports using automated tools (ruff) and manual verification.

**Files:**

- `src/fornero/translator/converter.py`
- `src/fornero/translator/strategies.py`
- `src/fornero/translator/optimizer.py`
- `src/fornero/translator/lambda_analyzer.py`
- `src/fornero/translator/apps_script.py`
- `src/fornero/translator/__init__.py`
- `src/fornero/executor/plan.py`
- `src/fornero/executor/sheets_executor.py`
- `src/fornero/executor/sheets_client.py`
- `src/fornero/executor/base.py`
- `src/fornero/executor/__init__.py`

**Approach:**

1. Run `ruff check --select F401 <file>` to detect unused imports
2. Run `ruff check --fix --select F401 <file>` to auto-remove
3. Verify tests still pass: `pytest tests/`

**Deliverables:**

- Cleaned files with unused imports removed
- Summary report: number of imports removed per file

**Acceptance Criteria:**

- No unused imports remain (verified by ruff)
- All tests pass after cleanup

---

## Agent 6: Unused Imports - Spreadsheet & Utils

**Scope:** `src/fornero/spreadsheet/`, `src/fornero/utils/`

**Task:** Remove unused imports using automated tools (ruff) and manual verification.

**Files:**

- `src/fornero/spreadsheet/model.py`
- `src/fornero/spreadsheet/operations.py`
- `src/fornero/spreadsheet/formulas.py`
- `src/fornero/spreadsheet/__init__.py`
- `src/fornero/utils/serialization.py`
- `src/fornero/utils/visualization.py`
- `src/fornero/utils/__init__.py`
- `src/fornero/exceptions.py`
- `src/fornero/__init__.py`

**Approach:**

1. Run `ruff check --select F401 <file>` to detect unused imports
2. Run `ruff check --fix --select F401 <file>` to auto-remove
3. Verify tests still pass: `pytest tests/`

**Deliverables:**

- Cleaned files with unused imports removed
- Summary report: number of imports removed per file

**Acceptance Criteria:**

- No unused imports remain (verified by ruff)
- All tests pass after cleanup

---

## Agent 7: Packing/Unpacking - Algebra Layer

**Scope:** `src/fornero/algebra/`

**Task:** Identify and eliminate unnecessary conversions between collection types (tuple↔dict, list↔tuple) within algebra operations.

**Files:**

- `src/fornero/algebra/operations.py`
- `src/fornero/algebra/logical_plan.py`
- `src/fornero/algebra/expressions.py`

**Pattern to Find:**

- Functions returning tuples/lists that are immediately unpacked by ALL callers
- Functions accepting dicts that are created from tuples by ALL callers
- Back-and-forth conversions across function boundaries

**Approach:**

1. Search for return statements with tuple/list/dict literals
2. Find all call sites for each function
3. Check if callers consistently pack/unpack the same way
4. Propose simplified signatures
5. Calculate LOC savings

**Deliverables:**

- Markdown table: `| Function | Current Pattern | Simpler Pattern | LOC Saved | Files Changed |`
- Detailed analysis for each identified pattern

**Acceptance Criteria:**

- At least 3 patterns identified OR conclusive report that none exist
- Each finding includes specific line numbers and caller analysis
- Proposed refactorings are validated for correctness

---

## Agent 8: Packing/Unpacking - Translator Layer

**Scope:** `src/fornero/translator/`

**Task:** Identify and eliminate unnecessary conversions between collection types within translation logic.

**Files:**

- `src/fornero/translator/converter.py`
- `src/fornero/translator/strategies.py`
- `src/fornero/translator/optimizer.py`

**Pattern to Find:**

- Functions returning tuples/lists that are immediately unpacked by ALL callers
- Functions accepting dicts that are created from tuples by ALL callers
- Back-and-forth conversions across function boundaries

**Approach:**

1. Search for return statements with tuple/list/dict literals
2. Find all call sites for each function
3. Check if callers consistently pack/unpack the same way
4. Propose simplified signatures
5. Calculate LOC savings

**Deliverables:**

- Markdown table: `| Function | Current Pattern | Simpler Pattern | LOC Saved | Files Changed |`
- Detailed analysis for each identified pattern

**Acceptance Criteria:**

- At least 3 patterns identified OR conclusive report that none exist
- Each finding includes specific line numbers and caller analysis
- Proposed refactorings are validated for correctness

---

## Agent 9: Packing/Unpacking - Executor & Spreadsheet

**Scope:** `src/fornero/executor/`, `src/fornero/spreadsheet/`

**Task:** Identify and eliminate unnecessary conversions between collection types in executor and spreadsheet model code.

**Files:**

- `src/fornero/executor/plan.py`
- `src/fornero/executor/sheets_executor.py`
- `src/fornero/executor/sheets_client.py`
- `src/fornero/spreadsheet/model.py`
- `src/fornero/spreadsheet/operations.py`

**Pattern to Find:**

- Functions returning tuples/lists that are immediately unpacked by ALL callers
- Functions accepting dicts that are created from tuples by ALL callers
- Back-and-forth conversions across function boundaries

**Approach:**

1. Search for return statements with tuple/list/dict literals
2. Find all call sites for each function
3. Check if callers consistently pack/unpack the same way
4. Propose simplified signatures
5. Calculate LOC savings

**Deliverables:**

- Markdown table: `| Function | Current Pattern | Simpler Pattern | LOC Saved | Files Changed |`
- Detailed analysis for each identified pattern

**Acceptance Criteria:**

- At least 3 patterns identified OR conclusive report that none exist
- Each finding includes specific line numbers and caller analysis
- Proposed refactorings are validated for correctness

---

## Agent 10: Overly Defensive Code - Algebra & Core

**Scope:** `src/fornero/algebra/`, `src/fornero/core/`

**Task:** Find and remove unnecessary Optional/None type hints and defensive checks where None is never actually passed.

**Files:**

- `src/fornero/algebra/operations.py`
- `src/fornero/algebra/logical_plan.py`
- `src/fornero/algebra/expressions.py`
- `src/fornero/core/dataframe.py`
- `src/fornero/core/tracer.py`

**Approach:**

1. Grep for `Optional[` and `| None` type hints
2. For each function with Optional parameters:
   - Find all call sites in the codebase
   - Check if None is ever passed
   - If never None: remove Optional, remove None checks (if/is None)
3. Run tests to confirm

**Deliverables:**

- Markdown table: `| Function | Parameter | Actually Optional? | Action Taken |`
- List of simplified type hints and removed defensive code

**Acceptance Criteria:**

- All Optional parameters analyzed
- Type hints simplified where appropriate
- Tests pass after changes

---

## Agent 11: Overly Defensive Code - Translator & Executor

**Scope:** `src/fornero/translator/`, `src/fornero/executor/`, `src/fornero/spreadsheet/`

**Task:** Find and remove unnecessary Optional/None type hints and defensive checks where None is never actually passed.

**Files:**

- `src/fornero/translator/converter.py`
- `src/fornero/translator/strategies.py`
- `src/fornero/translator/optimizer.py`
- `src/fornero/executor/plan.py`
- `src/fornero/executor/sheets_executor.py`
- `src/fornero/executor/sheets_client.py`
- `src/fornero/spreadsheet/model.py`
- `src/fornero/spreadsheet/operations.py`

**Approach:**

1. Grep for `Optional[` and `| None` type hints
2. For each function with Optional parameters:
   - Find all call sites in the codebase
   - Check if None is ever passed
   - If never None: remove Optional, remove None checks (if/is None)
3. Run tests to confirm

**Deliverables:**

- Markdown table: `| Function | Parameter | Actually Optional? | Action Taken |`
- List of simplified type hints and removed defensive code

**Acceptance Criteria:**

- All Optional parameters analyzed
- Type hints simplified where appropriate
- Tests pass after changes

---

## Execution Notes

- **Parallelization:** Agents 1-3 (semantics), 4-6 (imports), 7-9 (packing), and 10-11 (defensive code) can run fully in parallel
- **Dependencies:** None - all tasks are independent
- **Conflict Resolution:** Each agent works on distinct file sets or performs read-only analysis
- **Testing:** Agents that modify code (4-6, 10-11) must run `pytest tests/` to verify changes
- **Reporting:** All agents should output findings to `design-docs/cleanup-results/agent-N-<name>.md`
