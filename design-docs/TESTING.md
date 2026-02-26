# End-to-End Correctness Testing

The core promise of fornero is that the spreadsheet it produces contains the same numbers as the pandas program that generated it. To validate this, we use a **matrix comparison suite**: a collection of ~20 small dataframe programs, each producing a final 2D result, that we run through the full pipeline and compare cell-by-cell against the pandas output.

## Approach

1. **Generate a corpus of ~20 dataframe programs.** Each program uses a different combination of operations (filter, join, groupby, sort, computed columns, etc.) and produces a deterministic 2D matrix as its final output — the "expected" result.
2. **Run each program through fornero.** The program executes normally under `import fornero as pd`, building a logical plan. We then call `to_spreadsheet_plan()` and execute the plan against Google Sheets via `gspread`.
3. **Read back the main sheet.** Using `gspread`'s `worksheet.get_all_values()`, fetch the primary output sheet of the generated spreadsheet.
4. **Compare the spreadsheet matrix against the pandas DataFrame.** Assert that every cell in the spreadsheet matches the corresponding cell in the expected DataFrame, within a tolerance for floating-point values.

## Test Program Corpus

Each program is a self-contained Python function that returns the final `fornero.DataFrame`. The programs are designed to cover the operation space progressively:

| #   | Program Name               | Operations Used                        | Output Shape (approx) |
| --- | -------------------------- | -------------------------------------- | --------------------- |
| 1   | `identity`                 | Source only (no transforms)            | 10 × 4                |
| 2   | `select_columns`           | Select                                 | 10 × 2                |
| 3   | `filter_rows`              | Filter                                 | ~5 × 4                |
| 4   | `sort_single`              | Sort (single column)                   | 10 × 4                |
| 5   | `sort_multi`               | Sort (multiple columns)                | 10 × 4                |
| 6   | `head_limit`               | Limit (head)                           | 5 × 4                 |
| 7   | `computed_column`          | WithColumn (arithmetic)                | 10 × 5                |
| 8   | `filter_then_select`       | Filter → Select                        | ~5 × 2                |
| 9   | `select_then_sort`         | Select → Sort                          | 10 × 2                |
| 10  | `inner_join`               | Join (inner)                           | ~8 × 6                |
| 11  | `left_join`                | Join (left)                            | 10 × 6                |
| 12  | `groupby_sum`              | GroupBy + Aggregate (sum)              | ~4 × 2                |
| 13  | `groupby_multi_agg`        | GroupBy + Aggregate (sum, mean, count) | ~4 × 4                |
| 14  | `filter_join_select`       | Filter → Join → Select                 | ~5 × 3                |
| 15  | `join_groupby`             | Join → GroupBy + Aggregate             | ~4 × 3                |
| 16  | `union_vertical`           | Union (two frames stacked)             | 20 × 4                |
| 17  | `computed_then_filter`     | WithColumn → Filter                    | ~5 × 5                |
| 18  | `multi_step_pipeline`      | Filter → WithColumn → Sort → Select    | ~5 × 3                |
| 19  | `join_filter_groupby_sort` | Join → Filter → GroupBy → Sort         | ~3 × 3                |
| 20  | `pivot_simple`             | Pivot (reshape)                        | ~4 × 3                |

## What "Main Page" Means

Each program produces a spreadsheet that may contain multiple tabs (source data tabs, helper tabs for intermediate computations). The **main page** is always the first tab — the one representing the final output of the dataframe pipeline. This is the tab whose content must match the pandas result. Helper and source tabs are internal scaffolding and are not compared.

## Running the Suite

- **Full suite (live):** `pytest tests/test_correctness.py -v` — requires Google Sheets API credentials; creates and tears down real spreadsheets.
- **Offline / CI mode:** Mock the executor to capture the execution plan and verify the generated formulas structurally, without hitting the Sheets API. This validates that the translation is correct even in environments without API access.
- **Single program:** `pytest tests/test_correctness.py -k "groupby_sum" -v` — run one program for quick iteration.

## File Structure

The correctness tests live alongside the unit tests in the `tests/` directory (see full project tree in [IMPLEMENTATION.md](IMPLEMENTATION.md#file-structure)). The e2e-specific files:

```
tests/
├── test_correctness.py          # Parametrized test runner
├── programs/                    # Test program corpus
│   ├── __init__.py
│   ├── p01_identity.py
│   ├── p02_select_columns.py
│   ├── p03_filter_rows.py
│   ├── p04_sort_single.py
│   ├── p05_sort_multi.py
│   ├── p06_head_limit.py
│   ├── p07_computed_column.py
│   ├── p08_filter_then_select.py
│   ├── p09_select_then_sort.py
│   ├── p10_inner_join.py
│   ├── p11_left_join.py
│   ├── p12_groupby_sum.py
│   ├── p13_groupby_multi_agg.py
│   ├── p14_filter_join_select.py
│   ├── p15_join_groupby.py
│   ├── p16_union_vertical.py
│   ├── p17_computed_then_filter.py
│   ├── p18_multi_step_pipeline.py
│   ├── p19_join_filter_groupby_sort.py
│   └── p20_pivot_simple.py
└── helpers/
    ├── comparison.py            # assert_matrix_equal and utilities
    └── mock_executor.py         # Executor mock for offline/CI testing
```

## Dependencies

- `pytest` - For testing
