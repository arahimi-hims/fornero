"""
End-to-end demo: DataFrame operations → Google Sheets spreadsheet.

This script demonstrates the full fornero pipeline:
1. Build DataFrames and perform pandas-style operations (filter, sort, etc.)
2. Translate the resulting logical plan into spreadsheet operations
3. Execute those operations to create a live Google Sheet

The first two steps always run (no credentials needed).  Step 3 requires
Google Sheets credentials — see the project README for setup instructions.

Usage:
    uv run python examples/end_to_end_demo.py
"""

import sys

import gspread

import fornero as pd
from fornero.translator import Translator
from fornero.executor import ExecutionPlan, SheetsClient, SheetsExecutor


def _get_gspread_client() -> gspread.Client:
    """Authenticate with Google Sheets, trying service account then OAuth."""
    try:
        return gspread.service_account()
    except Exception:
        pass
    try:
        return gspread.oauth()
    except Exception as exc:
        print(f"Error: could not authenticate with Google Sheets: {exc}")
        print("See README for credential setup instructions.")
        sys.exit(1)


def build_dataframe():
    """Create DataFrames and perform a multi-step analysis pipeline."""
    employees = pd.DataFrame(
        {
            "name": [
                "Alice", "Bob", "Charlie", "Diana", "Eve",
                "Frank", "Grace", "Hank", "Ivy", "Jack",
            ],
            "age": [30, 25, 35, 28, 32, 45, 29, 38, 27, 33],
            "dept": [
                "eng", "eng", "sales", "eng", "sales",
                "hr", "eng", "sales", "hr", "eng",
            ],
            "salary": [
                95000, 85000, 72000, 90000, 78000,
                65000, 88000, 70000, 62000, 92000,
            ],
        },
        source_id="employees",
    )

    senior = employees[employees["age"] > 28]
    with_salary_k = senior.assign(salary_k=lambda x: x["salary"] / 1000)
    ranked = with_salary_k.sort_values("salary_k", ascending=False)
    result = ranked[["name", "dept", "salary_k"]]

    source_data = {"employees": employees.values.tolist()}
    return result, source_data


def main():
    result, source_data = build_dataframe()
    print(result.to_string(index=False))

    translator = Translator()
    ops = translator.translate(result._plan, source_data=source_data)

    gc = _get_gspread_client()
    client = SheetsClient(gc)

    executor = SheetsExecutor(client)
    plan = ExecutionPlan.from_operations(ops)
    spreadsheet = executor.execute(plan, "Fornero Demo - Employee Analysis")
    print(spreadsheet.url)


if __name__ == "__main__":
    main()
