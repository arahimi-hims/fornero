"""Run the README example to create a Google Sheet."""

import sys

import gspread
import fornero as pd
from fornero.translator import Translator
from fornero.executor import ExecutionPlan, SheetsClient, SheetsExecutor


def _get_gspread_client():
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


employees = pd.DataFrame(
    {
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [30, 25, 35, 28, 32],
        "dept": ["eng", "eng", "sales", "eng", "sales"],
        "salary": [95000, 85000, 72000, 90000, 78000],
    },
    source_id="employees",
)

senior = employees[employees["age"] > 28]
senior = senior.assign(salary_k=lambda x: x["salary"] / 1000)
result = senior.sort_values("salary_k", ascending=False)[["name", "dept", "salary_k"]]

ops = Translator().translate(
    result._plan,
    source_data={"employees": employees.values.tolist()},
)
client = SheetsClient(_get_gspread_client())
spreadsheet = SheetsExecutor(client).execute(
    ExecutionPlan.from_operations(ops), "Employee Analysis",
)
print(spreadsheet.url)
