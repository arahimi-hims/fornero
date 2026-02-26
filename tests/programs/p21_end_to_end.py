"""Program 21: end_to_end — Mirror of examples/end_to_end_demo.py as a test program.

Exercises the full pipeline: Filter → WithColumn → Sort → Select.
Output ≈ 7×3.
"""

import fornero as pd
from tests.programs import ProgramResult
from tests.helpers.comparison import extract_source_data

PROGRAM_NAME = "end_to_end"
OPERATIONS = ["Source", "Filter", "WithColumn", "Sort", "Select"]


def run() -> ProgramResult:
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

    source_data = {"employees": extract_source_data(employees)}
    return ProgramResult(result=result, source_data=source_data)
