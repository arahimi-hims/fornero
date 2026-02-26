"""Program 15: join_groupby — Join → GroupBy + Aggregate. Output ≈ 3×3."""

import fornero as pd
from tests.programs import ProgramResult
from tests.helpers.comparison import extract_source_data

PROGRAM_NAME = "join_groupby"
OPERATIONS = ["Source", "Join", "GroupBy"]


def run() -> ProgramResult:
    emp_data = {
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve",
                 "Frank", "Grace", "Hank", "Ivy", "Jack"],
        "age": [30, 25, 35, 28, 32, 45, 29, 38, 27, 33],
        "dept": ["eng", "eng", "sales", "eng", "sales",
                 "hr", "eng", "sales", "hr", "eng"],
        "salary": [95000, 85000, 72000, 90000, 78000,
                   65000, 88000, 70000, 62000, 92000],
    }
    dept_data = {
        "dept_id": ["eng", "sales", "hr"],
        "budget": [500000, 300000, 200000],
    }

    employees = pd.DataFrame(emp_data, source_id="employees")
    departments = pd.DataFrame(dept_data, source_id="departments")

    joined = employees.merge(departments, left_on="dept", right_on="dept_id", how="inner")
    result = joined.groupby("dept").agg({"salary": "sum"})

    source_data = {
        "employees": extract_source_data(employees),
        "departments": extract_source_data(departments),
    }
    return ProgramResult(result=result, source_data=source_data)
