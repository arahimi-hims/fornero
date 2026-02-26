"""Program 9: select_then_sort — Select → Sort. Output ≈ 10×2."""

import fornero as pd
from tests.programs import ProgramResult
from tests.helpers.comparison import extract_source_data

PROGRAM_NAME = "select_then_sort"
OPERATIONS = ["Source", "Select", "Sort"]


def run() -> ProgramResult:
    data = {
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve",
                 "Frank", "Grace", "Hank", "Ivy", "Jack"],
        "age": [30, 25, 35, 28, 32, 45, 29, 38, 27, 33],
        "dept": ["eng", "eng", "sales", "eng", "sales",
                 "hr", "eng", "sales", "hr", "eng"],
        "salary": [95000, 85000, 72000, 90000, 78000,
                   65000, 88000, 70000, 62000, 92000],
    }
    df = pd.DataFrame(data, source_id="employees")
    selected = df[["name", "age"]]
    result = selected.sort_values("age")

    source_data = {"employees": extract_source_data(df)}
    return ProgramResult(result=result, source_data=source_data)
