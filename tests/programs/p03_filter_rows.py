"""Program 3: filter_rows — Filter. Output ≈ 5×4."""

import fornero as pd
from tests.programs import ProgramResult
from tests.helpers.comparison import extract_source_data

PROGRAM_NAME = "filter_rows"
OPERATIONS = ["Source", "Filter"]


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
    result = df[df["age"] > 30]

    source_data = {"employees": extract_source_data(df)}
    return ProgramResult(result=result, source_data=source_data)
