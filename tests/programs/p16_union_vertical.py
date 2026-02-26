"""Program 16: union_vertical — Union (two frames stacked). Output ≈ 20×4."""

import fornero as pd
from tests.programs import ProgramResult
from tests.helpers.comparison import extract_source_data

PROGRAM_NAME = "union_vertical"
OPERATIONS = ["Source", "Union"]


def run() -> ProgramResult:
    data_a = {
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve",
                 "Frank", "Grace", "Hank", "Ivy", "Jack"],
        "age": [30, 25, 35, 28, 32, 45, 29, 38, 27, 33],
        "dept": ["eng", "eng", "sales", "eng", "sales",
                 "hr", "eng", "sales", "hr", "eng"],
        "salary": [95000, 85000, 72000, 90000, 78000,
                   65000, 88000, 70000, 62000, 92000],
    }
    data_b = {
        "name": ["Kate", "Liam", "Mia", "Noah", "Olivia",
                 "Pete", "Quinn", "Rosa", "Sam", "Tina"],
        "age": [26, 31, 44, 22, 37, 41, 28, 34, 39, 30],
        "dept": ["eng", "hr", "sales", "eng", "hr",
                 "sales", "eng", "hr", "sales", "eng"],
        "salary": [82000, 68000, 75000, 80000, 71000,
                   73000, 87000, 66000, 69000, 91000],
    }

    df_a = pd.DataFrame(data_a, source_id="team_a")
    df_b = pd.DataFrame(data_b, source_id="team_b")
    result = pd.concat([df_a, df_b])

    source_data = {
        "team_a": extract_source_data(df_a),
        "team_b": extract_source_data(df_b),
    }
    return ProgramResult(result=result, source_data=source_data)
