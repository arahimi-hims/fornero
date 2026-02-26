"""Program 20: pivot_simple — Pivot (reshape). Output ≈ 4×3."""

import fornero as pd
from tests.programs import ProgramResult
from tests.helpers.comparison import extract_source_data

PROGRAM_NAME = "pivot_simple"
OPERATIONS = ["Source", "Pivot"]


def run() -> ProgramResult:
    data = {
        "dept": ["eng", "eng", "sales", "sales", "hr", "hr"],
        "quarter": ["Q1", "Q2", "Q1", "Q2", "Q1", "Q2"],
        "revenue": [100, 120, 80, 90, 50, 55],
    }
    df = pd.DataFrame(data, source_id="quarterly")

    result = df.pivot_table(
        index="dept", columns="quarter", values="revenue", aggfunc="sum"
    )

    source_data = {"quarterly": extract_source_data(df)}
    return ProgramResult(result=result, source_data=source_data)
