# Fornero

Fornero is a compiler that converts pandas-style DataFrame programs into
spreadsheets. The compiler defines a formal algebra for dataframes and for
spreadsheets, and implements an optimizing compiler to translate from the former
to the latter. The algebras and the translation between them are
defined in `design-docs/ARCHITECTURE.md`

This compiler is for ex-core engineers who used to write code, but now mostly
attend meetings and write spreadsheets to be consumed by non-engineers. It lets
them plausibly claim that they "still write code", even though the only artifact
of value they produce is
spreadsheets.

## What it does

Write familiar pandas code — Fornero records every operation and produces a
live Google Sheet with equivalent formulas:

```python
import gspread
import fornero as pd
from fornero.translator import Translator
from fornero.executor import ExecutionPlan, SheetsClient, SheetsExecutor

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

# translate the recorded plan and execute it to create a live Google Sheet
ops = Translator().translate(
    result._plan,
    source_data={"employees": employees.values.tolist()},
)
client = SheetsClient(gspread.service_account())
spreadsheet = SheetsExecutor(client).execute(
    ExecutionPlan.from_operations(ops), "Employee Analysis",
)
print(spreadsheet.url)
```

Here is [the spreadsheet it produces](https://docs.google.com/spreadsheets/d/1YJ8mOm5zyEWSIJroD7eO2_oMMpdlUU5xAvK4qpBKBcc/edit?gid=0#gid=0).
Each operation (`filter`, `assign`, `sort_values`, column selection) becomes a
spreadsheet formula (`FILTER`, `ARRAYFORMULA`, `SORT`, column references) so
the generated sheet stays live — edit the source data and every derived cell
updates automatically.

## How It Works

Fornero executes every operation in two modes simultaneously. Its
`DataFrame` class subclasses `pandas.DataFrame`, so every data frame call runs
eagerly in pandas and gives you a real result to inspect. Behind the scenes,
each operation also appends a node to an internal **logical plan**, building an
algebraic tree that captures the full computation graph from source data to
final result.

The algebra layer defines a set of relational operations (`Source`, `Select`,
`Filter`, `Join`, `GroupBy`, `Sort`, `Limit`, `WithColumn`, `Union`, `Pivot`,
`Melt`, `Window`) that form the nodes of this tree. Each node records its
parameters and points back to its inputs, so the tree mirrors the chain of
transformations you wrote in Python. You can call `plan.explain()` at any point
to print a human-readable dump of the tree.

When you're ready to produce a spreadsheet, the **translator** walks the plan
tree bottom-up. For each node, the translator emits the corresponding tree of
spreadsheet instructions: creating sheets, writing static values for source
data, and setting formulas for derived data. Filters become `FILTER()` calls,
joins become `XLOOKUP`s, group-by aggregations become `QUERY` formulas, sorts
become `SORT()`, and so on. To keep the spreadsheet tidy, the translator tries
to maintain one dataframe per sheet. The translator tracks which sheet and cell
range each intermediate result occupies, so downstream formulas can reference
upstream ranges by name. The output is a flat list of spreadsheet operations
(`CreateSheet`, `SetValues`, `SetFormula`, `NamedRange`) that fully describe the
target spreadsheet. In this algebra, every dataframe operator becomes a sheet
in the spreadsheet. This can get cumbersome so are various optimization passes
to fuse operations together to reduce the number of sheets.

Finally, the **executor** takes that list of operations and materialises them in
a spreadsheet backend. Currently, there is one production backend (for Google
Sheets). There are also backends that exist just for testing (one that just
counts cells, and another one that simulates a spreadsheet on your local
machine).

## Setting Up Google Credentials

Fornero talks to Google Sheets through [`gspread`](https://docs.gspread.org/), which needs Google API credentials. You have two options:

### Option 1: Service Account (recommended for automation)

1. Go to the [Google Cloud Console](https://console.cloud.google.com/).
2. Create a project (or select an existing one).
3. Enable the **Google Sheets API** and the **Google Drive API**.
4. Go to **IAM & Admin > Service Accounts** and create a new service account.
5. Create a JSON key for the service account and download it.
6. Place the key file at `~/.config/gspread/service_account.json`, or pass the path explicitly when authenticating.
7. Share any spreadsheets you want to access with the service account's email address (found in the JSON key file).

With the key file in the default location, you can verify it works:

```bash
uv run python -c "import gspread; print('Authenticated as', gspread.service_account().http_client.credentials.service_account_email)"
```

### Option 2: OAuth (interactive / personal use)

1. In the Google Cloud Console, go to **APIs & Services > Credentials**.
2. Create an **OAuth 2.0 Client ID** (application type: Desktop).
3. Download the credentials JSON and place it at `~/.config/gspread/credentials.json`.
4. On first use, `gspread.oauth()` will open a browser window for you to authorize access. A token is cached locally for subsequent runs.

```bash
uv run python -c "import gspread; print('Authenticated as', gspread.oauth().http_client.credentials.client_id)"
```

## Running the Examples

Create a virtual environment and install Fornero with its dev dependencies:

```bash
uv sync
```

Then run the example scripts from the project root:

```bash
# Demos DataFrame operations → translator → Google Sheet.
# Requires Google credentials
uv run python examples/end_to_end_demo.py
```

To run the test suite:

```bash
uv run pytest
```
