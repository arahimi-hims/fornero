"""
Demonstration of the executor module (Tasks 13 and 15).

This script shows how to create spreadsheet operations, build an execution plan,
and execute it against a real Google Sheets spreadsheet.

Authentication: requires either a service account JSON at
~/.config/gspread/service_account.json or OAuth credentials at
~/.config/gspread/credentials.json (browser flow on first use).
"""

import sys

import gspread

from fornero.spreadsheet import CreateSheet, SetValues, SetFormula, NamedRange
from fornero.executor import ExecutionPlan, SheetsClient, SheetsExecutor


def _get_gspread_client() -> gspread.Client:
    """Authenticate with Google Sheets, trying service account then OAuth."""
    try:
        gc = gspread.service_account()
        print("✓ Authenticated via service account")
        return gc
    except Exception:
        pass
    try:
        gc = gspread.oauth()
        print("✓ Authenticated via OAuth")
        return gc
    except Exception as exc:
        print(f"✗ Could not authenticate with Google Sheets: {exc}")
        print()
        print("Set up credentials using one of:")
        print(
            "  • Service account: place key at ~/.config/gspread/service_account.json"
        )
        print("  • OAuth: place credentials at ~/.config/gspread/credentials.json")
        sys.exit(1)


def main():
    """Demonstrate the execution plan workflow with live Sheets execution."""

    print("=" * 70)
    print("Fornero Executor Module Demo")
    print("=" * 70)
    print()

    # Define a simple spreadsheet with source data and derived calculations
    operations = [
        # Create source data sheet
        CreateSheet(name="Sales", rows=100, cols=4),
        SetValues(
            sheet="Sales",
            row=0,
            col=0,
            values=[
                ["Product", "Price", "Quantity", "Total"],
                ["Widget A", 10.00, 5, None],  # Total will be calculated by formula
                ["Widget B", 15.00, 3, None],
                ["Widget C", 20.00, 2, None],
            ],
        ),
        # Add formulas to calculate totals in the Sales sheet
        SetFormula(sheet="Sales", row=1, col=3, formula="=B2*C2"),
        SetFormula(sheet="Sales", row=2, col=3, formula="=B3*C3"),
        SetFormula(sheet="Sales", row=3, col=3, formula="=B4*C4"),
        # Create summary sheet
        CreateSheet(name="Summary", rows=20, cols=2),
        SetValues(
            sheet="Summary",
            row=0,
            col=0,
            values=[
                ["Metric", "Value"],
                ["Total Revenue", None],  # Will be calculated by formula
                ["Average Price", None],
                ["Total Quantity", None],
            ],
        ),
        # Add summary formulas that reference the Sales sheet
        SetFormula(
            sheet="Summary", row=1, col=1, formula="=SUM(Sales!D:D)", ref="Sales"
        ),
        SetFormula(
            sheet="Summary", row=2, col=1, formula="=AVERAGE(Sales!B:B)", ref="Sales"
        ),
        SetFormula(
            sheet="Summary", row=3, col=1, formula="=SUM(Sales!C:C)", ref="Sales"
        ),
        # Create a named range for the sales data
        NamedRange(
            name="SalesData",
            sheet="Sales",
            row_start=1,  # Skip header
            col_start=0,
            row_end=3,
            col_end=3,
        ),
    ]

    print("Step 1: Defined {} spreadsheet operations".format(len(operations)))
    print()

    # Build the execution plan
    print("Step 2: Building execution plan...")
    plan = ExecutionPlan.from_operations(operations, main_sheet="Summary")
    print("✓ Plan created successfully!")
    print()

    # Inspect the plan
    print("Step 3: Inspecting execution plan:")
    print("-" * 70)
    print(plan.explain())
    print("-" * 70)
    print()

    # Show serialization capability
    print("Step 4: Testing serialization...")
    plan_dict = plan.to_dict()
    restored_plan = ExecutionPlan.from_dict(plan_dict)
    print("✓ Plan serialized and restored successfully!")
    print(f"  Original steps: {len(plan.steps)}")
    print(f"  Restored steps: {len(restored_plan.steps)}")
    print()

    # Show step details
    print("Step 5: Detailed step breakdown:")
    for i, step in enumerate(plan.steps, 1):
        step_name = step.step_type.value.replace("_", " ").title()
        print(f"  {i}. {step_name}")
        print(f"     - Operations: {len(step.operations)}")
        print(f"     - Target sheets: {', '.join(sorted(step.target_sheets))}")

        # Show a sample operation from each step
        if step.operations:
            sample_op = step.operations[0]
            op_type = type(sample_op).__name__
            print(f"     - Sample operation: {op_type}")
    print()

    # Authenticate with Google Sheets
    print("Step 6: Authenticating with Google Sheets...")
    gc = _get_gspread_client()
    print()

    client = SheetsClient(gc)

    executor = SheetsExecutor(client)

    # Execute the plan
    title = "Fornero Demo - Sales Report"
    print(f"Step 7: Executing plan → '{title}'...")
    spreadsheet = executor.execute(plan, title)
    print("✓ Spreadsheet created!")
    print("  URL:", spreadsheet.url)
    print()

    print("=" * 70)
    print("Demo complete! Open the URL above to see the result.")
    print("=" * 70)


if __name__ == "__main__":
    main()
