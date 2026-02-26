"""
Abstract executor interface for spreadsheet plan backends.

The Executor protocol defines the contract that every backend must satisfy:
execute an execution plan and return the resulting spreadsheet.
Concrete implementations include SheetsExecutor (Google Sheets API)
and LocalExecutor (in-process evaluation via formualizer).
"""

from typing import Any, Protocol

# Import for type hints only - avoiding circular import
if False:
    from fornero.executor.plan import ExecutionPlan


class Executor(Protocol):
    """Protocol for spreadsheet plan execution backends.

    An executor consumes an execution plan (containing ordered operations)
    and materializes it in some spreadsheet engine.
    """

    def execute(self, plan: "ExecutionPlan", title: str) -> Any:
        """Execute an execution plan, creating a new spreadsheet.

        Args:
            plan: The execution plan containing ordered operations with
                  dependency resolution and batching information
            title: Title for the new spreadsheet

        Returns:
            The created spreadsheet object. For SheetsExecutor this is
            a gspread.Spreadsheet. For LocalExecutor this is None (the
            spreadsheet is maintained internally and accessed via read_sheet).
        """
        ...
