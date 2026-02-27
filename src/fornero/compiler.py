"""
End-to-end compilation utilities.

Each function runs the full compiler pipeline — translate (with optimization),
build execution plan, execute — against a specific backend.  The optimizer is
always enabled by default in the translator.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from fornero.algebra.logical_plan import LogicalPlan
from fornero.executor.base import Executor
from fornero.executor.plan import ExecutionPlan
from fornero.translator.converter import Translator


def _run_pipeline(
    plan: LogicalPlan,
    source_data: Dict[str, Any],
    executor: Executor,
    title: str,
) -> Any:
    """Translate (with optimization) and execute a logical plan.

    This is the shared core that every public entry-point delegates to.
    The translator automatically optimizes the plan before translation.

    Args:
        plan: Logical plan produced by a fornero DataFrame.
        source_data: Mapping of source_id to row data (list of lists).
        executor: Backend that will materialise the spreadsheet.
        title: Title for the created spreadsheet.

    Returns:
        Whatever the executor returns (e.g. a ``gspread.Spreadsheet`` for
        ``SheetsExecutor``, ``None`` for ``LocalExecutor``).
    """
    ops = Translator().translate(plan, source_data=source_data)
    execution_plan = ExecutionPlan.from_operations(ops)
    return executor.execute(execution_plan, title)


def compile(
    plan: LogicalPlan,
    source_data: Dict[str, Any],
    executor: Executor,
    title: str,
) -> Any:
    """Run all compiler passes and execute against an arbitrary backend.

    Args:
        plan: Logical plan produced by a fornero DataFrame.
        source_data: Mapping of source_id to row data (list of lists).
        executor: Any object satisfying the ``Executor`` protocol.
        title: Title for the created spreadsheet.

    Returns:
        The result of ``executor.execute`` — type depends on the backend.
    """
    return _run_pipeline(plan, source_data, executor, title)


def compile_to_sheets(
    plan: LogicalPlan,
    source_data: Dict[str, Any],
    title: str,
    gc: Any,
) -> Any:
    """Compile a plan and materialise it as a Google Sheet.

    Args:
        plan: Logical plan produced by a fornero DataFrame.
        source_data: Mapping of source_id to row data (list of lists).
        title: Title for the new Google Sheet.
        gc: An authenticated ``gspread.Client`` (from
            ``gspread.service_account()`` or ``gspread.oauth()``).

    Returns:
        A ``gspread.Spreadsheet`` object for the newly created sheet.
    """
    from fornero.executor.sheets_client import SheetsClient
    from fornero.executor.sheets_executor import SheetsExecutor

    executor = SheetsExecutor(SheetsClient(gc))
    return _run_pipeline(plan, source_data, executor, title)


def compile_locally(
    plan: LogicalPlan,
    source_data: Dict[str, Any],
    title: str = "local",
) -> "LocalExecutor":
    """Compile a plan and evaluate it in-process (no network required).

    Args:
        plan: Logical plan produced by a fornero DataFrame.
        source_data: Mapping of source_id to row data (list of lists).
        title: Title for the spreadsheet (only used for protocol
            compliance; defaults to ``"local"``).

    Returns:
        The ``LocalExecutor`` instance.  Call ``executor.read_sheet(name)``
        to inspect the evaluated data.
    """
    from fornero.executor.local_executor import LocalExecutor

    executor = LocalExecutor()
    _run_pipeline(plan, source_data, executor, title)
    return executor
