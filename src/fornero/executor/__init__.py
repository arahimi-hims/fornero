"""
Executor module for fornero.

This module provides executor backends for materialising spreadsheet plans.
``SheetsExecutor`` targets the Google Sheets API; ``LocalExecutor`` evaluates
formulas in-process via formualizer (no network required).
"""

from fornero.executor.base import Executor
from fornero.executor.local_executor import LocalExecutor
from fornero.executor.plan import ExecutionPlan, ExecutionStep, StepType
from fornero.executor.sheets_client import SheetsClient
from fornero.executor.sheets_executor import SheetsExecutor

__all__ = [
    "Executor",
    "LocalExecutor",
    "SheetsClient",
    "SheetsExecutor",
    "ExecutionPlan",
    "ExecutionStep",
    "StepType",
]
