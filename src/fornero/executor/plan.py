"""
Execution plan for spreadsheet operations.

This module provides the ExecutionPlan class, which organizes a flat list of
spreadsheet algebra operations into ordered, batchable steps that respect
dependency constraints.

The plan ensures:
- Sheets are created before data is written
- Source data lands before formulas that reference it
- Formulas are written in topological order (dependencies first)
- Named ranges are registered after all formulas
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from fornero.exceptions import PlanValidationError
from fornero.spreadsheet.operations import (
    CreateSheet,
    SetValues,
    SetFormula,
    NamedRange,
    SpreadsheetOp,
    op_from_dict,
)


class StepType(Enum):
    """Type of execution step, defining the fixed execution order."""
    CREATE_SHEETS = "create_sheets"
    WRITE_SOURCE_DATA = "write_source_data"
    WRITE_FORMULAS = "write_formulas"
    REGISTER_NAMED_RANGES = "register_named_ranges"


@dataclass
class ExecutionStep:
    """A batch of operations that can be executed together.

    Attributes:
        step_type: The type of step (determines execution order)
        operations: List of operations to execute in this step
        target_sheets: Set of sheet names involved in this step
    """
    step_type: StepType
    operations: List[SpreadsheetOp]
    target_sheets: Set[str]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "step_type": self.step_type.value,
            "operations": [op.to_dict() for op in self.operations],
            "target_sheets": list(self.target_sheets),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutionStep":
        """Create from dictionary representation."""
        return cls(
            step_type=StepType(data["step_type"]),
            operations=[op_from_dict(op_data) for op_data in data["operations"]],
            target_sheets=set(data["target_sheets"]),
        )


class ExecutionPlan:
    """Organizes spreadsheet operations into an ordered execution plan.

    The plan groups operations by type and ensures correct dependency ordering:
    1. CreateSheets - all sheets created first
    2. WriteSourceData - static values written to sheets
    3. WriteFormulas - formulas written in topological order
    4. RegisterNamedRanges - named ranges registered last

    Attributes:
        steps: Ordered list of execution steps
        main_sheet: Name of the sheet containing the final output
    """

    def __init__(self, steps: List[ExecutionStep], main_sheet: Optional[str] = None):
        """Initialize an execution plan.

        Args:
            steps: Ordered list of execution steps
            main_sheet: Name of the main output sheet (if known)
        """
        self.steps = steps
        self.main_sheet = main_sheet

    @classmethod
    def from_operations(
        cls,
        ops: List[SpreadsheetOp],
        main_sheet: Optional[str] = None
    ) -> "ExecutionPlan":
        """Construct an execution plan from a flat list of operations.

        This method:
        1. Validates the operation list (no duplicate sheet names, valid references)
        2. Partitions operations by type
        3. Topologically sorts formulas by their dependencies
        4. Assembles the execution steps in order

        Args:
            ops: List of spreadsheet operations
            main_sheet: Optional name of the main output sheet

        Returns:
            ExecutionPlan ready for execution

        Raises:
            PlanValidationError: If the operation list is invalid
        """
        if not ops:
            # Empty operation list produces an empty plan
            return cls(steps=[], main_sheet=main_sheet)

        # Partition operations by type
        create_ops: List[CreateSheet] = []
        value_ops: List[SetValues] = []
        formula_ops: List[SetFormula] = []
        named_range_ops: List[NamedRange] = []

        for op in ops:
            if isinstance(op, CreateSheet):
                create_ops.append(op)
            elif isinstance(op, SetValues):
                value_ops.append(op)
            elif isinstance(op, SetFormula):
                formula_ops.append(op)
            elif isinstance(op, NamedRange):
                named_range_ops.append(op)

        # Validate: check for duplicate sheet names
        sheet_names = {op.name for op in create_ops}
        if len(sheet_names) != len(create_ops):
            # Find duplicates
            seen = set()
            duplicates = set()
            for op in create_ops:
                if op.name in seen:
                    duplicates.add(op.name)
                seen.add(op.name)
            raise PlanValidationError(
                f"Duplicate sheet names in CreateSheet operations: {duplicates}"
            )

        # Validate: check that all referenced sheets exist
        for op in value_ops:
            if op.sheet not in sheet_names:
                raise PlanValidationError(
                    f"SetValues references non-existent sheet: {op.sheet}"
                )

        for op in formula_ops:
            if op.sheet not in sheet_names:
                raise PlanValidationError(
                    f"SetFormula references non-existent sheet: {op.sheet}"
                )
            # If formula has a cross-sheet reference, validate it
            if op.ref and op.ref not in sheet_names:
                raise PlanValidationError(
                    f"SetFormula on sheet '{op.sheet}' references non-existent sheet: {op.ref}"
                )

        for op in named_range_ops:
            if op.sheet not in sheet_names:
                raise PlanValidationError(
                    f"NamedRange references non-existent sheet: {op.sheet}"
                )

        # Build execution steps
        steps: List[ExecutionStep] = []

        # Step 1: Create all sheets
        if create_ops:
            steps.append(ExecutionStep(
                step_type=StepType.CREATE_SHEETS,
                operations=create_ops,
                target_sheets={op.name for op in create_ops},
            ))

        # Step 2: Write source data (SetValues operations)
        # Group by sheet for batching
        if value_ops:
            sheets_with_values = {op.sheet for op in value_ops}
            steps.append(ExecutionStep(
                step_type=StepType.WRITE_SOURCE_DATA,
                operations=value_ops,
                target_sheets=sheets_with_values,
            ))

        # Step 3: Write formulas in topological order
        # We need to ensure that if a formula on sheet B references sheet A,
        # then A's data is written before B's formulas
        if formula_ops:
            # Sort formulas by their dependencies
            sorted_formulas = _topological_sort_formulas(formula_ops, sheet_names)
            sheets_with_formulas = {op.sheet for op in sorted_formulas}
            steps.append(ExecutionStep(
                step_type=StepType.WRITE_FORMULAS,
                operations=sorted_formulas,
                target_sheets=sheets_with_formulas,
            ))

        # Step 4: Register named ranges
        if named_range_ops:
            sheets_with_ranges = {op.sheet for op in named_range_ops}
            steps.append(ExecutionStep(
                step_type=StepType.REGISTER_NAMED_RANGES,
                operations=named_range_ops,
                target_sheets=sheets_with_ranges,
            ))

        return cls(steps=steps, main_sheet=main_sheet)

    def explain(self) -> str:
        """Generate a human-readable summary of the execution plan.

        Returns:
            Multi-line string describing the plan structure
        """
        if not self.steps:
            return "Empty execution plan (no operations)"

        lines = ["Execution Plan Summary", "=" * 50]

        # Count sheets, formulas, operations
        num_sheets = 0
        num_formulas = 0
        num_values = 0
        num_named_ranges = 0

        for step in self.steps:
            if step.step_type == StepType.CREATE_SHEETS:
                num_sheets = len(step.operations)
            elif step.step_type == StepType.WRITE_SOURCE_DATA:
                num_values = len(step.operations)
            elif step.step_type == StepType.WRITE_FORMULAS:
                num_formulas = len(step.operations)
            elif step.step_type == StepType.REGISTER_NAMED_RANGES:
                num_named_ranges = len(step.operations)

        lines.append(f"Sheets: {num_sheets}")
        lines.append(f"Source data operations: {num_values}")
        lines.append(f"Formula operations: {num_formulas}")
        lines.append(f"Named ranges: {num_named_ranges}")
        lines.append(f"Total execution steps: {len(self.steps)}")

        if self.main_sheet:
            lines.append(f"Main output sheet: {self.main_sheet}")

        lines.append("")
        lines.append("Execution Steps:")
        lines.append("-" * 50)

        for i, step in enumerate(self.steps, 1):
            step_name = step.step_type.value.replace("_", " ").title()
            lines.append(f"{i}. {step_name}")
            lines.append(f"   Operations: {len(step.operations)}")
            lines.append(f"   Target sheets: {', '.join(sorted(step.target_sheets))}")

        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the plan to a JSON-compatible dictionary.

        Returns:
            Dictionary representation suitable for JSON serialization
        """
        return {
            "steps": [step.to_dict() for step in self.steps],
            "main_sheet": self.main_sheet,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutionPlan":
        """Deserialize a plan from dictionary representation.

        Args:
            data: Dictionary produced by to_dict()

        Returns:
            Reconstructed ExecutionPlan
        """
        steps = [ExecutionStep.from_dict(step_data) for step_data in data["steps"]]
        return cls(steps=steps, main_sheet=data.get("main_sheet"))

    def __eq__(self, other: object) -> bool:
        """Check equality with another ExecutionPlan."""
        if not isinstance(other, ExecutionPlan):
            return NotImplemented
        return (
            len(self.steps) == len(other.steps)
            and all(
                s1.step_type == s2.step_type
                and s1.operations == s2.operations
                and s1.target_sheets == s2.target_sheets
                for s1, s2 in zip(self.steps, other.steps)
            )
            and self.main_sheet == other.main_sheet
        )


def _topological_sort_formulas(
    formula_ops: List[SetFormula],
    available_sheets: Set[str]
) -> List[SetFormula]:
    """Sort formulas in topological order based on cross-sheet dependencies.

    If a formula on sheet B references sheet A, then formulas on sheet A
    should be written before formulas on sheet B.

    Args:
        formula_ops: List of SetFormula operations
        available_sheets: Set of all available sheet names

    Returns:
        Sorted list of formulas respecting dependencies
    """
    # Build dependency graph: sheet -> set of sheets it depends on
    dependencies: Dict[str, Set[str]] = {}
    formulas_by_sheet: Dict[str, List[SetFormula]] = {}

    for op in formula_ops:
        if op.sheet not in formulas_by_sheet:
            formulas_by_sheet[op.sheet] = []
            dependencies[op.sheet] = set()

        formulas_by_sheet[op.sheet].append(op)

        # Track cross-sheet dependency
        if op.ref and op.ref != op.sheet:
            dependencies[op.sheet].add(op.ref)

    # Topological sort using Kahn's algorithm
    # Compute in-degree for each sheet (number of dependencies)
    in_degree: Dict[str, int] = {sheet: 0 for sheet in formulas_by_sheet}
    for sheet, deps in dependencies.items():
        if sheet in in_degree:
            in_degree[sheet] = len(deps)

    # Start with sheets that have no dependencies
    queue: List[str] = [sheet for sheet, degree in in_degree.items() if degree == 0]
    sorted_sheets: List[str] = []

    while queue:
        # Sort for deterministic ordering
        queue.sort()
        sheet = queue.pop(0)
        sorted_sheets.append(sheet)

        # Reduce in-degree for dependent sheets
        for other_sheet, deps in dependencies.items():
            if sheet in deps:
                in_degree[other_sheet] -= 1
                if in_degree[other_sheet] == 0:
                    queue.append(other_sheet)

    # Collect formulas in sorted order
    result: List[SetFormula] = []
    for sheet in sorted_sheets:
        result.extend(formulas_by_sheet[sheet])

    return result
