"""
Plan serialization utilities.

Provides JSON serialization and deserialization for logical plans. The serialized
format includes versioning for forward compatibility and supports all operation types.
"""

import json
from typing import Dict, Any
from ..algebra.logical_plan import LogicalPlan
from ..algebra.operations import Operation


# Current serialization format version
SERIALIZATION_VERSION = "1.0"


def serialize(plan: LogicalPlan) -> Dict[str, Any]:
    """Serialize a logical plan to a JSON-serializable dictionary.

    The serialized format includes:
    - version: Format version string for forward compatibility
    - root: The root operation serialized as a nested dictionary

    Args:
        plan: The logical plan to serialize

    Returns:
        Dictionary that can be serialized to JSON

    Raises:
        TypeError: If plan is not a LogicalPlan instance

    Example:
        >>> source = Source(source_id="data.csv", schema=["a", "b"])
        >>> plan = LogicalPlan(source)
        >>> data = serialize(plan)
        >>> assert data["version"] == "1.0"
        >>> assert "root" in data
    """
    if not isinstance(plan, LogicalPlan):
        raise TypeError(f"Expected LogicalPlan, got {type(plan)}")

    return {
        "version": SERIALIZATION_VERSION,
        "root": plan.root.to_dict()
    }


def deserialize(data: Dict[str, Any]) -> LogicalPlan:
    """Deserialize a logical plan from a dictionary.

    Args:
        data: Dictionary containing serialized plan data

    Returns:
        Reconstructed LogicalPlan instance

    Raises:
        ValueError: If data is missing required fields or has invalid structure
        TypeError: If data is not a dictionary

    Example:
        >>> data = {"version": "1.0", "root": {...}}
        >>> plan = deserialize(data)
        >>> assert isinstance(plan, LogicalPlan)
    """
    if not isinstance(data, dict):
        raise TypeError(f"Expected dict, got {type(data)}")

    # Validate required fields
    if "version" not in data:
        raise ValueError("Serialized plan must have 'version' field")
    if "root" not in data:
        raise ValueError("Serialized plan must have 'root' field")

    # Check version (for now we only support 1.0)
    version = data["version"]
    if version != SERIALIZATION_VERSION:
        # In a real implementation, we might have migration logic here
        raise ValueError(
            f"Unsupported serialization version: {version}. "
            f"Expected {SERIALIZATION_VERSION}"
        )

    # Deserialize the root operation
    try:
        root = Operation.from_dict(data["root"])
    except ValueError as e:
        raise ValueError(f"Failed to deserialize root operation: {e}") from e
    except KeyError as e:
        raise ValueError(f"Missing required field in operation: {e}") from e

    return LogicalPlan(root)


def to_json(plan: LogicalPlan, **kwargs) -> str:
    """Serialize a logical plan to a JSON string.

    Args:
        plan: The logical plan to serialize
        **kwargs: Additional arguments to pass to json.dumps (e.g., indent=2)

    Returns:
        JSON string representation of the plan

    Example:
        >>> source = Source(source_id="data.csv")
        >>> plan = LogicalPlan(source)
        >>> json_str = to_json(plan, indent=2)
        >>> assert isinstance(json_str, str)
    """
    data = serialize(plan)
    return json.dumps(data, **kwargs)


def from_json(json_str: str) -> LogicalPlan:
    """Deserialize a logical plan from a JSON string.

    Args:
        json_str: JSON string containing serialized plan

    Returns:
        Reconstructed LogicalPlan instance

    Raises:
        ValueError: If JSON is invalid or plan structure is invalid
        json.JSONDecodeError: If JSON string is malformed
    """
    if not isinstance(json_str, str):
        raise TypeError(f"Expected str, got {type(json_str)}")

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}") from e

    return deserialize(data)
