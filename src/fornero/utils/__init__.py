"""
Utility functions for fornero.

This module provides utilities for working with logical plans:
- visualization: Text-based tree rendering of plans
- serialization: JSON serialization/deserialization of plans
"""

from .visualization import visualize
from .serialization import (
    serialize,
    deserialize,
    to_json,
    from_json,
    SERIALIZATION_VERSION
)

__all__ = [
    'visualize',
    'serialize',
    'deserialize',
    'to_json',
    'from_json',
    'SERIALIZATION_VERSION'
]
