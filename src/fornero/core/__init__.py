"""
Core module for fornero.

This module provides the DataFrame subclass and operation tracing functionality.
"""

from .dataframe import DataFrame, DataFrameGroupBy
from . import tracer

__all__ = ["DataFrame", "DataFrameGroupBy", "tracer"]
