"""Shared pytest configuration and fixtures for fornero tests."""

import pandas as pd
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-slow",
        action="store_true",
        default=False,
        help="Include tests marked @pytest.mark.slow (e.g. live Google Sheets)",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: marks tests as slow (skipped unless --run-slow)")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-slow"):
        return
    skip = pytest.mark.skip(reason="slow test — pass --run-slow to include")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def employees() -> pd.DataFrame:
    return pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Heidi"],
        "age": [30, 45, 28, 35, 50, 33, 29, 40],
        "dept": ["eng", "eng", "sales", "eng", "hr", "sales", "hr", "eng"],
        "salary": [90000, 120000, 65000, 95000, 80000, 70000, 75000, 110000],
    })


@pytest.fixture
def departments() -> pd.DataFrame:
    return pd.DataFrame({
        "dept": ["eng", "sales", "hr", "marketing"],
        "budget": [500000, 300000, 200000, 150000],
    })


@pytest.fixture
def long_format() -> pd.DataFrame:
    return pd.DataFrame({
        "name": ["Alice", "Alice", "Alice", "Bob", "Bob", "Bob"],
        "metric": ["q1", "q2", "q3", "q1", "q2", "q3"],
        "value": [10, 20, 30, 40, 50, 60],
    })


@pytest.fixture
def wide_format() -> pd.DataFrame:
    return pd.DataFrame({
        "name": ["Alice", "Bob"],
        "q1": [10, 40],
        "q2": [20, 50],
        "q3": [30, 60],
    })
