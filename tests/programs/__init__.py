"""
End-to-end test program corpus.

Each sub-module exposes:
    PROGRAM_NAME  – human-readable label
    OPERATIONS    – list of algebra operation names exercised
    run()         – returns a ProgramResult(result, source_data)

``discover()`` collects every program module in this package so the
parametrized test runner can iterate over them.
"""

from collections import namedtuple
from typing import List
import importlib
import pkgutil

ProgramResult = namedtuple("ProgramResult", ["result", "source_data"])


def discover() -> List:
    """Return a list of (module_name, module) pairs for every p##_*.py file."""
    programs = []
    package = __name__
    pkg_path = __path__

    for importer, modname, ispkg in pkgutil.iter_modules(pkg_path):
        if modname.startswith("p") and not ispkg:
            mod = importlib.import_module(f"{package}.{modname}")
            programs.append((modname, mod))

    programs.sort(key=lambda t: t[0])
    return programs
