"""
Translator module for converting dataframe algebra to spreadsheet operations.

This module provides:
- Translator: Main class for translating logical plans to spreadsheet operations
- Optimizer: Optimization passes for logical plans
- LambdaAnalyzer: Analyzing and translating lambda functions to formulas
- AppsScriptGenerator: Generating Google Apps Script for complex functions
"""

from fornero.translator.converter import Translator
from fornero.translator.optimizer import Optimizer, optimize_plan
from fornero.translator.lambda_analyzer import LambdaAnalyzer, translate_lambda
from fornero.translator.apps_script import AppsScriptGenerator, generate_apps_script_function

__all__ = [
    'Translator',
    'Optimizer',
    'optimize_plan',
    'LambdaAnalyzer',
    'translate_lambda',
    'AppsScriptGenerator',
    'generate_apps_script_function',
]
