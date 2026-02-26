"""
Lambda function analyzer for translating simple Python lambdas to spreadsheet formulas.

Supports:
- Simple arithmetic: lambda x: x * 2
- Column references: lambda row: row['a'] + row['b']
- Nested expressions: lambda x: (x * 2) + 3

Does not support:
- Method calls: lambda x: x.upper()
- Complex control flow
- Multiple statements
"""

import ast
from typing import Dict
from fornero.exceptions import UnsupportedOperationError


class LambdaAnalyzer:
    """Analyzes lambda functions to determine if they can be translated to formulas."""

    def __init__(self):
        """Initialize analyzer."""
        pass

    def analyze(self, lambda_expr: str) -> Dict[str, any]:
        """Analyze a lambda expression.

        Args:
            lambda_expr: Lambda expression string (e.g., "lambda x: x * 2")

        Returns:
            Dictionary with:
            - 'translatable': bool
            - 'formula_template': str (if translatable)
            - 'parameters': list of parameter names
            - 'error': str (if not translatable)

        Raises:
            UnsupportedOperationError: If lambda cannot be translated
        """
        try:
            # Parse the lambda
            tree = ast.parse(lambda_expr, mode='eval')

            if not isinstance(tree.body, ast.Lambda):
                return {
                    'translatable': False,
                    'error': 'Expression is not a lambda function'
                }

            lambda_node = tree.body

            # Extract parameters
            params = [arg.arg for arg in lambda_node.args.args]

            # Analyze body
            formula_template, refs = self._analyze_expression(lambda_node.body)

            return {
                'translatable': True,
                'formula_template': formula_template,
                'parameters': params,
                'column_refs': refs
            }

        except SyntaxError as e:
            return {
                'translatable': False,
                'error': f'Syntax error: {e}'
            }
        except UnsupportedOperationError as e:
            return {
                'translatable': False,
                'error': str(e)
            }

    def translate_to_formula(self, lambda_expr: str, col_mapping: Dict[str, str]) -> str:
        """Translate a lambda expression to a spreadsheet formula.

        Args:
            lambda_expr: Lambda expression string
            col_mapping: Mapping from parameter/column names to cell references
                        (e.g., {'x': 'A2', 'a': 'A2', 'b': 'B2'})

        Returns:
            Formula string (with leading =)

        Raises:
            UnsupportedOperationError: If lambda cannot be translated
        """
        analysis = self.analyze(lambda_expr)

        if not analysis['translatable']:
            raise UnsupportedOperationError(
                f"Lambda function cannot be translated: {analysis.get('error', 'unknown reason')}"
            )

        formula_template = analysis['formula_template']
        column_refs = analysis['column_refs']

        # Replace placeholders with cell references
        formula = formula_template
        for ref_name in column_refs:
            if ref_name in col_mapping:
                cell_ref = col_mapping[ref_name]
                # Replace {{name}} with cell reference
                formula = formula.replace(f"{{{{{ref_name}}}}}", cell_ref)
            else:
                raise ValueError(f"No cell reference mapping for column '{ref_name}'")

        return f"={formula}"

    def _analyze_expression(self, node: ast.AST) -> tuple:
        """Recursively analyze an AST expression node.

        Args:
            node: AST node

        Returns:
            (formula_template, column_refs)
            formula_template has placeholders like {{x}}
            column_refs is a set of referenced names

        Raises:
            UnsupportedOperationError: If expression cannot be translated
        """
        if isinstance(node, ast.Constant):
            # Literal value
            if isinstance(node.value, str):
                return f'"{node.value}"', set()
            else:
                return str(node.value), set()

        elif isinstance(node, ast.Name):
            # Variable reference (parameter or column name)
            return f"{{{{{node.id}}}}}", {node.id}

        elif isinstance(node, ast.BinOp):
            # Binary operation: +, -, *, /, etc.
            left_formula, left_refs = self._analyze_expression(node.left)
            right_formula, right_refs = self._analyze_expression(node.right)

            op_map = {
                ast.Add: '+',
                ast.Sub: '-',
                ast.Mult: '*',
                ast.Div: '/',
                ast.Pow: '^',
                ast.Mod: 'MOD',  # Special case
            }

            op_type = type(node.op)
            if op_type in op_map:
                op_str = op_map[op_type]
                if op_type == ast.Mod:
                    # MOD is a function in spreadsheets
                    formula = f"MOD({left_formula}, {right_formula})"
                else:
                    formula = f"({left_formula} {op_str} {right_formula})"

                return formula, left_refs | right_refs
            else:
                raise UnsupportedOperationError(f"Binary operator {op_type.__name__} not supported")

        elif isinstance(node, ast.UnaryOp):
            # Unary operation: -, +, not
            operand_formula, operand_refs = self._analyze_expression(node.operand)

            if isinstance(node.op, ast.USub):
                return f"(-{operand_formula})", operand_refs
            elif isinstance(node.op, ast.UAdd):
                return operand_formula, operand_refs
            else:
                raise UnsupportedOperationError(f"Unary operator {type(node.op).__name__} not supported")

        elif isinstance(node, ast.Compare):
            # Comparison: <, >, ==, etc.
            left_formula, left_refs = self._analyze_expression(node.left)

            if len(node.ops) != 1 or len(node.comparators) != 1:
                raise UnsupportedOperationError("Chained comparisons not supported")

            right_formula, right_refs = self._analyze_expression(node.comparators[0])

            op_map = {
                ast.Eq: '=',
                ast.NotEq: '<>',
                ast.Lt: '<',
                ast.LtE: '<=',
                ast.Gt: '>',
                ast.GtE: '>=',
            }

            op_type = type(node.ops[0])
            if op_type in op_map:
                op_str = op_map[op_type]
                formula = f"({left_formula} {op_str} {right_formula})"
                return formula, left_refs | right_refs
            else:
                raise UnsupportedOperationError(f"Comparison operator {op_type.__name__} not supported")

        elif isinstance(node, ast.Subscript):
            # Subscript: row['column']
            if isinstance(node.value, ast.Name) and isinstance(node.slice, ast.Constant):
                # row['column'] -> treat as column reference
                var_name = node.value.id
                col_name = node.slice.value
                # Use column name as reference
                return f"{{{{{col_name}}}}}", {col_name}
            else:
                raise UnsupportedOperationError("Complex subscript expressions not supported")

        elif isinstance(node, ast.Call):
            # Function call
            if isinstance(node.func, ast.Name):
                func_name = node.func.id.upper()

                # Map Python functions to spreadsheet functions
                func_map = {
                    'ABS': 'ABS',
                    'MIN': 'MIN',
                    'MAX': 'MAX',
                    'ROUND': 'ROUND',
                    'SQRT': 'SQRT',
                    'LEN': 'LEN',
                }

                if func_name in func_map:
                    sheets_func = func_map[func_name]
                    arg_formulas = []
                    all_refs = set()

                    for arg in node.args:
                        arg_formula, arg_refs = self._analyze_expression(arg)
                        arg_formulas.append(arg_formula)
                        all_refs |= arg_refs

                    formula = f"{sheets_func}({', '.join(arg_formulas)})"
                    return formula, all_refs
                else:
                    raise UnsupportedOperationError(f"Function '{func_name}' not supported")
            else:
                raise UnsupportedOperationError("Method calls not supported")

        elif isinstance(node, ast.Attribute):
            # Attribute access: x.upper()
            raise UnsupportedOperationError("Attribute access and method calls not supported")

        else:
            raise UnsupportedOperationError(f"AST node type {type(node).__name__} not supported")


def translate_lambda(lambda_expr: str, col_mapping: Dict[str, str]) -> str:
    """Convenience function to translate a lambda to a formula.

    Args:
        lambda_expr: Lambda expression string
        col_mapping: Column to cell reference mapping

    Returns:
        Formula string (with leading =)

    Raises:
        UnsupportedOperationError: If lambda cannot be translated
    """
    analyzer = LambdaAnalyzer()
    return analyzer.translate_to_formula(lambda_expr, col_mapping)
