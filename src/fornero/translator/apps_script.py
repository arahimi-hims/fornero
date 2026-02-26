"""
Google Apps Script generation for complex custom functions.

When a lambda or expression is too complex to translate to a formula,
we generate an Apps Script custom function instead.
"""

from typing import Optional
import hashlib


class AppsScriptGenerator:
    """Generates Google Apps Script code for custom functions."""

    def __init__(self):
        """Initialize generator."""
        pass

    def generate_custom_function(self, func_name: str, params: list, body: str,
                                 description: Optional[str] = None) -> str:
        """Generate an Apps Script custom function.

        Args:
            func_name: Function name (must be valid JavaScript identifier)
            params: List of parameter names
            body: Function body (JavaScript code)
            description: Optional function description

        Returns:
            Complete Apps Script function code

        Example:
            >>> gen = AppsScriptGenerator()
            >>> code = gen.generate_custom_function(
            ...     'COMPLEX_CALC',
            ...     ['value', 'threshold'],
            ...     'return value > threshold ? value * 2 : value / 2;',
            ...     'Complex calculation based on threshold'
            ... )
        """
        lines = []

        if description:
            lines.append("/**")
            lines.append(f" * {description}")
            lines.append(" *")
            for param in params:
                lines.append(f" * @param {{{param}}} {param}")
            lines.append(" * @return Computed value")
            lines.append(" * @customfunction")
            lines.append(" */")

        params_str = ", ".join(params)
        lines.append(f"function {func_name}({params_str}) {{")

        # Indent body
        for line in body.split('\n'):
            lines.append(f"  {line}")

        lines.append("}")

        return "\n".join(lines)

    def generate_from_lambda(self, lambda_expr: str, base_name: str = "CUSTOM_FUNC") -> tuple:
        """Generate an Apps Script function from a Python lambda.

        This is a fallback for complex lambdas that can't be translated to formulas.
        The generated code is a stub that needs manual implementation.

        Args:
            lambda_expr: Lambda expression string
            base_name: Base name for the generated function

        Returns:
            (function_name, apps_script_code)

        Example:
            >>> gen = AppsScriptGenerator()
            >>> name, code = gen.generate_from_lambda('lambda x: x.upper()')
        """
        # Generate a deterministic function name based on lambda content
        hash_suffix = hashlib.md5(lambda_expr.encode()).hexdigest()[:8].upper()
        func_name = f"{base_name}_{hash_suffix}"

        # Extract parameters from lambda (simplified)
        import ast
        try:
            tree = ast.parse(lambda_expr, mode='eval')
            if isinstance(tree.body, ast.Lambda):
                params = [arg.arg for arg in tree.body.args.args]
            else:
                params = ['value']
        except:
            params = ['value']

        # Generate placeholder body
        body = (
            "// TODO: Implement custom logic\n"
            f"// Original Python lambda: {lambda_expr}\n"
            "// This function was automatically generated and requires manual implementation.\n"
            "Logger.log('Custom function called with: ' + arguments);\n"
            "return null; // Placeholder return value"
        )

        description = f"Custom function generated from lambda: {lambda_expr}"

        code = self.generate_custom_function(func_name, params, body, description)

        return func_name, code

    def generate_array_function(self, func_name: str, description: str) -> str:
        """Generate an Apps Script function that operates on array inputs.

        Args:
            func_name: Function name
            description: Function description

        Returns:
            Apps Script code for array function

        Example:
            >>> gen = AppsScriptGenerator()
            >>> code = gen.generate_array_function(
            ...     'TRANSFORM_ARRAY',
            ...     'Transform array values'
            ... )
        """
        code = f'''/**
 * {description}
 *
 * @param {{Array<Array>}} input Input array (range)
 * @return {{Array<Array>}} Transformed array
 * @customfunction
 */
function {func_name}(input) {{
  if (!Array.isArray(input)) {{
    return "Error: Input must be a range";
  }}

  // Process each row
  return input.map(function(row) {{
    // Process each cell in the row
    return row.map(function(cell) {{
      // TODO: Implement transformation logic
      return cell;
    }});
  }});
}}
'''
        return code

    def generate_deployment_script(self, functions: list) -> str:
        """Generate a complete Apps Script file with multiple functions.

        Args:
            functions: List of (func_name, func_code) tuples

        Returns:
            Complete Apps Script code for deployment

        Example:
            >>> gen = AppsScriptGenerator()
            >>> code = gen.generate_deployment_script([
            ...     ('FUNC1', 'function FUNC1() { return 1; }'),
            ...     ('FUNC2', 'function FUNC2() { return 2; }')
            ... ])
        """
        lines = []

        lines.append("// Google Apps Script - Auto-generated by Fornero")
        lines.append("// Generated custom functions for spreadsheet operations")
        lines.append("")
        lines.append("// This file contains custom functions that implement complex")
        lines.append("// logic that cannot be expressed as standard spreadsheet formulas.")
        lines.append("")

        for func_name, func_code in functions:
            lines.append("")
            lines.append("// " + "=" * 70)
            lines.append(f"// {func_name}")
            lines.append("// " + "=" * 70)
            lines.append("")
            lines.append(func_code)

        lines.append("")
        lines.append("// End of auto-generated functions")

        return "\n".join(lines)


def generate_apps_script_function(lambda_expr: str, base_name: str = "CUSTOM_FUNC") -> tuple:
    """Convenience function to generate Apps Script from lambda.

    Args:
        lambda_expr: Lambda expression string
        base_name: Base name for function

    Returns:
        (function_name, apps_script_code)
    """
    generator = AppsScriptGenerator()
    return generator.generate_from_lambda(lambda_expr, base_name)
