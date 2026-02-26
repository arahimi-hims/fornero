"""
Exception classes for fornero.

These exceptions are used throughout the fornero package to signal various error conditions
during plan construction, translation, and execution.
"""


class UnsupportedOperationError(Exception):
    """Raised when operation cannot be translated to spreadsheet.

    This error is raised when the translator encounters a dataframe operation
    that cannot be expressed as spreadsheet formulas or when a feature is not
    yet implemented.

    Examples:
        - Custom Python functions that don't map to spreadsheet formulas
        - Complex aggregations not supported by Google Sheets QUERY function
        - Operations that require data inspection (violates static analysis constraint)
    """
    pass


class SheetsAPIError(Exception):
    """Raised when Google Sheets API call fails.

    This error wraps exceptions from the Google Sheets API (via gspread) and provides
    context about which operation failed. Common causes include:
        - Authentication failures
        - Rate limiting (HTTP 429)
        - Network connectivity issues
        - Invalid spreadsheet IDs or permissions errors
        - API quota exceeded
    """
    pass


class PlanValidationError(Exception):
    """Raised when plan is invalid.

    This error is raised when a logical plan or execution plan contains structural
    inconsistencies or violates invariants. Examples:
        - Formula referencing a non-existent sheet
        - Circular dependencies between formulas
        - Duplicate sheet names
        - Invalid range references
        - Operation with missing required inputs
    """
    pass
