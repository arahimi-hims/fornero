"""
Google Sheets API client wrapper.

This module provides a high-level interface to the Google Sheets API via gspread,
with error wrapping for common spreadsheet operations.
"""

from typing import Any, Dict, List

import gspread
from gspread.exceptions import APIError

from fornero.exceptions import SheetsAPIError


class SheetsClient:
    """
    A wrapper around gspread for Google Sheets API operations.

    This client wraps an authenticated gspread client, adding error handling
    and a clean interface for common spreadsheet operations needed by the executor.

    Attributes:
        gc: The authenticated gspread client instance
    """

    def __init__(self, gc: gspread.Client) -> None:
        """
        Initialize the Sheets client with an authenticated gspread client.

        Args:
            gc: An authenticated gspread client, e.g. from ``gspread.service_account()``
                or ``gspread.oauth()``.
        """
        self.gc = gc

    def create_spreadsheet(self, title: str) -> gspread.Spreadsheet:
        """
        Create a new spreadsheet.

        Args:
            title: The title for the new spreadsheet

        Returns:
            The created Spreadsheet object

        Raises:
            SheetsAPIError: If the API call fails
        """
        try:
            return self.gc.create(title)
        except APIError as e:
            raise SheetsAPIError(f"Failed to create spreadsheet '{title}': {e}") from e

    def add_sheet(
        self,
        spreadsheet: gspread.Spreadsheet,
        name: str,
        rows: int = 1000,
        cols: int = 26
    ) -> gspread.Worksheet:
        """
        Add a new worksheet (tab) to an existing spreadsheet.

        Args:
            spreadsheet: The spreadsheet to add the worksheet to
            name: The name for the new worksheet
            rows: Number of rows in the new worksheet (default: 1000)
            cols: Number of columns in the new worksheet (default: 26)

        Returns:
            The created Worksheet object

        Raises:
            SheetsAPIError: If the API call fails
        """
        try:
            return spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
        except APIError as e:
            raise SheetsAPIError(
                f"Failed to add worksheet '{name}' to spreadsheet: {e}"
            ) from e

    def write_values(
        self,
        worksheet: gspread.Worksheet,
        range_name: str,
        values: List[List[Any]]
    ) -> None:
        """
        Write values to a range in a worksheet.

        Args:
            worksheet: The worksheet to write to
            range_name: The A1 notation range (e.g., "A1:C10")
            values: A 2D list of values to write

        Raises:
            SheetsAPIError: If the API call fails
        """
        try:
            worksheet.update(values, range_name=range_name)
        except APIError as e:
            raise SheetsAPIError(
                f"Failed to write values to range '{range_name}': {e}"
            ) from e

    def write_formula(
        self,
        worksheet: gspread.Worksheet,
        cell: str,
        formula: str
    ) -> None:
        """
        Write a formula to a specific cell in a worksheet.

        Args:
            worksheet: The worksheet to write to
            cell: The cell address in A1 notation (e.g., "B2")
            formula: The formula to write (should start with '=')

        Raises:
            SheetsAPIError: If the API call fails
        """
        try:
            worksheet.update([[formula]], range_name=cell, raw=False)
        except APIError as e:
            raise SheetsAPIError(
                f"Failed to write formula to cell '{cell}': {e}"
            ) from e

    def batch_update_values(
        self,
        worksheet: gspread.Worksheet,
        updates: List[Dict[str, Any]]
    ) -> None:
        """
        Batch update multiple value ranges in a worksheet.

        Args:
            worksheet: The worksheet to write to
            updates: List of dictionaries with 'range' and 'values' keys where:
                - range: A1 notation range (e.g., "A1:C10")
                - values: 2D list of values to write

        Raises:
            SheetsAPIError: If the API call fails
        """
        if not updates:
            return

        try:
            worksheet.batch_update(updates)
        except APIError as e:
            raise SheetsAPIError(
                f"Failed to batch update {len(updates)} value ranges: {e}"
            ) from e

    def batch_update_formulas(
        self,
        worksheet: gspread.Worksheet,
        updates: List[Dict[str, Any]]
    ) -> None:
        """
        Batch update multiple formulas in a worksheet.

        Args:
            worksheet: The worksheet to write to
            updates: List of dictionaries with 'range' and 'values' keys where:
                - range: A1 notation cell (e.g., "B2")
                - values: 2D list containing the formula

        Raises:
            SheetsAPIError: If the API call fails
        """
        if not updates:
            return

        try:
            worksheet.batch_update(updates, raw=False)
        except APIError as e:
            raise SheetsAPIError(
                f"Failed to batch update {len(updates)} formulas: {e}"
            ) from e
