"""
Spreadsheet algebra model classes.

This module provides abstractions for working with spreadsheet concepts:
- Sheet: A single spreadsheet tab with dimensions
- Range: A rectangular cell region (e.g., A2:C100)
- Formula: A cell formula expression (e.g., =FILTER(...))
- Value: Static cell content
- Reference: Cell/range reference for use in formulas
"""

import re
from typing import Optional, Union


class Sheet:
    """Represents a single spreadsheet tab with name and dimensions.

    Attributes:
        name: The sheet name (must be non-empty)
        rows: Number of rows (must be positive)
        cols: Number of columns (must be positive)
    """

    def __init__(self, name: str, rows: int, cols: int) -> None:
        """Initialize a Sheet.

        Args:
            name: Sheet name (non-empty string)
            rows: Number of rows (positive integer)
            cols: Number of columns (positive integer)

        Raises:
            ValueError: If name is empty or dimensions are not positive
        """
        if not name or not isinstance(name, str):
            raise ValueError("Sheet name must be a non-empty string")
        if rows <= 0 or cols <= 0:
            raise ValueError("Sheet dimensions must be positive integers")

        self.name = name
        self.rows = rows
        self.cols = cols

    def __repr__(self) -> str:
        return f"Sheet(name={self.name!r}, rows={self.rows}, cols={self.cols})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Sheet):
            return NotImplemented
        return self.name == other.name and self.rows == other.rows and self.cols == other.cols


class Range:
    """Represents a rectangular cell region in A1 notation.

    IMPORTANT: Range uses 0-indexed coordinates internally (Python convention),
    but converts to 1-indexed A1 notation for spreadsheet APIs via to_a1().

    A Range can be:
    - A single cell: A1 corresponds to (row=0, col=0, row_end=0, col_end=0)
    - A cell range: A1:B10 corresponds to (row=0, col=0, row_end=9, col_end=1)

    Attributes:
        row: Starting row (0-indexed, internal representation)
        col: Starting column (0-indexed, internal representation)
        row_end: Ending row (0-indexed, inclusive, internal representation)
        col_end: Ending column (0-indexed, inclusive, internal representation)
    """

    def __init__(
        self,
        row: int,
        col: int,
        row_end: Optional[int] = None,
        col_end: Optional[int] = None
    ) -> None:
        """Initialize a Range with 0-indexed coordinates.

        Args:
            row: Starting row (0-indexed, non-negative)
            col: Starting column (0-indexed, non-negative)
            row_end: Ending row (0-indexed, defaults to row for single cell)
            col_end: Ending column (0-indexed, defaults to col for single cell)

        Raises:
            ValueError: If coordinates are invalid
        """
        if row < 0 or col < 0:
            raise ValueError("Row and column must be non-negative (0-indexed)")

        self.row = row
        self.col = col
        self.row_end = row_end if row_end is not None else row
        self.col_end = col_end if col_end is not None else col

        if self.row_end < self.row or self.col_end < self.col:
            raise ValueError("End coordinates must be >= start coordinates")

    @staticmethod
    def _col_to_letter(col: int) -> str:
        """Convert column number (0-indexed internal) to letter(s) for A1 notation.

        Args:
            col: Column number (0-indexed: 0 = A, 25 = Z, 26 = AA, etc.)

        Returns:
            Column letter(s) in A1 notation
        """
        # Convert 0-indexed to 1-indexed for A1 notation
        col_1indexed = col + 1
        result = ""
        while col_1indexed > 0:
            col_1indexed -= 1
            result = chr(65 + (col_1indexed % 26)) + result
            col_1indexed //= 26
        return result

    @staticmethod
    def _letter_to_col(letters: str) -> int:
        """Convert column letter(s) to number (0-indexed internal).

        Args:
            letters: Column letter(s) in A1 notation (A, Z, AA, etc.)

        Returns:
            Column number (0-indexed: A = 0, Z = 25, AA = 26, etc.)
        """
        col_1indexed = 0
        for char in letters.upper():
            col_1indexed = col_1indexed * 26 + (ord(char) - 64)
        # Convert from 1-indexed to 0-indexed
        return col_1indexed - 1

    @classmethod
    def from_a1(cls, notation: str) -> "Range":
        """Parse A1 notation string to create a Range with 0-indexed coordinates.

        Supports:
        - Single cell: A1, ZZ100
        - Cell range: A1:B10, A1:ZZ100

        Args:
            notation: A1 notation string (1-indexed spreadsheet convention)

        Returns:
            Range object with 0-indexed internal coordinates

        Raises:
            ValueError: If notation is invalid
        """
        notation = notation.strip()
        if not notation:
            raise ValueError("Empty range notation")

        # Check if it's a range (contains :)
        if ":" in notation:
            parts = notation.split(":")
            if len(parts) != 2:
                raise ValueError(f"Invalid range notation: {notation}")

            start_cell, end_cell = parts
            start_match = re.match(r"^([A-Z]+)(\d+)$", start_cell.strip().upper())
            end_match = re.match(r"^([A-Z]+)(\d+)$", end_cell.strip().upper())

            if not start_match or not end_match:
                raise ValueError(f"Invalid range notation: {notation}")

            start_col_letter, start_row_str = start_match.groups()
            end_col_letter, end_row_str = end_match.groups()

            # Parse 1-indexed A1 notation and convert to 0-indexed internal
            row_1indexed = int(start_row_str)
            col = cls._letter_to_col(start_col_letter)
            row_end_1indexed = int(end_row_str)
            col_end = cls._letter_to_col(end_col_letter)

            # Convert rows from 1-indexed to 0-indexed
            row = row_1indexed - 1
            row_end = row_end_1indexed - 1

            return cls(row=row, col=col, row_end=row_end, col_end=col_end)
        else:
            # Single cell
            match = re.match(r"^([A-Z]+)(\d+)$", notation.upper())
            if not match:
                raise ValueError(f"Invalid cell notation: {notation}")

            col_letter, row_str = match.groups()
            row_1indexed = int(row_str)
            col = cls._letter_to_col(col_letter)

            # Convert row from 1-indexed to 0-indexed
            row = row_1indexed - 1

            return cls(row=row, col=col)

    def to_a1(self) -> str:
        """Convert Range to A1 notation string (1-indexed for spreadsheet APIs).

        Converts internal 0-indexed coordinates to 1-indexed A1 notation.

        Returns:
            A1 notation string (e.g., "A1" or "A1:B10")
        """
        # Convert 0-indexed internal to 1-indexed A1 notation
        start_cell = f"{self._col_to_letter(self.col)}{self.row + 1}"

        # If it's a single cell
        if self.row == self.row_end and self.col == self.col_end:
            return start_cell

        # Otherwise it's a range
        end_cell = f"{self._col_to_letter(self.col_end)}{self.row_end + 1}"
        return f"{start_cell}:{end_cell}"

    def intersect(self, other: "Range") -> Optional["Range"]:
        """Compute the intersection of two ranges.

        Args:
            other: Another Range

        Returns:
            New Range representing the intersection, or None if no overlap
        """
        row_start = max(self.row, other.row)
        col_start = max(self.col, other.col)
        row_end = min(self.row_end, other.row_end)
        col_end = min(self.col_end, other.col_end)

        if row_start > row_end or col_start > col_end:
            return None

        return Range(row=row_start, col=col_start, row_end=row_end, col_end=col_end)

    def union(self, other: "Range") -> "Range":
        """Compute the bounding box of two ranges.

        Args:
            other: Another Range

        Returns:
            New Range representing the smallest range containing both ranges
        """
        row_start = min(self.row, other.row)
        col_start = min(self.col, other.col)
        row_end = max(self.row_end, other.row_end)
        col_end = max(self.col_end, other.col_end)

        return Range(row=row_start, col=col_start, row_end=row_end, col_end=col_end)

    def offset(self, row_offset: int = 0, col_offset: int = 0) -> "Range":
        """Create a new Range offset by the given amounts.

        Args:
            row_offset: Number of rows to offset (can be negative)
            col_offset: Number of columns to offset (can be negative)

        Returns:
            New Range with offset coordinates

        Raises:
            ValueError: If offset would result in invalid coordinates (< 0)
        """
        new_row = self.row + row_offset
        new_col = self.col + col_offset
        new_row_end = self.row_end + row_offset
        new_col_end = self.col_end + col_offset

        if new_row < 0 or new_col < 0:
            raise ValueError("Offset results in invalid coordinates (< 0)")

        return Range(row=new_row, col=new_col, row_end=new_row_end, col_end=new_col_end)

    def expand(self, rows: int = 0, cols: int = 0) -> "Range":
        """Create a new Range expanded by the given amounts.

        Args:
            rows: Number of rows to add to the end
            cols: Number of columns to add to the end

        Returns:
            New Range with expanded dimensions

        Raises:
            ValueError: If expansion would result in invalid coordinates
        """
        new_row_end = self.row_end + rows
        new_col_end = self.col_end + cols

        if new_row_end < self.row or new_col_end < self.col:
            raise ValueError("Expansion results in invalid range")

        return Range(row=self.row, col=self.col, row_end=new_row_end, col_end=new_col_end)

    def __repr__(self) -> str:
        return f"Range({self.to_a1()!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Range):
            return NotImplemented
        return (
            self.row == other.row
            and self.col == other.col
            and self.row_end == other.row_end
            and self.col_end == other.col_end
        )


class Formula:
    """Represents a cell formula expression.

    A Formula stores an expression string that will be evaluated by the
    spreadsheet engine. The expression may or may not start with '='.

    Attributes:
        expression: The formula expression string
    """

    def __init__(self, expression: str) -> None:
        """Initialize a Formula.

        Args:
            expression: Formula expression string (with or without leading '=')
        """
        if not isinstance(expression, str):
            raise ValueError("Formula expression must be a string")
        self.expression = expression.strip()

    def __str__(self) -> str:
        """Convert Formula to string, ensuring it starts with '='.

        Returns:
            Formula string with leading '='
        """
        if self.expression.startswith("="):
            return self.expression
        return f"={self.expression}"

    def __repr__(self) -> str:
        return f"Formula({str(self)!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Formula):
            return NotImplemented
        # Compare normalized forms (both with '=')
        return str(self) == str(other)


class Reference:
    """Represents a cell or range reference for use in formulas.

    A Reference can be:
    - Same-sheet: A1:B10 (range_ref only)
    - Cross-sheet: Sheet2!A1:B10 (sheet_name and range_ref)

    Attributes:
        range_ref: Range or string reference (e.g., "A1:B10")
        sheet_name: Optional sheet name for cross-sheet references
    """

    def __init__(
        self, range_ref: Union[str, Range], sheet_name: Optional[str] = None
    ) -> None:
        """Initialize a Reference.

        Args:
            range_ref: Range object or A1 notation string
            sheet_name: Optional sheet name for cross-sheet references
        """
        if isinstance(range_ref, Range):
            self.range_ref = range_ref.to_a1()
        elif isinstance(range_ref, str):
            self.range_ref = range_ref.strip()
        else:
            raise ValueError("range_ref must be a Range object or string")

        self.sheet_name = sheet_name.strip() if sheet_name else None

    def to_string(self) -> str:
        """Convert Reference to formula-ready string.

        Returns:
            Reference string (e.g., "A1:B10" or "Sheet2!A1:B10")
        """
        if self.sheet_name:
            # Quote sheet name if it contains spaces or special characters
            if " " in self.sheet_name or "!" in self.sheet_name:
                return f"'{self.sheet_name}'!{self.range_ref}"
            return f"{self.sheet_name}!{self.range_ref}"
        return self.range_ref

    def is_cross_sheet(self) -> bool:
        """Check if this is a cross-sheet reference.

        Returns:
            True if this reference includes a sheet name
        """
        return self.sheet_name is not None

    def __str__(self) -> str:
        return self.to_string()

    def __repr__(self) -> str:
        if self.sheet_name:
            return f"Reference({self.range_ref!r}, sheet_name={self.sheet_name!r})"
        return f"Reference({self.range_ref!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Reference):
            return NotImplemented
        return self.range_ref == other.range_ref and self.sheet_name == other.sheet_name


class Value:
    """Wraps Python scalars for spreadsheet cell values.

    Converts Python values to spreadsheet-safe representations:
    - None → empty string
    - bool → TRUE/FALSE
    - Numbers and strings → as-is

    Attributes:
        value: The wrapped Python value
    """

    def __init__(self, value: Union[str, int, float, bool, None]) -> None:
        """Initialize a Value.

        Args:
            value: Python scalar (str, int, float, bool, or None)
        """
        self.value = value

    def to_spreadsheet(self) -> Union[str, int, float, bool]:
        """Convert to spreadsheet-safe representation.

        Returns:
            Spreadsheet-safe value:
            - None → ""
            - bool → True/False (Python bool, spreadsheet will render as TRUE/FALSE)
            - Numbers and strings → as-is
        """
        if self.value is None:
            return ""
        return self.value

    def __repr__(self) -> str:
        return f"Value({self.value!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Value):
            return NotImplemented
        return self.value == other.value
