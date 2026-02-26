"""
Unit tests for spreadsheet model classes (Task 7).

Tests cover:
- Sheet: name validation, dimension storage
- Range: A1 notation parsing/formatting, arithmetic operations
- Formula: expression storage, string representation
- Reference: same-sheet vs cross-sheet references
- Value: Python scalar wrapping and spreadsheet conversion
"""

import pytest
from fornero.spreadsheet.model import Sheet, Range, Formula, Reference, Value


class TestSheet:
    """Test Suite for Sheet class."""

    def test_sheet_creation_valid(self):
        """Test creating a sheet with valid parameters."""
        sheet = Sheet("Data", 100, 26)
        assert sheet.name == "Data"
        assert sheet.rows == 100
        assert sheet.cols == 26

    def test_sheet_name_must_be_non_empty(self):
        """Test that sheet name cannot be empty."""
        with pytest.raises(ValueError, match="non-empty"):
            Sheet("", 100, 26)

    def test_sheet_name_must_be_string(self):
        """Test that sheet name must be a string."""
        with pytest.raises(ValueError, match="non-empty"):
            Sheet(None, 100, 26)  # type: ignore

    def test_sheet_dimensions_must_be_positive(self):
        """Test that sheet dimensions must be positive."""
        with pytest.raises(ValueError, match="positive"):
            Sheet("Data", 0, 26)

        with pytest.raises(ValueError, match="positive"):
            Sheet("Data", 100, 0)

        with pytest.raises(ValueError, match="positive"):
            Sheet("Data", -10, 26)

    def test_sheet_equality(self):
        """Test sheet equality comparison."""
        sheet1 = Sheet("Data", 100, 26)
        sheet2 = Sheet("Data", 100, 26)
        sheet3 = Sheet("Data", 200, 26)
        sheet4 = Sheet("Other", 100, 26)

        assert sheet1 == sheet2
        assert sheet1 != sheet3
        assert sheet1 != sheet4

    def test_sheet_repr(self):
        """Test sheet string representation."""
        sheet = Sheet("Data", 100, 26)
        assert repr(sheet) == "Sheet(name='Data', rows=100, cols=26)"


class TestRange:
    """Test Suite for Range class."""

    def test_range_single_cell(self):
        """Test creating a single-cell range."""
        r = Range(row=1, col=1)
        assert r.row == 1
        assert r.col == 1
        assert r.row_end == 1
        assert r.col_end == 1

    def test_range_multi_cell(self):
        """Test creating a multi-cell range."""
        r = Range(row=1, col=1, row_end=10, col_end=3)
        assert r.row == 1
        assert r.col == 1
        assert r.row_end == 10
        assert r.col_end == 3

    def test_range_invalid_coordinates(self):
        """Test that invalid coordinates raise errors (0-indexed)."""
        with pytest.raises(ValueError, match="non-negative"):
            Range(row=-1, col=0)

        with pytest.raises(ValueError, match="non-negative"):
            Range(row=0, col=-1)

        with pytest.raises(ValueError, match="End coordinates"):
            Range(row=10, col=5, row_end=5, col_end=10)

    def test_from_a1_single_cell(self):
        """Test parsing single cell A1 notation (returns 0-indexed)."""
        r = Range.from_a1("A1")
        assert r.row == 0  # 0-indexed internally
        assert r.col == 0  # 0-indexed internally
        assert r.row_end == 0
        assert r.col_end == 0

    def test_from_a1_single_cell_multi_letter(self):
        """Test parsing single cell with multi-letter column (returns 0-indexed)."""
        r = Range.from_a1("ZZ1")
        assert r.row == 0  # 0-indexed: row 1 -> index 0
        assert r.col == 701  # 0-indexed: ZZ (1-indexed 702) -> index 701

        r2 = Range.from_a1("AA10")
        assert r2.row == 9  # 0-indexed: row 10 -> index 9
        assert r2.col == 26  # 0-indexed: AA (1-indexed 27) -> index 26

    def test_from_a1_cell_range(self):
        """Test parsing cell range A1 notation (returns 0-indexed)."""
        r = Range.from_a1("A2:C100")
        assert r.row == 1  # 0-indexed: row 2 -> index 1
        assert r.col == 0  # 0-indexed: col A -> index 0
        assert r.row_end == 99  # 0-indexed: row 100 -> index 99
        assert r.col_end == 2  # 0-indexed: col C -> index 2

    def test_from_a1_multi_letter_range(self):
        """Test parsing range with multi-letter columns (returns 0-indexed)."""
        r = Range.from_a1("A1:ZZ100")
        assert r.row == 0  # 0-indexed: row 1 -> index 0
        assert r.col == 0  # 0-indexed: col A -> index 0
        assert r.row_end == 99  # 0-indexed: row 100 -> index 99
        assert r.col_end == 701  # 0-indexed: col ZZ (1-indexed 702) -> index 701

    def test_from_a1_case_insensitive(self):
        """Test that A1 notation parsing is case-insensitive."""
        r1 = Range.from_a1("a1:b10")
        r2 = Range.from_a1("A1:B10")
        assert r1 == r2

    def test_from_a1_whitespace_handling(self):
        """Test that whitespace is handled correctly (returns 0-indexed)."""
        r = Range.from_a1("  A1:B10  ")
        assert r.row == 0  # 0-indexed: row 1 -> index 0
        assert r.col == 0  # 0-indexed: col A -> index 0
        assert r.row_end == 9  # 0-indexed: row 10 -> index 9
        assert r.col_end == 1  # 0-indexed: col B -> index 1

    def test_from_a1_invalid_notation(self):
        """Test that invalid A1 notation raises errors."""
        with pytest.raises(ValueError, match="Empty"):
            Range.from_a1("")

        with pytest.raises(ValueError, match="Invalid"):
            Range.from_a1("ABC")

        with pytest.raises(ValueError, match="Invalid"):
            Range.from_a1("1A")

        with pytest.raises(ValueError, match="Invalid"):
            Range.from_a1("A1:B2:C3")

    def test_to_a1_single_cell(self):
        """Test converting single cell to A1 notation (from 0-indexed)."""
        r = Range(row=0, col=0)
        assert r.to_a1() == "A1"

        r2 = Range(row=9, col=26)  # 0-indexed: row 10, col AA
        assert r2.to_a1() == "AA10"

    def test_to_a1_multi_cell_range(self):
        """Test converting range to A1 notation (from 0-indexed)."""
        r = Range(row=1, col=0, row_end=99, col_end=2)  # 0-indexed: A2:C100
        assert r.to_a1() == "A2:C100"

    def test_to_a1_multi_letter_column(self):
        """Test converting range with multi-letter columns (from 0-indexed)."""
        r = Range(row=0, col=0, row_end=99, col_end=701)  # 0-indexed: A1:ZZ100
        assert r.to_a1() == "A1:ZZ100"

    def test_a1_round_trip(self):
        """Test that from_a1 and to_a1 round-trip correctly."""
        test_cases = ["A1", "ZZ1", "A2:C100", "AA1:ZZ1000"]
        for notation in test_cases:
            r = Range.from_a1(notation)
            assert r.to_a1() == notation

        # Single-cell range normalizes to single cell notation
        r = Range.from_a1("B5:B5")
        assert r.to_a1() == "B5"

    def test_range_intersection_overlapping(self):
        """Test intersection of overlapping ranges."""
        r1 = Range.from_a1("A1:C10")
        r2 = Range.from_a1("B5:D15")
        intersection = r1.intersect(r2)

        assert intersection is not None
        assert intersection.to_a1() == "B5:C10"

    def test_range_intersection_no_overlap(self):
        """Test intersection of non-overlapping ranges."""
        r1 = Range.from_a1("A1:B10")
        r2 = Range.from_a1("D1:E10")
        intersection = r1.intersect(r2)

        assert intersection is None

    def test_range_intersection_contained(self):
        """Test intersection where one range contains another."""
        r1 = Range.from_a1("A1:D20")
        r2 = Range.from_a1("B5:C15")
        intersection = r1.intersect(r2)

        assert intersection is not None
        assert intersection == r2

    def test_range_union(self):
        """Test union (bounding box) of ranges."""
        r1 = Range.from_a1("A1:B10")
        r2 = Range.from_a1("C5:D15")
        union = r1.union(r2)

        assert union.to_a1() == "A1:D15"

    def test_range_union_contained(self):
        """Test union where one range contains another."""
        r1 = Range.from_a1("A1:D20")
        r2 = Range.from_a1("B5:C15")
        union = r1.union(r2)

        assert union == r1

    def test_range_offset_positive(self):
        """Test offsetting range by positive amounts."""
        r = Range.from_a1("A1:B10")
        offset = r.offset(row_offset=5, col_offset=2)

        assert offset.to_a1() == "C6:D15"

    def test_range_offset_negative(self):
        """Test offsetting range by negative amounts."""
        r = Range.from_a1("D10:E20")
        offset = r.offset(row_offset=-5, col_offset=-2)

        assert offset.to_a1() == "B5:C15"

    def test_range_offset_invalid(self):
        """Test that invalid offset raises error."""
        r = Range.from_a1("A1:B10")
        with pytest.raises(ValueError, match="invalid coordinates"):
            r.offset(row_offset=-1, col_offset=0)

        with pytest.raises(ValueError, match="invalid coordinates"):
            r.offset(row_offset=0, col_offset=-1)

    def test_range_expand(self):
        """Test expanding range dimensions."""
        r = Range.from_a1("A1:B10")
        expanded = r.expand(rows=5, cols=3)

        assert expanded.to_a1() == "A1:E15"

    def test_range_expand_shrink_invalid(self):
        """Test that shrinking beyond start raises error."""
        r = Range.from_a1("A1:B10")
        with pytest.raises(ValueError, match="invalid range"):
            r.expand(rows=-20, cols=0)

    def test_range_equality(self):
        """Test range equality comparison."""
        r1 = Range.from_a1("A1:B10")
        r2 = Range.from_a1("A1:B10")
        r3 = Range.from_a1("A1:C10")

        assert r1 == r2
        assert r1 != r3

    def test_range_repr(self):
        """Test range string representation."""
        r = Range.from_a1("A1:B10")
        assert repr(r) == "Range('A1:B10')"


class TestFormula:
    """Test Suite for Formula class."""

    def test_formula_with_equals(self):
        """Test formula that starts with '='."""
        f = Formula("=SUM(A1:A10)")
        assert f.expression == "=SUM(A1:A10)"
        assert str(f) == "=SUM(A1:A10)"

    def test_formula_without_equals(self):
        """Test formula without leading '=' gets it prepended."""
        f = Formula("SUM(A1:A10)")
        assert f.expression == "SUM(A1:A10)"
        assert str(f) == "=SUM(A1:A10)"

    def test_formula_complex_expression(self):
        """Test formula with complex expression."""
        expr = "=FILTER(A2:C100, B2:B100>10)"
        f = Formula(expr)
        assert str(f) == expr

    def test_formula_whitespace_trimmed(self):
        """Test that formula whitespace is trimmed."""
        f = Formula("  =SUM(A1:A10)  ")
        assert f.expression == "=SUM(A1:A10)"

    def test_formula_equality(self):
        """Test formula equality comparison (normalized with '=')."""
        f1 = Formula("=SUM(A1:A10)")
        f2 = Formula("SUM(A1:A10)")
        f3 = Formula("=SUM(A1:A20)")

        assert f1 == f2  # Both normalize to same form
        assert f1 != f3

    def test_formula_repr(self):
        """Test formula string representation."""
        f = Formula("SUM(A1:A10)")
        assert repr(f) == "Formula('=SUM(A1:A10)')"

    def test_formula_must_be_string(self):
        """Test that formula expression must be a string."""
        with pytest.raises(ValueError, match="string"):
            Formula(123)  # type: ignore


class TestReference:
    """Test Suite for Reference class."""

    def test_reference_same_sheet_string(self):
        """Test same-sheet reference with string."""
        ref = Reference("A1:B10")
        assert ref.range_ref == "A1:B10"
        assert ref.sheet_name is None
        assert not ref.is_cross_sheet()
        assert ref.to_string() == "A1:B10"

    def test_reference_same_sheet_range_object(self):
        """Test same-sheet reference with Range object."""
        r = Range.from_a1("A1:B10")
        ref = Reference(r)
        assert ref.range_ref == "A1:B10"
        assert ref.sheet_name is None
        assert not ref.is_cross_sheet()
        assert ref.to_string() == "A1:B10"

    def test_reference_cross_sheet(self):
        """Test cross-sheet reference."""
        ref = Reference("A1:B10", sheet_name="Sheet2")
        assert ref.range_ref == "A1:B10"
        assert ref.sheet_name == "Sheet2"
        assert ref.is_cross_sheet()
        assert ref.to_string() == "Sheet2!A1:B10"

    def test_reference_cross_sheet_with_spaces(self):
        """Test cross-sheet reference with spaces in sheet name."""
        ref = Reference("A1:B10", sheet_name="My Data")
        assert ref.to_string() == "'My Data'!A1:B10"

    def test_reference_cross_sheet_with_exclamation(self):
        """Test cross-sheet reference with exclamation in sheet name."""
        ref = Reference("A1:B10", sheet_name="Data!")
        assert ref.to_string() == "'Data!'!A1:B10"

    def test_reference_equality(self):
        """Test reference equality comparison."""
        ref1 = Reference("A1:B10")
        ref2 = Reference("A1:B10")
        ref3 = Reference("A1:B10", sheet_name="Sheet2")
        ref4 = Reference("A1:B10", sheet_name="Sheet2")
        ref5 = Reference("A1:C10")

        assert ref1 == ref2
        assert ref1 != ref3
        assert ref3 == ref4
        assert ref1 != ref5

    def test_reference_str_same_sheet(self):
        """Test string representation for same-sheet reference."""
        ref = Reference("A1:B10")
        assert str(ref) == "A1:B10"

    def test_reference_str_cross_sheet(self):
        """Test string representation for cross-sheet reference."""
        ref = Reference("A1:B10", sheet_name="Sheet2")
        assert str(ref) == "Sheet2!A1:B10"

    def test_reference_repr(self):
        """Test reference repr."""
        ref1 = Reference("A1:B10")
        assert repr(ref1) == "Reference('A1:B10')"

        ref2 = Reference("A1:B10", sheet_name="Sheet2")
        assert repr(ref2) == "Reference('A1:B10', sheet_name='Sheet2')"

    def test_reference_invalid_type(self):
        """Test that invalid range_ref type raises error."""
        with pytest.raises(ValueError, match="Range object or string"):
            Reference(123)  # type: ignore


class TestValue:
    """Test Suite for Value class."""

    def test_value_string(self):
        """Test Value with string."""
        v = Value("hello")
        assert v.value == "hello"
        assert v.to_spreadsheet() == "hello"

    def test_value_int(self):
        """Test Value with integer."""
        v = Value(42)
        assert v.value == 42
        assert v.to_spreadsheet() == 42

    def test_value_float(self):
        """Test Value with float."""
        v = Value(3.14)
        assert v.value == 3.14
        assert v.to_spreadsheet() == 3.14

    def test_value_bool_true(self):
        """Test Value with boolean True."""
        v = Value(True)
        assert v.value is True
        assert v.to_spreadsheet() is True

    def test_value_bool_false(self):
        """Test Value with boolean False."""
        v = Value(False)
        assert v.value is False
        assert v.to_spreadsheet() is False

    def test_value_none(self):
        """Test Value with None converts to empty string."""
        v = Value(None)
        assert v.value is None
        assert v.to_spreadsheet() == ""

    def test_value_equality(self):
        """Test value equality comparison."""
        v1 = Value(42)
        v2 = Value(42)
        v3 = Value(43)
        v4 = Value(None)
        v5 = Value(None)

        assert v1 == v2
        assert v1 != v3
        assert v4 == v5

    def test_value_repr(self):
        """Test value string representation."""
        v1 = Value("hello")
        assert repr(v1) == "Value('hello')"

        v2 = Value(None)
        assert repr(v2) == "Value(None)"


class TestColumnLetterConversion:
    """Test Suite for column letter/number conversion (0-indexed)."""

    def test_col_to_letter_single(self):
        """Test converting single-letter columns (0-indexed input)."""
        assert Range._col_to_letter(0) == "A"   # 0-indexed: 0 -> A
        assert Range._col_to_letter(25) == "Z"  # 0-indexed: 25 -> Z

    def test_col_to_letter_double(self):
        """Test converting double-letter columns (0-indexed input)."""
        assert Range._col_to_letter(26) == "AA"   # 0-indexed: 26 -> AA
        assert Range._col_to_letter(51) == "AZ"   # 0-indexed: 51 -> AZ
        assert Range._col_to_letter(701) == "ZZ"  # 0-indexed: 701 -> ZZ

    def test_col_to_letter_triple(self):
        """Test converting triple-letter columns (0-indexed input)."""
        assert Range._col_to_letter(702) == "AAA"  # 0-indexed: 702 -> AAA

    def test_letter_to_col_single(self):
        """Test converting single-letter columns to numbers (0-indexed output)."""
        assert Range._letter_to_col("A") == 0   # A -> 0-indexed: 0
        assert Range._letter_to_col("Z") == 25  # Z -> 0-indexed: 25

    def test_letter_to_col_double(self):
        """Test converting double-letter columns to numbers (0-indexed output)."""
        assert Range._letter_to_col("AA") == 26   # AA -> 0-indexed: 26
        assert Range._letter_to_col("AZ") == 51   # AZ -> 0-indexed: 51
        assert Range._letter_to_col("ZZ") == 701  # ZZ -> 0-indexed: 701

    def test_letter_to_col_triple(self):
        """Test converting triple-letter columns to numbers (0-indexed output)."""
        assert Range._letter_to_col("AAA") == 702  # AAA -> 0-indexed: 702

    def test_letter_to_col_case_insensitive(self):
        """Test that letter to column conversion is case-insensitive (0-indexed output)."""
        assert Range._letter_to_col("aa") == 26  # AA -> 0-indexed: 26
        assert Range._letter_to_col("AA") == 26
        assert Range._letter_to_col("Aa") == 26

    def test_column_conversion_round_trip(self):
        """Test that column conversions round-trip correctly (0-indexed)."""
        test_cols = [0, 25, 26, 51, 99, 701, 702]  # 0-indexed values
        for col in test_cols:
            letter = Range._col_to_letter(col)
            assert Range._letter_to_col(letter) == col


class TestIntegration:
    """Integration tests combining multiple classes."""

    def test_formula_with_reference(self):
        """Test creating a formula that uses a reference."""
        ref = Reference("A1:A10")
        formula = Formula(f"=SUM({ref})")
        assert str(formula) == "=SUM(A1:A10)"

    def test_formula_with_cross_sheet_reference(self):
        """Test creating a formula with cross-sheet reference."""
        ref = Reference("A1:A10", sheet_name="Data")
        formula = Formula(f"=SUM({ref})")
        assert str(formula) == "=SUM(Data!A1:A10)"

    def test_range_operations_chain(self):
        """Test chaining range operations."""
        r = Range.from_a1("A1:B10")
        r2 = r.offset(row_offset=5, col_offset=0)
        r3 = r2.expand(rows=5, cols=0)

        assert r3.to_a1() == "A6:B20"

    def test_sheet_with_range(self):
        """Test using sheet and range together."""
        sheet = Sheet("Data", 1000, 100)
        data_range = Range.from_a1("A2:Z100")

        ref = Reference(data_range, sheet_name=sheet.name)
        assert ref.to_string() == "Data!A2:Z100"
