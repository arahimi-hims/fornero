"""
Unit tests for early schema validation in algebra operations.

Tests verify that operations constructed with explicit schemas validate
column references at construction time and raise appropriate errors.
"""

import pytest
from fornero.algebra.operations import (
    Source, Select, Filter, Join, Sort, WithColumn, Union,
    SchemaValidationError
)
from fornero.algebra.expressions import col


class TestSelectValidation:
    """Tests for Select schema validation."""

    def test_select_valid_columns_succeeds(self):
        """Select with valid columns succeeds."""
        source = Source(source_id="data.csv", schema=["a", "b", "c"])
        select = Select(columns=["a", "b"], inputs=[source])
        assert select.columns == ["a", "b"]

    def test_select_invalid_column_fails(self):
        """Select with non-existent column raises SchemaValidationError."""
        source = Source(source_id="data.csv", schema=["a", "b", "c"])
        with pytest.raises(SchemaValidationError, match="non-existent columns: \\['x'\\]"):
            Select(columns=["a", "x"], inputs=[source])

    def test_select_multiple_invalid_columns_fails(self):
        """Select with multiple non-existent columns reports all."""
        source = Source(source_id="data.csv", schema=["a", "b", "c"])
        with pytest.raises(SchemaValidationError, match="\\['x', 'y'\\]"):
            Select(columns=["a", "x", "y"], inputs=[source])

    def test_select_without_schema_succeeds(self):
        """Select without input schema skips validation."""
        source = Source(source_id="data.csv")  # No schema
        select = Select(columns=["a", "b"], inputs=[source])
        assert select.columns == ["a", "b"]


class TestFilterValidation:
    """Tests for Filter schema validation."""

    def test_filter_valid_expression_succeeds(self):
        """Filter with valid expression column succeeds."""
        source = Source(source_id="data.csv", schema=["age", "name"])
        filt = Filter(predicate=col("age") > 25, inputs=[source])
        assert filt.predicate is not None

    def test_filter_invalid_column_fails(self):
        """Filter with non-existent column raises SchemaValidationError."""
        source = Source(source_id="data.csv", schema=["age", "name"])
        with pytest.raises(SchemaValidationError, match="non-existent columns: \\['salary'\\]"):
            Filter(predicate=col("salary") > 1000, inputs=[source])

    def test_filter_complex_expression_validates_all_columns(self):
        """Filter with complex expression validates all referenced columns."""
        source = Source(source_id="data.csv", schema=["a", "b", "c"])

        # Valid expression
        filt = Filter(predicate=(col("a") > 10) & (col("b") < 20), inputs=[source])
        assert filt.predicate is not None

        # Invalid expression - references non-existent column
        with pytest.raises(SchemaValidationError, match="non-existent columns: \\['x'\\]"):
            Filter(predicate=(col("a") > 10) & (col("x") < 20), inputs=[source])

    def test_filter_string_predicate_skips_validation(self):
        """Filter with string predicate skips validation (can't parse reliably)."""
        source = Source(source_id="data.csv", schema=["age", "name"])
        # String predicates can't be validated, so no error
        filt = Filter(predicate="salary > 1000", inputs=[source])
        assert filt.predicate == "salary > 1000"

    def test_filter_without_schema_succeeds(self):
        """Filter without input schema skips validation."""
        source = Source(source_id="data.csv")  # No schema
        filt = Filter(predicate=col("age") > 25, inputs=[source])
        assert filt.predicate is not None


class TestJoinValidation:
    """Tests for Join schema validation."""

    def test_join_valid_keys_succeeds(self):
        """Join with valid keys succeeds."""
        left = Source(source_id="left.csv", schema=["id", "name"])
        right = Source(source_id="right.csv", schema=["user_id", "email"])
        join = Join(left_on="id", right_on="user_id", inputs=[left, right])
        assert join.left_on == ["id"]
        assert join.right_on == ["user_id"]

    def test_join_invalid_left_key_fails(self):
        """Join with non-existent left key raises SchemaValidationError."""
        left = Source(source_id="left.csv", schema=["id", "name"])
        right = Source(source_id="right.csv", schema=["user_id", "email"])
        with pytest.raises(SchemaValidationError, match="left_on references non-existent columns: \\['user_id'\\]"):
            Join(left_on="user_id", right_on="user_id", inputs=[left, right])

    def test_join_invalid_right_key_fails(self):
        """Join with non-existent right key raises SchemaValidationError."""
        left = Source(source_id="left.csv", schema=["id", "name"])
        right = Source(source_id="right.csv", schema=["user_id", "email"])
        with pytest.raises(SchemaValidationError, match="right_on references non-existent columns: \\['id'\\]"):
            Join(left_on="id", right_on="id", inputs=[left, right])

    def test_join_multi_key_validates_all(self):
        """Join with multiple keys validates all keys."""
        left = Source(source_id="left.csv", schema=["id", "date", "name"])
        right = Source(source_id="right.csv", schema=["user_id", "timestamp", "email"])

        # Valid multi-key join
        join = Join(left_on=["id", "date"], right_on=["user_id", "timestamp"], inputs=[left, right])
        assert join.left_on == ["id", "date"]

        # Invalid multi-key join
        with pytest.raises(SchemaValidationError, match="left_on references non-existent columns: \\['x'\\]"):
            Join(left_on=["id", "x"], right_on=["user_id", "timestamp"], inputs=[left, right])

    def test_join_without_left_schema_skips_left_validation(self):
        """Join without left schema skips left key validation."""
        left = Source(source_id="left.csv")  # No schema
        right = Source(source_id="right.csv", schema=["user_id", "email"])
        join = Join(left_on="id", right_on="user_id", inputs=[left, right])
        assert join.left_on == ["id"]

    def test_join_without_right_schema_skips_right_validation(self):
        """Join without right schema skips right key validation."""
        left = Source(source_id="left.csv", schema=["id", "name"])
        right = Source(source_id="right.csv")  # No schema
        join = Join(left_on="id", right_on="user_id", inputs=[left, right])
        assert join.right_on == ["user_id"]


class TestSortValidation:
    """Tests for Sort schema validation."""

    def test_sort_valid_columns_succeeds(self):
        """Sort with valid columns succeeds."""
        source = Source(source_id="data.csv", schema=["age", "name", "salary"])
        sort = Sort(keys=[("age", "asc"), ("name", "desc")], inputs=[source])
        assert len(sort.keys) == 2

    def test_sort_invalid_column_fails(self):
        """Sort with non-existent column raises SchemaValidationError."""
        source = Source(source_id="data.csv", schema=["age", "name"])
        with pytest.raises(SchemaValidationError, match="non-existent columns: \\['salary'\\]"):
            Sort(keys=[("age", "asc"), ("salary", "desc")], inputs=[source])

    def test_sort_without_schema_succeeds(self):
        """Sort without input schema skips validation."""
        source = Source(source_id="data.csv")  # No schema
        sort = Sort(keys=[("age", "asc")], inputs=[source])
        assert sort.keys == [("age", "asc")]


class TestWithColumnValidation:
    """Tests for WithColumn schema validation."""

    def test_with_column_valid_expression_succeeds(self):
        """WithColumn with valid expression succeeds."""
        source = Source(source_id="data.csv", schema=["price", "quantity"])
        wc = WithColumn(column="total", expression=col("price") * col("quantity"), inputs=[source])
        assert wc.column == "total"

    def test_with_column_invalid_column_fails(self):
        """WithColumn with non-existent column raises SchemaValidationError."""
        source = Source(source_id="data.csv", schema=["price", "quantity"])
        with pytest.raises(SchemaValidationError, match="non-existent columns: \\['cost'\\]"):
            WithColumn(column="total", expression=col("cost") * col("quantity"), inputs=[source])

    def test_with_column_complex_expression_validates_all(self):
        """WithColumn with complex expression validates all columns."""
        source = Source(source_id="data.csv", schema=["a", "b", "c"])

        # Valid expression
        wc = WithColumn(column="result", expression=(col("a") + col("b")) / col("c"), inputs=[source])
        assert wc.column == "result"

        # Invalid expression
        with pytest.raises(SchemaValidationError, match="non-existent columns: \\['x'\\]"):
            WithColumn(column="result", expression=(col("a") + col("x")) / col("c"), inputs=[source])

    def test_with_column_string_expression_skips_validation(self):
        """WithColumn with string expression skips validation."""
        source = Source(source_id="data.csv", schema=["price", "quantity"])
        # String expressions can't be validated
        wc = WithColumn(column="total", expression="cost * quantity", inputs=[source])
        assert wc.expression == "cost * quantity"

    def test_with_column_without_schema_succeeds(self):
        """WithColumn without input schema skips validation."""
        source = Source(source_id="data.csv")  # No schema
        wc = WithColumn(column="total", expression=col("price") * col("quantity"), inputs=[source])
        assert wc.column == "total"


class TestUnionValidation:
    """Tests for Union schema validation."""

    def test_union_identical_schemas_succeeds(self):
        """Union with identical schemas succeeds."""
        left = Source(source_id="left.csv", schema=["a", "b", "c"])
        right = Source(source_id="right.csv", schema=["a", "b", "c"])
        union = Union(inputs=[left, right])
        assert len(union.inputs) == 2

    def test_union_different_schemas_fails(self):
        """Union with different schemas raises SchemaValidationError."""
        left = Source(source_id="left.csv", schema=["a", "b", "c"])
        right = Source(source_id="right.csv", schema=["a", "b", "d"])
        with pytest.raises(SchemaValidationError, match="Union requires identical schemas"):
            Union(inputs=[left, right])

    def test_union_different_column_order_fails(self):
        """Union with different column order fails (schema equality is strict)."""
        left = Source(source_id="left.csv", schema=["a", "b", "c"])
        right = Source(source_id="right.csv", schema=["a", "c", "b"])
        with pytest.raises(SchemaValidationError, match="Union requires identical schemas"):
            Union(inputs=[left, right])

    def test_union_different_lengths_fails(self):
        """Union with different schema lengths fails."""
        left = Source(source_id="left.csv", schema=["a", "b", "c"])
        right = Source(source_id="right.csv", schema=["a", "b"])
        with pytest.raises(SchemaValidationError, match="Union requires identical schemas"):
            Union(inputs=[left, right])

    def test_union_without_left_schema_skips_validation(self):
        """Union without left schema skips validation."""
        left = Source(source_id="left.csv")  # No schema
        right = Source(source_id="right.csv", schema=["a", "b", "c"])
        union = Union(inputs=[left, right])
        assert len(union.inputs) == 2

    def test_union_without_right_schema_skips_validation(self):
        """Union without right schema skips validation."""
        left = Source(source_id="left.csv", schema=["a", "b", "c"])
        right = Source(source_id="right.csv")  # No schema
        union = Union(inputs=[left, right])
        assert len(union.inputs) == 2

    def test_union_without_either_schema_skips_validation(self):
        """Union without either schema skips validation."""
        left = Source(source_id="left.csv")  # No schema
        right = Source(source_id="right.csv")  # No schema
        union = Union(inputs=[left, right])
        assert len(union.inputs) == 2


class TestValidationErrorMessages:
    """Tests for schema validation error message quality."""

    def test_error_message_shows_available_columns(self):
        """Error messages include available columns for context."""
        source = Source(source_id="data.csv", schema=["a", "b", "c"])
        try:
            Select(columns=["x"], inputs=[source])
            pytest.fail("Expected SchemaValidationError")
        except SchemaValidationError as e:
            assert "Available columns: ['a', 'b', 'c']" in str(e)

    def test_error_message_shows_missing_columns(self):
        """Error messages clearly identify missing columns."""
        source = Source(source_id="data.csv", schema=["a", "b", "c"])
        try:
            Select(columns=["a", "x", "y"], inputs=[source])
            pytest.fail("Expected SchemaValidationError")
        except SchemaValidationError as e:
            assert "['x', 'y']" in str(e)

    def test_union_error_shows_both_schemas(self):
        """Union error shows both schemas for comparison."""
        left = Source(source_id="left.csv", schema=["a", "b", "c"])
        right = Source(source_id="right.csv", schema=["a", "b", "d"])
        try:
            Union(inputs=[left, right])
            pytest.fail("Expected SchemaValidationError")
        except SchemaValidationError as e:
            assert "Left schema:" in str(e)
            assert "Right schema:" in str(e)
            assert "['a', 'b', 'c']" in str(e)
            assert "['a', 'b', 'd']" in str(e)
