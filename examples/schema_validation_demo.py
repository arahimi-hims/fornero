"""
Demo: Early Schema Validation

This script demonstrates the early schema validation feature added to algebra operations.
When operations are constructed with explicit schemas, validation happens immediately
rather than being deferred to execution or translation time.
"""

from fornero.algebra import Source, Select, Filter, Join, Sort, Union, WithColumn, col, SchemaValidationError


def demo_select_validation():
    """Select validates that columns exist in the input schema."""
    print("=== Select Validation ===")

    # Create a source with explicit schema
    source = Source(source_id="data.csv", schema=["name", "age", "salary"])

    # Valid select - this works
    try:
        valid_select = Select(columns=["name", "age"], inputs=[source])
        print("Valid select created successfully")
    except SchemaValidationError as e:
        print(f"Error: {e}")

    # Invalid select - references non-existent column
    try:
        invalid_select = Select(columns=["name", "department"], inputs=[source])
        print("This shouldn't print - validation should have failed!")
    except SchemaValidationError as e:
        print(f"Caught error as expected: {e}")

    print()


def demo_filter_validation():
    """Filter validates that predicate columns exist."""
    print("=== Filter Validation ===")

    source = Source(source_id="data.csv", schema=["name", "age", "salary"])

    # Valid filter with Expression AST
    try:
        valid_filter = Filter(predicate=col("age") > 25, inputs=[source])
        print("Valid filter created successfully")
    except SchemaValidationError as e:
        print(f"Error: {e}")

    # Invalid filter - references non-existent column
    try:
        invalid_filter = Filter(predicate=col("department") == "Engineering", inputs=[source])
        print("This shouldn't print!")
    except SchemaValidationError as e:
        print(f"Caught error as expected: {e}")

    # String predicates are not validated (can't parse reliably)
    try:
        string_filter = Filter(predicate="department == 'Engineering'", inputs=[source])
        print("String predicate: validation skipped (OK)")
    except SchemaValidationError as e:
        print(f"Error: {e}")

    print()


def demo_join_validation():
    """Join validates that join keys exist in both inputs."""
    print("=== Join Validation ===")

    left = Source(source_id="employees.csv", schema=["emp_id", "name", "dept_id"])
    right = Source(source_id="departments.csv", schema=["dept_id", "dept_name"])

    # Valid join
    try:
        valid_join = Join(left_on="dept_id", right_on="dept_id", inputs=[left, right])
        print("Valid join created successfully")
    except SchemaValidationError as e:
        print(f"Error: {e}")

    # Invalid left key
    try:
        invalid_join = Join(left_on="department_id", right_on="dept_id", inputs=[left, right])
        print("This shouldn't print!")
    except SchemaValidationError as e:
        print(f"Caught error as expected: {e}")

    # Invalid right key
    try:
        invalid_join = Join(left_on="dept_id", right_on="id", inputs=[left, right])
        print("This shouldn't print!")
    except SchemaValidationError as e:
        print(f"Caught error as expected: {e}")

    print()


def demo_union_validation():
    """Union validates that both inputs have identical schemas."""
    print("=== Union Validation ===")

    # Valid union - identical schemas
    try:
        left = Source(source_id="data1.csv", schema=["a", "b", "c"])
        right = Source(source_id="data2.csv", schema=["a", "b", "c"])
        valid_union = Union(inputs=[left, right])
        print("Valid union created successfully")
    except SchemaValidationError as e:
        print(f"Error: {e}")

    # Invalid union - different schemas
    try:
        left = Source(source_id="data1.csv", schema=["a", "b", "c"])
        right = Source(source_id="data2.csv", schema=["a", "b", "d"])
        invalid_union = Union(inputs=[left, right])
        print("This shouldn't print!")
    except SchemaValidationError as e:
        print(f"Caught error as expected: {e}")

    # Invalid union - different column order
    try:
        left = Source(source_id="data1.csv", schema=["a", "b", "c"])
        right = Source(source_id="data2.csv", schema=["a", "c", "b"])
        invalid_union = Union(inputs=[left, right])
        print("This shouldn't print!")
    except SchemaValidationError as e:
        print(f"Caught error as expected: {e}")

    print()


def demo_graceful_skipping():
    """Validation is skipped when schemas are not available."""
    print("=== Graceful Skipping ===")

    # Source without schema - validation skipped
    source = Source(source_id="data.csv")  # No schema provided

    # These operations will succeed even with invalid columns
    # because validation is skipped when schema is unavailable
    try:
        select = Select(columns=["any", "columns"], inputs=[source])
        print("Select without schema: validation skipped (OK)")

        filter_op = Filter(predicate=col("nonexistent") > 10, inputs=[source])
        print("Filter without schema: validation skipped (OK)")

        sort_op = Sort(keys=[("missing_column", "asc")], inputs=[source])
        print("Sort without schema: validation skipped (OK)")

    except SchemaValidationError as e:
        print(f"Unexpected error: {e}")

    print()


def demo_with_column_validation():
    """WithColumn validates expression column references."""
    print("=== WithColumn Validation ===")

    source = Source(source_id="data.csv", schema=["price", "quantity", "discount"])

    # Valid with column
    try:
        valid_wc = WithColumn(
            column="total",
            expression=col("price") * col("quantity"),
            inputs=[source]
        )
        print("Valid WithColumn created successfully")
    except SchemaValidationError as e:
        print(f"Error: {e}")

    # Invalid - references non-existent column
    try:
        invalid_wc = WithColumn(
            column="total",
            expression=col("price") * col("amount"),  # 'amount' doesn't exist
            inputs=[source]
        )
        print("This shouldn't print!")
    except SchemaValidationError as e:
        print(f"Caught error as expected: {e}")

    print()


if __name__ == "__main__":
    print("Schema Validation Demo\n")
    print("This demonstrates early validation of algebra operations.")
    print("Errors are caught at construction time when schemas are available.\n")

    demo_select_validation()
    demo_filter_validation()
    demo_join_validation()
    demo_union_validation()
    demo_with_column_validation()
    demo_graceful_skipping()

    print("=== Summary ===")
    print("Early schema validation provides:")
    print("- Immediate error detection at construction time")
    print("- Clear error messages with available columns")
    print("- Graceful degradation when schemas unavailable")
    print("- No impact on existing code without explicit schemas")
