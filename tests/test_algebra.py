"""
Unit tests for the algebra module (Tasks 5-6).

These tests verify:
1. All operation nodes can be constructed with valid arguments
2. Invalid construction arguments raise appropriate errors
3. to_dict() produces JSON-serializable output with correct structure
4. from_dict() can deserialize operations (round-trip)
5. LogicalPlan.explain() produces readable output

No external dependencies (no API calls, no filesystem except fixtures).
"""

import pytest
from fornero.algebra import (
    LogicalPlan,
    Operation,
    Source,
    Select,
    Filter,
    Join,
    GroupBy,
    Aggregate,
    Sort,
    Limit,
    WithColumn,
    Union,
    Pivot,
    Melt,
    Window,
)


class TestSource:
    """Tests for Source operation."""

    def test_construction_succeeds(self):
        """Source with valid arguments constructs successfully."""
        source = Source(source_id="data.csv", schema=["a", "b", "c"])
        assert source.source_id == "data.csv"
        assert source.schema == ["a", "b", "c"]
        assert source.inputs == []

    def test_construction_with_inputs_fails(self):
        """Source with inputs raises ValueError."""
        dummy_source = Source(source_id="dummy")
        with pytest.raises(ValueError, match="Source operation cannot have inputs"):
            Source(source_id="data.csv", inputs=[dummy_source])

    def test_to_dict(self):
        """to_dict() returns correct structure."""
        source = Source(source_id="data.csv", schema=["a", "b"])
        data = source.to_dict()
        assert data['type'] == 'source'
        assert data['source_id'] == "data.csv"
        assert data['schema'] == ["a", "b"]
        assert data['inputs'] == []

    def test_round_trip(self):
        """from_dict(to_dict()) produces equivalent operation."""
        source = Source(source_id="test.csv", schema=["x", "y"])
        data = source.to_dict()
        restored = Operation.from_dict(data)
        assert isinstance(restored, Source)
        assert restored.source_id == source.source_id
        assert restored.schema == source.schema
        assert restored.inputs == []


class TestSelect:
    """Tests for Select operation."""

    def test_construction_succeeds(self):
        """Select with valid arguments constructs successfully."""
        source = Source(source_id="data.csv")
        select = Select(columns=["a", "b"], inputs=[source])
        assert select.columns == ["a", "b"]
        assert len(select.inputs) == 1

    def test_construction_without_input_fails(self):
        """Select without input raises ValueError."""
        with pytest.raises(ValueError, match="must have exactly one input"):
            Select(columns=["a"])

    def test_construction_with_empty_columns_fails(self):
        """Select with empty columns list raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must specify at least one column"):
            Select(columns=[], inputs=[source])

    def test_to_dict(self):
        """to_dict() returns correct structure."""
        source = Source(source_id="data.csv")
        select = Select(columns=["a", "b"], inputs=[source])
        data = select.to_dict()
        assert data['type'] == 'select'
        assert data['columns'] == ["a", "b"]
        assert data['input']['type'] == 'source'

    def test_round_trip(self):
        """from_dict(to_dict()) produces equivalent operation."""
        source = Source(source_id="data.csv")
        select = Select(columns=["a", "b"], inputs=[source])
        data = select.to_dict()
        restored = Operation.from_dict(data)
        assert isinstance(restored, Select)
        assert restored.columns == select.columns
        assert len(restored.inputs) == 1


class TestFilter:
    """Tests for Filter operation."""

    def test_construction_succeeds(self):
        """Filter with valid arguments constructs successfully."""
        source = Source(source_id="data.csv")
        filt = Filter(predicate="age > 25", inputs=[source])
        assert filt.predicate == "age > 25"
        assert len(filt.inputs) == 1

    def test_construction_without_input_fails(self):
        """Filter without input raises ValueError."""
        with pytest.raises(ValueError, match="must have exactly one input"):
            Filter(predicate="age > 25")

    def test_construction_without_predicate_fails(self):
        """Filter without predicate raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must specify a predicate"):
            Filter(predicate="", inputs=[source])

    def test_to_dict(self):
        """to_dict() returns correct structure."""
        source = Source(source_id="data.csv")
        filt = Filter(predicate="x > 10", inputs=[source])
        data = filt.to_dict()
        assert data['type'] == 'filter'
        assert data['predicate'] == "x > 10"
        assert data['input']['type'] == 'source'

    def test_round_trip(self):
        """from_dict(to_dict()) produces equivalent operation."""
        source = Source(source_id="data.csv")
        filt = Filter(predicate="x > 10", inputs=[source])
        data = filt.to_dict()
        restored = Operation.from_dict(data)
        assert isinstance(restored, Filter)
        assert restored.predicate == filt.predicate


class TestJoin:
    """Tests for Join operation."""

    def test_construction_succeeds(self):
        """Join with valid arguments constructs successfully."""
        left = Source(source_id="left.csv")
        right = Source(source_id="right.csv")
        join = Join(left_on="id", right_on="user_id", join_type="inner", inputs=[left, right])
        assert join.left_on == ["id"]
        assert join.right_on == ["user_id"]
        assert join.join_type == "inner"
        assert len(join.inputs) == 2

    def test_construction_without_two_inputs_fails(self):
        """Join without exactly two inputs raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must have exactly two inputs"):
            Join(left_on="id", right_on="user_id", inputs=[source])

    def test_construction_without_keys_fails(self):
        """Join without join keys raises ValueError."""
        left = Source(source_id="left.csv")
        right = Source(source_id="right.csv")
        with pytest.raises(ValueError, match="must specify join keys"):
            Join(left_on="", right_on="", inputs=[left, right])

    def test_invalid_join_type_fails(self):
        """Join with invalid join_type raises ValueError."""
        left = Source(source_id="left.csv")
        right = Source(source_id="right.csv")
        with pytest.raises(ValueError, match="Join type must be one of"):
            Join(left_on="id", right_on="user_id", join_type="invalid", inputs=[left, right])

    def test_join_types_accepted(self):
        """All valid join types are accepted."""
        left = Source(source_id="left.csv")
        right = Source(source_id="right.csv")
        for join_type in ["inner", "left", "right", "outer"]:
            join = Join(left_on="id", right_on="user_id", join_type=join_type, inputs=[left, right])
            assert join.join_type == join_type

    def test_to_dict(self):
        """to_dict() returns correct structure."""
        left = Source(source_id="left.csv")
        right = Source(source_id="right.csv")
        join = Join(left_on="id", right_on="user_id", join_type="left", inputs=[left, right])
        data = join.to_dict()
        assert data['type'] == 'join'
        assert data['left_on'] == ["id"]
        assert data['right_on'] == ["user_id"]
        assert data['join_type'] == "left"
        assert len(data['inputs']) == 2  # binary ops keep 'inputs'

    def test_round_trip(self):
        """from_dict(to_dict()) produces equivalent operation."""
        left = Source(source_id="left.csv")
        right = Source(source_id="right.csv")
        join = Join(left_on=["id"], right_on=["user_id"], join_type="inner", inputs=[left, right])
        data = join.to_dict()
        restored = Operation.from_dict(data)
        assert isinstance(restored, Join)
        assert restored.left_on == join.left_on
        assert restored.right_on == join.right_on
        assert restored.join_type == join.join_type


class TestGroupBy:
    """Tests for GroupBy operation."""

    def test_construction_succeeds(self):
        """GroupBy with valid arguments constructs successfully."""
        source = Source(source_id="data.csv")
        groupby = GroupBy(
            keys=["category"],
            aggregations=[("total", "sum", "amount"), ("count", "count", "id")],
            inputs=[source]
        )
        assert groupby.keys == ["category"]
        assert len(groupby.aggregations) == 2
        assert groupby.aggregations[0] == ("total", "sum", "amount")

    def test_construction_without_input_fails(self):
        """GroupBy without input raises ValueError."""
        with pytest.raises(ValueError, match="must have exactly one input"):
            GroupBy(keys=["category"], aggregations=[("total", "sum", "amount")])

    def test_construction_without_aggregations_fails(self):
        """GroupBy without aggregations raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must specify at least one aggregation"):
            GroupBy(keys=["category"], aggregations=[], inputs=[source])

    def test_aggregation_tuples_stored_correctly(self):
        """Aggregation tuples are stored and round-trip correctly."""
        source = Source(source_id="data.csv")
        aggs = [("avg_age", "mean", "age"), ("max_salary", "max", "salary")]
        groupby = GroupBy(keys=["dept"], aggregations=aggs, inputs=[source])
        assert groupby.aggregations == aggs

    def test_to_dict(self):
        """to_dict() returns correct structure."""
        source = Source(source_id="data.csv")
        groupby = GroupBy(
            keys=["category"],
            aggregations=[("total", "sum", "amount")],
            inputs=[source]
        )
        data = groupby.to_dict()
        assert data['type'] == 'groupby'
        assert data['keys'] == ["category"]
        assert data['aggregations'] == [["total", "sum", "amount"]]

    def test_round_trip(self):
        """from_dict(to_dict()) produces equivalent operation."""
        source = Source(source_id="data.csv")
        groupby = GroupBy(
            keys=["category"],
            aggregations=[("total", "sum", "amount")],
            inputs=[source]
        )
        data = groupby.to_dict()
        restored = Operation.from_dict(data)
        assert isinstance(restored, GroupBy)
        assert restored.keys == groupby.keys
        assert restored.aggregations == groupby.aggregations


class TestAggregate:
    """Tests for Aggregate operation."""

    def test_construction_succeeds(self):
        """Aggregate with valid arguments constructs successfully."""
        source = Source(source_id="data.csv")
        agg = Aggregate(
            aggregations=[("total", "sum", "amount"), ("count", "count", "id")],
            inputs=[source]
        )
        assert len(agg.aggregations) == 2

    def test_construction_without_input_fails(self):
        """Aggregate without input raises ValueError."""
        with pytest.raises(ValueError, match="must have exactly one input"):
            Aggregate(aggregations=[("total", "sum", "amount")])

    def test_construction_without_aggregations_fails(self):
        """Aggregate without aggregations raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must specify at least one aggregation"):
            Aggregate(aggregations=[], inputs=[source])

    def test_to_dict(self):
        """to_dict() returns correct structure."""
        source = Source(source_id="data.csv")
        agg = Aggregate(aggregations=[("total", "sum", "amount")], inputs=[source])
        data = agg.to_dict()
        assert data['type'] == 'aggregate'
        assert data['aggregations'] == [["total", "sum", "amount"]]

    def test_round_trip(self):
        """from_dict(to_dict()) produces equivalent operation."""
        source = Source(source_id="data.csv")
        agg = Aggregate(aggregations=[("total", "sum", "amount")], inputs=[source])
        data = agg.to_dict()
        restored = Operation.from_dict(data)
        assert isinstance(restored, Aggregate)
        assert restored.aggregations == agg.aggregations


class TestSort:
    """Tests for Sort operation."""

    def test_construction_succeeds(self):
        """Sort with valid arguments constructs successfully."""
        source = Source(source_id="data.csv")
        sort = Sort(keys=[("age", "asc"), ("name", "desc")], inputs=[source])
        assert len(sort.keys) == 2
        assert sort.keys[0] == ("age", "asc")
        assert sort.keys[1] == ("name", "desc")

    def test_construction_without_input_fails(self):
        """Sort without input raises ValueError."""
        with pytest.raises(ValueError, match="must have exactly one input"):
            Sort(keys=[("age", "asc")])

    def test_construction_without_keys_fails(self):
        """Sort without keys raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must specify at least one sort key"):
            Sort(keys=[], inputs=[source])

    def test_invalid_direction_fails(self):
        """Sort with invalid direction raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="direction must be 'asc' or 'desc'"):
            Sort(keys=[("age", "invalid")], inputs=[source])

    def test_direction_flags_preserved(self):
        """Direction flags (asc/desc) are preserved per key."""
        source = Source(source_id="data.csv")
        sort = Sort(keys=[("a", "asc"), ("b", "desc"), ("c", "asc")], inputs=[source])
        assert sort.keys[0][1] == "asc"
        assert sort.keys[1][1] == "desc"
        assert sort.keys[2][1] == "asc"

    def test_to_dict(self):
        """to_dict() returns correct structure."""
        source = Source(source_id="data.csv")
        sort = Sort(keys=[("age", "asc")], inputs=[source])
        data = sort.to_dict()
        assert data['type'] == 'sort'
        assert data['keys'] == [["age", "asc"]]

    def test_round_trip(self):
        """from_dict(to_dict()) produces equivalent operation."""
        source = Source(source_id="data.csv")
        sort = Sort(keys=[("age", "asc"), ("name", "desc")], inputs=[source])
        data = sort.to_dict()
        restored = Operation.from_dict(data)
        assert isinstance(restored, Sort)
        assert restored.keys == sort.keys


class TestLimit:
    """Tests for Limit operation."""

    def test_construction_succeeds(self):
        """Limit with valid arguments constructs successfully."""
        source = Source(source_id="data.csv")
        limit = Limit(count=10, end="head", inputs=[source])
        assert limit.count == 10
        assert limit.end == "head"

    def test_construction_without_input_fails(self):
        """Limit without input raises ValueError."""
        with pytest.raises(ValueError, match="must have exactly one input"):
            Limit(count=10, end="head")

    def test_negative_count_fails(self):
        """Limit with negative count raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="count must be non-negative"):
            Limit(count=-5, end="head", inputs=[source])

    def test_invalid_end_fails(self):
        """Limit with invalid end raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="end must be 'head' or 'tail'"):
            Limit(count=10, end="invalid", inputs=[source])

    def test_head_vs_tail_selector_stored(self):
        """Head vs tail selector is stored correctly."""
        source = Source(source_id="data.csv")
        head_limit = Limit(count=10, end="head", inputs=[source])
        tail_limit = Limit(count=10, end="tail", inputs=[source])
        assert head_limit.end == "head"
        assert tail_limit.end == "tail"

    def test_to_dict(self):
        """to_dict() returns correct structure."""
        source = Source(source_id="data.csv")
        limit = Limit(count=5, end="tail", inputs=[source])
        data = limit.to_dict()
        assert data['type'] == 'limit'
        assert data['count'] == 5
        assert data['end'] == "tail"

    def test_round_trip(self):
        """from_dict(to_dict()) produces equivalent operation."""
        source = Source(source_id="data.csv")
        limit = Limit(count=100, end="head", inputs=[source])
        data = limit.to_dict()
        restored = Operation.from_dict(data)
        assert isinstance(restored, Limit)
        assert restored.count == limit.count
        assert restored.end == limit.end


class TestWithColumn:
    """Tests for WithColumn operation."""

    def test_construction_succeeds(self):
        """WithColumn with valid arguments constructs successfully."""
        source = Source(source_id="data.csv")
        with_col = WithColumn(column="total", expression="price * quantity", inputs=[source])
        assert with_col.column == "total"
        assert with_col.expression == "price * quantity"

    def test_construction_without_input_fails(self):
        """WithColumn without input raises ValueError."""
        with pytest.raises(ValueError, match="must have exactly one input"):
            WithColumn(column="total", expression="price * quantity")

    def test_construction_without_column_fails(self):
        """WithColumn without column name raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must specify a column name"):
            WithColumn(column="", expression="price * quantity", inputs=[source])

    def test_construction_without_expression_fails(self):
        """WithColumn without expression raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must specify an expression"):
            WithColumn(column="total", expression="", inputs=[source])

    def test_to_dict(self):
        """to_dict() returns correct structure."""
        source = Source(source_id="data.csv")
        with_col = WithColumn(column="total", expression="a + b", inputs=[source])
        data = with_col.to_dict()
        assert data['type'] == 'with_column'
        assert data['column'] == "total"
        assert data['expression'] == "a + b"

    def test_round_trip(self):
        """from_dict(to_dict()) produces equivalent operation."""
        source = Source(source_id="data.csv")
        with_col = WithColumn(column="total", expression="a + b", inputs=[source])
        data = with_col.to_dict()
        restored = Operation.from_dict(data)
        assert isinstance(restored, WithColumn)
        assert restored.column == with_col.column
        assert restored.expression == with_col.expression


class TestUnion:
    """Tests for Union operation."""

    def test_construction_succeeds(self):
        """Union with valid arguments constructs successfully."""
        left = Source(source_id="left.csv")
        right = Source(source_id="right.csv")
        union = Union(inputs=[left, right])
        assert len(union.inputs) == 2

    def test_construction_without_two_inputs_fails(self):
        """Union without exactly two inputs raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must have exactly two inputs"):
            Union(inputs=[source])

    def test_to_dict(self):
        """to_dict() returns correct structure."""
        left = Source(source_id="left.csv")
        right = Source(source_id="right.csv")
        union = Union(inputs=[left, right])
        data = union.to_dict()
        assert data['type'] == 'union'
        assert len(data['inputs']) == 2

    def test_round_trip(self):
        """from_dict(to_dict()) produces equivalent operation."""
        left = Source(source_id="left.csv")
        right = Source(source_id="right.csv")
        union = Union(inputs=[left, right])
        data = union.to_dict()
        restored = Operation.from_dict(data)
        assert isinstance(restored, Union)
        assert len(restored.inputs) == 2


class TestPivot:
    """Tests for Pivot operation."""

    def test_construction_succeeds(self):
        """Pivot with valid arguments constructs successfully."""
        source = Source(source_id="data.csv")
        pivot = Pivot(index="date", columns="category", values="amount", inputs=[source])
        assert pivot.index == ["date"]
        assert pivot.columns == "category"
        assert pivot.values == "amount"

    def test_construction_without_input_fails(self):
        """Pivot without input raises ValueError."""
        with pytest.raises(ValueError, match="must have exactly one input"):
            Pivot(index="date", columns="category", values="amount")

    def test_construction_without_index_fails(self):
        """Pivot without index raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must specify index"):
            Pivot(index="", columns="category", values="amount", inputs=[source])

    def test_construction_without_columns_fails(self):
        """Pivot without columns raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must specify columns"):
            Pivot(index="date", columns="", values="amount", inputs=[source])

    def test_construction_without_values_fails(self):
        """Pivot without values raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must specify values"):
            Pivot(index="date", columns="category", values="", inputs=[source])

    def test_to_dict(self):
        """to_dict() returns correct structure."""
        source = Source(source_id="data.csv")
        pivot = Pivot(index="date", columns="category", values="amount", inputs=[source])
        data = pivot.to_dict()
        assert data['type'] == 'pivot'
        assert data['index'] == ["date"]
        assert data['columns'] == "category"
        assert data['values'] == "amount"

    def test_round_trip(self):
        """from_dict(to_dict()) produces equivalent operation."""
        source = Source(source_id="data.csv")
        pivot = Pivot(index=["date"], columns="category", values="amount", inputs=[source])
        data = pivot.to_dict()
        restored = Operation.from_dict(data)
        assert isinstance(restored, Pivot)
        assert restored.index == pivot.index
        assert restored.columns == pivot.columns
        assert restored.values == pivot.values


class TestMelt:
    """Tests for Melt operation."""

    def test_construction_succeeds(self):
        """Melt with valid arguments constructs successfully."""
        source = Source(source_id="data.csv")
        melt = Melt(id_vars=["date"], value_vars=["a", "b", "c"], inputs=[source])
        assert melt.id_vars == ["date"]
        assert melt.value_vars == ["a", "b", "c"]

    def test_construction_without_input_fails(self):
        """Melt without input raises ValueError."""
        with pytest.raises(ValueError, match="must have exactly one input"):
            Melt(id_vars=["date"])

    def test_construction_without_id_vars_fails(self):
        """Melt without id_vars raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must specify id_vars"):
            Melt(id_vars=[], inputs=[source])

    def test_to_dict(self):
        """to_dict() returns correct structure."""
        source = Source(source_id="data.csv")
        melt = Melt(
            id_vars=["date"],
            value_vars=["a", "b"],
            var_name="metric",
            value_name="val",
            inputs=[source]
        )
        data = melt.to_dict()
        assert data['type'] == 'melt'
        assert data['id_vars'] == ["date"]
        assert data['value_vars'] == ["a", "b"]
        assert data['var_name'] == "metric"
        assert data['value_name'] == "val"

    def test_round_trip(self):
        """from_dict(to_dict()) produces equivalent operation."""
        source = Source(source_id="data.csv")
        melt = Melt(id_vars=["date"], value_vars=["a", "b"], inputs=[source])
        data = melt.to_dict()
        restored = Operation.from_dict(data)
        assert isinstance(restored, Melt)
        assert restored.id_vars == melt.id_vars
        assert restored.value_vars == melt.value_vars


class TestWindow:
    """Tests for Window operation."""

    def test_construction_succeeds(self):
        """Window with valid arguments constructs successfully."""
        source = Source(source_id="data.csv")
        window = Window(
            function="rank",
            output_column="rank",
            partition_by=["category"],
            order_by=[("amount", "desc")],
            inputs=[source]
        )
        assert window.function == "rank"
        assert window.output_column == "rank"
        assert window.partition_by == ["category"]
        assert window.order_by == [("amount", "desc")]

    def test_construction_without_input_fails(self):
        """Window without input raises ValueError."""
        with pytest.raises(ValueError, match="must have exactly one input"):
            Window(function="rank", output_column="rank")

    def test_construction_without_function_fails(self):
        """Window without function raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must specify a function"):
            Window(function="", output_column="rank", inputs=[source])

    def test_construction_without_output_column_fails(self):
        """Window without output column raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="must specify an output column"):
            Window(function="rank", output_column="", inputs=[source])

    def test_partition_keys_order_keys_frame_captured(self):
        """Partition keys, order keys, and frame spec are all captured."""
        source = Source(source_id="data.csv")
        window = Window(
            function="sum",
            input_column="amount",
            output_column="running_total",
            partition_by=["category"],
            order_by=[("date", "asc")],
            frame="rows between unbounded preceding and current row",
            inputs=[source]
        )
        assert window.partition_by == ["category"]
        assert window.order_by == [("date", "asc")]
        assert window.frame == "rows between unbounded preceding and current row"

    def test_invalid_order_direction_fails(self):
        """Window with invalid order direction raises ValueError."""
        source = Source(source_id="data.csv")
        with pytest.raises(ValueError, match="direction must be 'asc' or 'desc'"):
            Window(
                function="rank",
                output_column="rank",
                order_by=[("amount", "invalid")],
                inputs=[source]
            )

    def test_to_dict(self):
        """to_dict() returns correct structure."""
        source = Source(source_id="data.csv")
        window = Window(
            function="rank",
            output_column="rank",
            partition_by=["category"],
            order_by=[("amount", "desc")],
            inputs=[source]
        )
        data = window.to_dict()
        assert data['type'] == 'window'
        assert data['function'] == "rank"
        assert data['output_column'] == "rank"
        assert data['partition_by'] == ["category"]
        assert data['order_by'] == [["amount", "desc"]]

    def test_round_trip(self):
        """from_dict(to_dict()) produces equivalent operation."""
        source = Source(source_id="data.csv")
        window = Window(
            function="rank",
            output_column="rank",
            partition_by=["category"],
            order_by=[("amount", "desc")],
            inputs=[source]
        )
        data = window.to_dict()
        restored = Operation.from_dict(data)
        assert isinstance(restored, Window)
        assert restored.function == window.function
        assert restored.output_column == window.output_column
        assert restored.partition_by == window.partition_by
        assert restored.order_by == window.order_by


class TestLogicalPlan:
    """Tests for LogicalPlan class."""

    def test_construction_succeeds(self):
        """LogicalPlan with valid root constructs successfully."""
        source = Source(source_id="data.csv")
        plan = LogicalPlan(source)
        assert plan.root == source

    def test_construction_with_non_operation_fails(self):
        """LogicalPlan with non-Operation root raises TypeError."""
        with pytest.raises(TypeError, match="must be an Operation"):
            LogicalPlan("not an operation")

    def test_single_node_plan_explain(self):
        """Single-node plan (Source) produces output mentioning the source."""
        source = Source(source_id="data.csv", schema=["a", "b", "c"])
        plan = LogicalPlan(source)
        explanation = plan.explain()
        assert "Source" in explanation
        assert "data.csv" in explanation

    def test_multi_step_plan_explain(self):
        """Multi-step plan produces output listing each operation."""
        source = Source(source_id="data.csv", schema=["a", "b", "c"])
        filtered = Filter(predicate="a > 10", inputs=[source])
        selected = Select(columns=["a", "b"], inputs=[filtered])
        plan = LogicalPlan(selected)
        explanation = plan.explain()

        # Check that all operations appear
        assert "Source" in explanation
        assert "Filter" in explanation
        assert "Select" in explanation

        # Check operation details
        assert "data.csv" in explanation
        assert "a > 10" in explanation
        assert "['a', 'b']" in explanation or "columns=" in explanation

    def test_explain_includes_operation_details(self):
        """Explain output includes operation-specific details."""
        source = Source(source_id="data.csv")

        # Test Filter details
        filtered = Filter(predicate="age > 25", inputs=[source])
        plan = LogicalPlan(filtered)
        explanation = plan.explain()
        assert "age > 25" in explanation

        # Test Join details
        left = Source(source_id="left.csv")
        right = Source(source_id="right.csv")
        joined = Join(left_on="id", right_on="user_id", join_type="left", inputs=[left, right])
        plan = LogicalPlan(joined)
        explanation = plan.explain()
        assert "id" in explanation
        assert "user_id" in explanation
        assert "left" in explanation

    def test_to_dict(self):
        """to_dict() returns the root operation dict directly."""
        source = Source(source_id="data.csv")
        filtered = Filter(predicate="a > 10", inputs=[source])
        plan = LogicalPlan(filtered)
        data = plan.to_dict()
        assert data['type'] == 'filter'

    def test_from_dict(self):
        """from_dict() reconstructs the plan correctly."""
        source = Source(source_id="data.csv")
        filtered = Filter(predicate="a > 10", inputs=[source])
        plan = LogicalPlan(filtered)
        data = plan.to_dict()
        restored = LogicalPlan.from_dict(data)
        assert isinstance(restored.root, Filter)
        assert restored.root.predicate == "a > 10"

    def test_round_trip(self):
        """from_dict(to_dict()) produces equivalent plan."""
        source = Source(source_id="data.csv", schema=["a", "b", "c"])
        filtered = Filter(predicate="a > 10", inputs=[source])
        selected = Select(columns=["a", "b"], inputs=[filtered])
        plan = LogicalPlan(selected)

        data = plan.to_dict()
        restored = LogicalPlan.from_dict(data)

        assert isinstance(restored.root, Select)
        assert restored.root.columns == ["a", "b"]
        assert isinstance(restored.root.inputs[0], Filter)
        assert restored.root.inputs[0].predicate == "a > 10"
        assert isinstance(restored.root.inputs[0].inputs[0], Source)

    def test_copy(self):
        """copy() creates a new plan with the same root."""
        source = Source(source_id="data.csv")
        plan = LogicalPlan(source)
        copied = plan.copy()
        assert copied is not plan
        assert copied.root is plan.root

    def test_complex_plan_explain(self):
        """Complex plan with multiple operations explains correctly."""
        # Build: Source -> Filter -> GroupBy -> Sort -> Limit
        source = Source(source_id="sales.csv", schema=["date", "category", "amount"])
        filtered = Filter(predicate="amount > 0", inputs=[source])
        grouped = GroupBy(
            keys=["category"],
            aggregations=[("total", "sum", "amount")],
            inputs=[filtered]
        )
        sorted_op = Sort(keys=[("total", "desc")], inputs=[grouped])
        limited = Limit(count=10, end="head", inputs=[sorted_op])
        plan = LogicalPlan(limited)

        explanation = plan.explain()

        # Verify all operations appear
        assert "Source" in explanation
        assert "Filter" in explanation
        assert "GroupBy" in explanation
        assert "Sort" in explanation
        assert "Limit" in explanation

        # Verify details
        assert "sales.csv" in explanation
        assert "amount > 0" in explanation
        assert "category" in explanation
        assert "10" in explanation


class TestOperationInputs:
    """Test that inputs list is always a list with correct arity."""

    def test_source_has_zero_inputs(self):
        """Source has zero inputs."""
        source = Source(source_id="data.csv")
        assert source.inputs == []

    def test_unary_operations_have_one_input(self):
        """Unary operations have exactly one input."""
        source = Source(source_id="data.csv")

        select = Select(columns=["a"], inputs=[source])
        assert len(select.inputs) == 1

        filt = Filter(predicate="a > 0", inputs=[source])
        assert len(filt.inputs) == 1

        sort = Sort(keys=[("a", "asc")], inputs=[source])
        assert len(sort.inputs) == 1

    def test_binary_operations_have_two_inputs(self):
        """Binary operations have exactly two inputs."""
        left = Source(source_id="left.csv")
        right = Source(source_id="right.csv")

        join = Join(left_on="id", right_on="user_id", inputs=[left, right])
        assert len(join.inputs) == 2

        union = Union(inputs=[left, right])
        assert len(union.inputs) == 2
