"""
Unit tests for the utils module (Tasks 16-17).

These tests verify:
Task 16 - Visualization:
1. visualize(plan) for a single-node plan returns a string containing the node type
2. visualize(plan) for a multi-step plan returns a tree-shaped string with indentation
3. The output is deterministic (same plan → same string)

Task 17 - Serialization:
1. serialize(plan) returns a JSON-serializable dict
2. deserialize(serialize(plan)) produces a plan structurally equal to the original (round-trip)
3. Serialized output includes a version key for forward compatibility
4. Serializing a plan with all operation types succeeds without error
5. Deserializing a dict with an unknown operation type raises a clear error
6. Deserializing a dict with a missing required field raises a clear error
"""

import pytest
import json
from fornero.algebra import (
    LogicalPlan,
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
from fornero.utils import (
    visualize,
    serialize,
    deserialize,
    to_json,
    from_json,
    SERIALIZATION_VERSION,
)


class TestVisualization:
    """Tests for Task 16: Plan visualization."""

    def test_single_node_plan_contains_node_type(self):
        """visualize(plan) for a single-node plan returns a string containing the node type."""
        source = Source(source_id="data.csv", schema=["a", "b"])
        plan = LogicalPlan(source)

        result = visualize(plan)

        assert isinstance(result, str)
        assert "Source" in result
        assert "data.csv" in result

    def test_multi_step_plan_shows_tree_structure(self):
        """visualize(plan) for a multi-step plan returns a tree-shaped string with indentation."""
        source = Source(source_id="data.csv", schema=["a", "b", "c"])
        filtered = Filter(predicate="a > 10", inputs=[source])
        selected = Select(columns=["a", "b"], inputs=[filtered])
        plan = LogicalPlan(selected)

        result = visualize(plan)

        # Should contain all operation types
        assert "Select" in result
        assert "Filter" in result
        assert "Source" in result

        # Should show hierarchy with connectors
        assert "└──" in result or "├──" in result

        # Select should come before its input operations
        select_pos = result.index("Select")
        filter_pos = result.index("Filter")
        source_pos = result.index("Source")
        assert select_pos < filter_pos < source_pos

    def test_output_is_deterministic(self):
        """The output is deterministic (same plan → same string)."""
        source = Source(source_id="data.csv", schema=["a", "b"])
        filtered = Filter(predicate="a > 10", inputs=[source])
        plan = LogicalPlan(filtered)

        result1 = visualize(plan)
        result2 = visualize(plan)

        assert result1 == result2

    def test_join_operation_shows_both_inputs(self):
        """Join operations should show both input branches."""
        left = Source(source_id="left.csv", schema=["id", "value"])
        right = Source(source_id="right.csv", schema=["id", "label"])
        joined = Join(left_on=["id"], right_on=["id"], join_type="inner", inputs=[left, right])
        plan = LogicalPlan(joined)

        result = visualize(plan)

        assert "Join" in result
        assert "left.csv" in result
        assert "right.csv" in result
        assert result.count("Source") == 2

    def test_complex_plan_with_multiple_branches(self):
        """Complex plan with multiple operation types shows proper tree structure."""
        source1 = Source(source_id="data1.csv", schema=["a", "b"])
        source2 = Source(source_id="data2.csv", schema=["c", "d"])
        filtered1 = Filter(predicate="a > 5", inputs=[source1])
        union = Union(inputs=[filtered1, source2])
        sorted_plan = Sort(keys=[("a", "asc")], inputs=[union])
        plan = LogicalPlan(sorted_plan)

        result = visualize(plan)

        # All operations should be present
        assert "Sort" in result
        assert "Union" in result
        assert "Filter" in result
        assert result.count("Source") == 2

    def test_visualize_with_groupby(self):
        """GroupBy operation shows keys and aggregations."""
        source = Source(source_id="data.csv", schema=["category", "amount"])
        grouped = GroupBy(
            keys=["category"],
            aggregations=[("total", "sum", "amount")],
            inputs=[source]
        )
        plan = LogicalPlan(grouped)

        result = visualize(plan)

        assert "GroupBy" in result
        assert "category" in result

    def test_visualize_with_window(self):
        """Window operation shows function and partitioning."""
        source = Source(source_id="data.csv", schema=["category", "value"])
        windowed = Window(
            function="rank",
            output_column="rank",
            partition_by=["category"],
            order_by=[("value", "desc")],
            inputs=[source]
        )
        plan = LogicalPlan(windowed)

        result = visualize(plan)

        assert "Window" in result
        assert "rank" in result

    def test_visualize_invalid_input(self):
        """visualize with non-LogicalPlan raises TypeError."""
        with pytest.raises(TypeError, match="Expected LogicalPlan"):
            visualize("not a plan")


class TestSerialization:
    """Tests for Task 17: Plan serialization."""

    def test_serialize_returns_json_serializable_dict(self):
        """serialize(plan) returns a JSON-serializable dict."""
        source = Source(source_id="data.csv", schema=["a", "b"])
        plan = LogicalPlan(source)

        result = serialize(plan)

        assert isinstance(result, dict)
        # Verify it's JSON-serializable
        json_str = json.dumps(result)
        assert isinstance(json_str, str)

    def test_round_trip_preserves_structure(self):
        """deserialize(serialize(plan)) produces a plan structurally equal to the original."""
        source = Source(source_id="data.csv", schema=["a", "b", "c"])
        filtered = Filter(predicate="a > 10", inputs=[source])
        selected = Select(columns=["a", "b"], inputs=[filtered])
        original_plan = LogicalPlan(selected)

        # Serialize and deserialize
        serialized = serialize(original_plan)
        restored_plan = deserialize(serialized)

        # Check structure is preserved
        assert isinstance(restored_plan, LogicalPlan)
        assert isinstance(restored_plan.root, Select)
        assert restored_plan.root.columns == ["a", "b"]
        assert len(restored_plan.root.inputs) == 1

        filter_op = restored_plan.root.inputs[0]
        assert isinstance(filter_op, Filter)
        assert filter_op.predicate == "a > 10"
        assert len(filter_op.inputs) == 1

        source_op = filter_op.inputs[0]
        assert isinstance(source_op, Source)
        assert source_op.source_id == "data.csv"
        assert source_op.schema == ["a", "b", "c"]

    def test_serialized_output_includes_version(self):
        """Serialized output includes a version key for forward compatibility."""
        source = Source(source_id="data.csv")
        plan = LogicalPlan(source)

        result = serialize(plan)

        assert "version" in result
        assert result["version"] == SERIALIZATION_VERSION
        assert isinstance(result["version"], str)

    def test_serialize_all_operation_types(self):
        """Serializing a plan with all operation types succeeds without error."""
        # Create a complex plan with many operation types
        source = Source(source_id="data.csv", schema=["a", "b", "c"])
        selected = Select(columns=["a", "b"], inputs=[source])
        filtered = Filter(predicate="a > 5", inputs=[selected])
        with_col = WithColumn(column="d", expression="a + b", inputs=[filtered])
        sorted_op = Sort(keys=[("a", "asc")], inputs=[with_col])
        limited = Limit(count=10, end="head", inputs=[sorted_op])
        plan = LogicalPlan(limited)

        # Should not raise any errors
        result = serialize(plan)
        assert isinstance(result, dict)

        # Verify round-trip works
        restored = deserialize(result)
        assert isinstance(restored, LogicalPlan)

    def test_serialize_join_operation(self):
        """Join operation can be serialized and deserialized."""
        left = Source(source_id="left.csv", schema=["id", "value"])
        right = Source(source_id="right.csv", schema=["id", "label"])
        joined = Join(
            left_on=["id"],
            right_on=["id"],
            join_type="left",
            inputs=[left, right]
        )
        plan = LogicalPlan(joined)

        serialized = serialize(plan)
        restored = deserialize(serialized)

        assert isinstance(restored.root, Join)
        assert restored.root.left_on == ["id"]
        assert restored.root.right_on == ["id"]
        assert restored.root.join_type == "left"

    def test_serialize_groupby_operation(self):
        """GroupBy operation can be serialized and deserialized."""
        source = Source(source_id="data.csv", schema=["category", "amount"])
        grouped = GroupBy(
            keys=["category"],
            aggregations=[("total", "sum", "amount"), ("count", "count", "amount")],
            inputs=[source]
        )
        plan = LogicalPlan(grouped)

        serialized = serialize(plan)
        restored = deserialize(serialized)

        assert isinstance(restored.root, GroupBy)
        assert restored.root.keys == ["category"]
        assert len(restored.root.aggregations) == 2
        assert restored.root.aggregations[0] == ("total", "sum", "amount")

    def test_serialize_aggregate_operation(self):
        """Aggregate operation can be serialized and deserialized."""
        source = Source(source_id="data.csv", schema=["value"])
        agg = Aggregate(
            aggregations=[("avg", "mean", "value"), ("total", "sum", "value")],
            inputs=[source]
        )
        plan = LogicalPlan(agg)

        serialized = serialize(plan)
        restored = deserialize(serialized)

        assert isinstance(restored.root, Aggregate)
        assert len(restored.root.aggregations) == 2

    def test_serialize_union_operation(self):
        """Union operation can be serialized and deserialized."""
        source1 = Source(source_id="data1.csv", schema=["a", "b"])
        source2 = Source(source_id="data2.csv", schema=["a", "b"])
        union = Union(inputs=[source1, source2])
        plan = LogicalPlan(union)

        serialized = serialize(plan)
        restored = deserialize(serialized)

        assert isinstance(restored.root, Union)
        assert len(restored.root.inputs) == 2

    def test_serialize_pivot_operation(self):
        """Pivot operation can be serialized and deserialized."""
        source = Source(source_id="data.csv", schema=["date", "category", "value"])
        pivot = Pivot(
            index=["date"],
            columns="category",
            values="value",
            aggfunc="sum",
            inputs=[source]
        )
        plan = LogicalPlan(pivot)

        serialized = serialize(plan)
        restored = deserialize(serialized)

        assert isinstance(restored.root, Pivot)
        assert restored.root.index == ["date"]
        assert restored.root.columns == "category"
        assert restored.root.values == "value"

    def test_serialize_melt_operation(self):
        """Melt operation can be serialized and deserialized."""
        source = Source(source_id="data.csv", schema=["id", "a", "b", "c"])
        melt = Melt(
            id_vars=["id"],
            value_vars=["a", "b", "c"],
            var_name="variable",
            value_name="value",
            inputs=[source]
        )
        plan = LogicalPlan(melt)

        serialized = serialize(plan)
        restored = deserialize(serialized)

        assert isinstance(restored.root, Melt)
        assert restored.root.id_vars == ["id"]
        assert restored.root.value_vars == ["a", "b", "c"]

    def test_serialize_window_operation(self):
        """Window operation can be serialized and deserialized."""
        source = Source(source_id="data.csv", schema=["category", "value"])
        window = Window(
            function="rank",
            output_column="rank",
            partition_by=["category"],
            order_by=[("value", "desc")],
            inputs=[source]
        )
        plan = LogicalPlan(window)

        serialized = serialize(plan)
        restored = deserialize(serialized)

        assert isinstance(restored.root, Window)
        assert restored.root.function == "rank"
        assert restored.root.output_column == "rank"
        assert restored.root.partition_by == ["category"]
        assert restored.root.order_by == [("value", "desc")]

    def test_deserialize_unknown_operation_type_raises_error(self):
        """Deserializing a dict with an unknown operation type raises a clear error."""
        bad_data = {
            "version": SERIALIZATION_VERSION,
            "root": {
                "type": "unknown_operation",
                "inputs": []
            }
        }

        with pytest.raises(ValueError, match="Unknown operation type"):
            deserialize(bad_data)

    def test_deserialize_missing_version_raises_error(self):
        """Deserializing a dict with a missing version field raises a clear error."""
        bad_data = {
            "root": {
                "type": "source",
                "source_id": "data.csv",
                "schema": None,
                "inputs": []
            }
        }

        with pytest.raises(ValueError, match="must have 'version' field"):
            deserialize(bad_data)

    def test_deserialize_missing_root_raises_error(self):
        """Deserializing a dict with a missing root field raises a clear error."""
        bad_data = {
            "version": SERIALIZATION_VERSION
        }

        with pytest.raises(ValueError, match="must have 'root' field"):
            deserialize(bad_data)

    def test_deserialize_invalid_version_raises_error(self):
        """Deserializing a dict with an unsupported version raises a clear error."""
        bad_data = {
            "version": "99.0",
            "root": {
                "type": "source",
                "source_id": "data.csv",
                "schema": None,
                "inputs": []
            }
        }

        with pytest.raises(ValueError, match="Unsupported serialization version"):
            deserialize(bad_data)

    def test_deserialize_missing_operation_field_raises_error(self):
        """Deserializing with a missing required operation field raises a clear error."""
        bad_data = {
            "version": SERIALIZATION_VERSION,
            "root": {
                "type": "filter",
                # Missing 'predicate' field
                "inputs": [
                    {
                        "type": "source",
                        "source_id": "data.csv",
                        "schema": None,
                        "inputs": []
                    }
                ]
            }
        }

        # Filter.__init__ will raise ValueError due to empty predicate
        with pytest.raises(ValueError):
            deserialize(bad_data)

    def test_deserialize_non_dict_raises_error(self):
        """Deserializing a non-dict raises TypeError."""
        with pytest.raises(TypeError, match="Expected dict"):
            deserialize("not a dict")

    def test_serialize_non_plan_raises_error(self):
        """Serializing a non-LogicalPlan raises TypeError."""
        with pytest.raises(TypeError, match="Expected LogicalPlan"):
            serialize("not a plan")

    def test_to_json_produces_valid_json_string(self):
        """to_json produces a valid JSON string."""
        source = Source(source_id="data.csv", schema=["a", "b"])
        plan = LogicalPlan(source)

        json_str = to_json(plan)

        assert isinstance(json_str, str)
        # Should be valid JSON
        parsed = json.loads(json_str)
        assert isinstance(parsed, dict)
        assert "version" in parsed
        assert "root" in parsed

    def test_to_json_with_indent(self):
        """to_json with indent parameter produces formatted JSON."""
        source = Source(source_id="data.csv")
        plan = LogicalPlan(source)

        json_str = to_json(plan, indent=2)

        # Indented JSON should have newlines
        assert "\n" in json_str
        assert isinstance(json_str, str)

    def test_from_json_parses_json_string(self):
        """from_json parses a JSON string and returns a LogicalPlan."""
        source = Source(source_id="data.csv", schema=["a", "b"])
        original_plan = LogicalPlan(source)

        json_str = to_json(original_plan)
        restored_plan = from_json(json_str)

        assert isinstance(restored_plan, LogicalPlan)
        assert isinstance(restored_plan.root, Source)
        assert restored_plan.root.source_id == "data.csv"

    def test_from_json_invalid_json_raises_error(self):
        """from_json with invalid JSON raises ValueError."""
        bad_json = "{ invalid json }"

        with pytest.raises(ValueError, match="Invalid JSON"):
            from_json(bad_json)

    def test_from_json_non_string_raises_error(self):
        """from_json with non-string raises TypeError."""
        with pytest.raises(TypeError, match="Expected str"):
            from_json({"not": "a string"})

    def test_round_trip_with_json(self):
        """Round-trip through JSON string preserves plan structure."""
        source = Source(source_id="data.csv", schema=["a", "b", "c"])
        filtered = Filter(predicate="a > 10", inputs=[source])
        selected = Select(columns=["a", "b"], inputs=[filtered])
        original_plan = LogicalPlan(selected)

        # Serialize to JSON string and back
        json_str = to_json(original_plan)
        restored_plan = from_json(json_str)

        # Verify structure
        assert isinstance(restored_plan.root, Select)
        assert restored_plan.root.columns == ["a", "b"]
        assert isinstance(restored_plan.root.inputs[0], Filter)
        assert isinstance(restored_plan.root.inputs[0].inputs[0], Source)

    def test_serialize_complex_multi_branch_plan(self):
        """Complex plan with multiple branches serializes correctly."""
        # Create a plan with joins, unions, and various operations
        left = Source(source_id="left.csv", schema=["id", "value"])
        right = Source(source_id="right.csv", schema=["id", "label"])
        joined = Join(left_on=["id"], right_on=["id"], join_type="inner", inputs=[left, right])

        source3 = Source(source_id="other.csv", schema=["id", "value", "label"])
        union = Union(inputs=[joined, source3])

        grouped = GroupBy(
            keys=["label"],
            aggregations=[("total", "sum", "value")],
            inputs=[union]
        )
        plan = LogicalPlan(grouped)

        # Serialize and deserialize
        serialized = serialize(plan)
        restored = deserialize(serialized)

        # Verify structure
        assert isinstance(restored.root, GroupBy)
        assert isinstance(restored.root.inputs[0], Union)
        union_node = restored.root.inputs[0]
        assert isinstance(union_node.inputs[0], Join)
        assert isinstance(union_node.inputs[1], Source)
