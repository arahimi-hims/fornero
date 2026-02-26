"""
Unit tests for the core module (Tasks 2-4).

These tests verify:
1. fornero.DataFrame construction and plan attachment
2. fornero.__init__ re-exports work correctly
3. Operation tracer captures operations and builds correct plans
4. Dual-mode invariant: operations execute in pandas AND record in plan
5. Plan propagation through _metadata

No external dependencies (no API calls, no real filesystem).
"""

import pandas as pd
import fornero
from fornero import DataFrame, LogicalPlan
from fornero.algebra import Source, Select, Filter, Sort, Limit, GroupBy, Join, WithColumn, Union


class TestDataFrameConstruction:
    """Tests for Task 2: fornero.DataFrame subclass construction."""

    def test_construction_from_dict_attaches_source_plan(self):
        """Constructing DataFrame from dict attaches LogicalPlan with Source root."""
        df = DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})

        assert hasattr(df, '_plan')
        assert isinstance(df._plan, LogicalPlan)
        assert isinstance(df._plan.root, Source)
        assert df._plan.root.schema == ['a', 'b']

    def test_construction_from_pandas_df_preserves_data_and_attaches_plan(self):
        """Constructing from pandas DataFrame preserves data and attaches fresh plan."""
        pd_df = pd.DataFrame({'x': [10, 20], 'y': [30, 40]})
        fornero_df = DataFrame(pd_df)

        # Data is preserved
        assert list(fornero_df.columns) == ['x', 'y']
        assert len(fornero_df) == 2
        assert fornero_df['x'].tolist() == [10, 20]

        # Plan is attached
        assert hasattr(fornero_df, '_plan')
        assert isinstance(fornero_df._plan, LogicalPlan)
        assert isinstance(fornero_df._plan.root, Source)

    def test_plan_survives_slicing(self):
        """_plan attribute survives pandas operations via _metadata propagation."""
        df = DataFrame({'a': [1, 2, 3, 4], 'b': [5, 6, 7, 8]})
        original_plan_root_type = type(df._plan.root)

        # Slicing should preserve the plan
        sliced = df.iloc[1:3]

        assert hasattr(sliced, '_plan')
        assert isinstance(sliced._plan, LogicalPlan)
        # The plan root type should be preserved (Source in this case)
        assert isinstance(sliced._plan.root, original_plan_root_type)

    def test_plan_survives_copying(self):
        """_plan attribute survives copy operations."""
        df = DataFrame({'a': [1, 2, 3]})
        copied = df.copy()

        assert hasattr(copied, '_plan')
        assert isinstance(copied._plan, LogicalPlan)

    def test_to_spreadsheet_plan_exists(self):
        """to_spreadsheet_plan() method exists and is callable."""
        df = DataFrame({'a': [1, 2, 3]})

        # Method should exist
        assert hasattr(df, 'to_spreadsheet_plan')
        assert callable(df.to_spreadsheet_plan)

        # Note: We don't test execution here since translator may not be implemented
        # The test just verifies the API exists


class TestForneroInitReexports:
    """Tests for Task 3: fornero.__init__ re-exports."""

    def test_import_fornero_dataframe_produces_fornero_dataframe(self):
        """import fornero as pd; pd.DataFrame(...) produces fornero.DataFrame."""
        df = fornero.DataFrame({'a': [1, 2, 3]})

        assert isinstance(df, DataFrame)
        assert not isinstance(df, pd.DataFrame) or isinstance(df, DataFrame)
        assert hasattr(df, '_plan')

    def test_read_csv_returns_fornero_dataframe_with_source(self):
        """fornero.read_csv (stubbed) returns fornero.DataFrame with Source node."""
        # Create a temporary CSV in memory using StringIO
        import io
        csv_data = "a,b,c\n1,2,3\n4,5,6"
        csv_buffer = io.StringIO(csv_data)

        df = fornero.read_csv(csv_buffer)

        assert isinstance(df, DataFrame)
        assert hasattr(df, '_plan')
        assert isinstance(df._plan.root, Source)
        assert df._plan.root.schema == ['a', 'b', 'c']

    def test_merge_returns_tracked_frame(self):
        """fornero.merge returns a fornero.DataFrame with Join node."""
        left = DataFrame({'id': [1, 2], 'x': [10, 20]})
        right = DataFrame({'id': [1, 2], 'y': [30, 40]})

        result = fornero.merge(left, right, on='id')

        assert isinstance(result, DataFrame)
        assert hasattr(result, '_plan')
        assert isinstance(result._plan.root, Join)

    def test_concat_returns_tracked_frame(self):
        """fornero.concat returns a fornero.DataFrame with appropriate tracking."""
        df1 = DataFrame({'a': [1, 2]})
        df2 = DataFrame({'a': [3, 4]})

        result = fornero.concat([df1, df2])

        assert isinstance(result, DataFrame)
        assert hasattr(result, '_plan')
        # For two DataFrames concatenated vertically, should have Union node
        assert isinstance(result._plan.root, Union)


class TestOperationTracer:
    """Tests for Task 4: Operation tracer."""

    def test_filter_appends_filter_node(self):
        """Filter operation appends Filter node to plan."""
        df = DataFrame({'age': [20, 30, 40]})

        # Apply filter via boolean indexing
        result = df[df['age'] > 25]

        assert isinstance(result._plan.root, Filter)
        assert len(result._plan.root.inputs) == 1
        assert isinstance(result._plan.root.inputs[0], Source)

    def test_select_appends_select_node(self):
        """Column selection appends Select node to plan."""
        df = DataFrame({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]})

        result = df[['a', 'b']]

        assert isinstance(result._plan.root, Select)
        assert result._plan.root.columns == ['a', 'b']
        assert len(result._plan.root.inputs) == 1

    def test_sort_appends_sort_node(self):
        """Sort operation appends Sort node to plan."""
        df = DataFrame({'x': [3, 1, 2]})

        result = df.sort_values('x')

        assert isinstance(result._plan.root, Sort)
        assert result._plan.root.keys == [('x', 'asc')]

    def test_sort_descending(self):
        """Sort with descending direction captures direction correctly."""
        df = DataFrame({'x': [1, 2, 3]})

        result = df.sort_values('x', ascending=False)

        assert isinstance(result._plan.root, Sort)
        assert result._plan.root.keys == [('x', 'desc')]

    def test_head_appends_limit_node(self):
        """head() appends Limit node with end='head'."""
        df = DataFrame({'a': [1, 2, 3, 4, 5]})

        result = df.head(3)

        assert isinstance(result._plan.root, Limit)
        assert result._plan.root.count == 3
        assert result._plan.root.end == 'head'

    def test_tail_appends_limit_node(self):
        """tail() appends Limit node with end='tail'."""
        df = DataFrame({'a': [1, 2, 3, 4, 5]})

        result = df.tail(2)

        assert isinstance(result._plan.root, Limit)
        assert result._plan.root.count == 2
        assert result._plan.root.end == 'tail'

    def test_groupby_agg_appends_groupby_node(self):
        """groupby().agg() appends GroupBy node with aggregations."""
        df = DataFrame({'category': ['A', 'B', 'A'], 'amount': [10, 20, 30]})

        result = df.groupby('category').agg({'amount': 'sum'})

        assert isinstance(result._plan.root, GroupBy)
        assert result._plan.root.keys == ['category']
        assert len(result._plan.root.aggregations) > 0

    def test_merge_appends_join_node(self):
        """merge() appends Join node to plan."""
        left = DataFrame({'id': [1, 2], 'x': [10, 20]})
        right = DataFrame({'id': [1, 2], 'y': [30, 40]})

        result = left.merge(right, on='id', how='left')

        assert isinstance(result._plan.root, Join)
        assert result._plan.root.join_type == 'left'

    def test_assign_appends_withcolumn_node(self):
        """assign() appends WithColumn node(s) to plan."""
        df = DataFrame({'a': [1, 2], 'b': [3, 4]})

        result = df.assign(c=5)

        assert isinstance(result._plan.root, WithColumn)
        assert result._plan.root.column == 'c'

    def test_chaining_operations_produces_nested_plan(self):
        """Chaining operations produces plan with nested nodes in correct order."""
        df = DataFrame({'a': [1, 2, 3, 4], 'b': [5, 6, 7, 8]})

        # Chain: filter -> select
        result = df[df['a'] > 2][['a', 'b']]

        # Root should be Select
        assert isinstance(result._plan.root, Select)
        # Select's input should be Filter
        assert isinstance(result._plan.root.inputs[0], Filter)
        # Filter's input should be Source
        assert isinstance(result._plan.root.inputs[0].inputs[0], Source)

    def test_complex_chain_preserves_order(self):
        """Complex chain preserves operation order in plan."""
        df = DataFrame({'x': [3, 1, 4, 2], 'y': [10, 20, 30, 40]})

        # Chain: filter -> sort -> head
        result = df[df['x'] > 1].sort_values('x').head(2)

        # Root should be Limit (from head)
        assert isinstance(result._plan.root, Limit)
        # Next should be Sort
        assert isinstance(result._plan.root.inputs[0], Sort)
        # Next should be Filter
        assert isinstance(result._plan.root.inputs[0].inputs[0], Filter)
        # Base should be Source
        assert isinstance(result._plan.root.inputs[0].inputs[0].inputs[0], Source)

    def test_tracer_captures_column_names(self):
        """Tracer captures column names faithfully."""
        df = DataFrame({'name': ['Alice', 'Bob'], 'age': [25, 30]})

        result = df[['name']]

        assert isinstance(result._plan.root, Select)
        assert result._plan.root.columns == ['name']

    def test_tracer_captures_sort_directions(self):
        """Tracer captures sort directions for multiple columns."""
        df = DataFrame({'a': [1, 2], 'b': [3, 4]})

        result = df.sort_values(['a', 'b'], ascending=[True, False])

        assert isinstance(result._plan.root, Sort)
        assert result._plan.root.keys == [('a', 'asc'), ('b', 'desc')]

    def test_tracer_captures_join_keys(self):
        """Tracer captures join keys correctly."""
        left = DataFrame({'id': [1, 2], 'x': [10, 20]})
        right = DataFrame({'user_id': [1, 2], 'y': [30, 40]})

        result = left.merge(right, left_on='id', right_on='user_id')

        assert isinstance(result._plan.root, Join)
        assert result._plan.root.left_on == ['id']
        assert result._plan.root.right_on == ['user_id']

    def test_operations_execute_eagerly_in_pandas(self):
        """Operations execute eagerly in pandas (dual-mode invariant)."""
        df = DataFrame({'a': [1, 2, 3, 4], 'b': [5, 6, 7, 8]})

        # Filter operation
        result = df[df['a'] > 2]

        # Verify pandas execution
        assert len(result) == 2  # Only rows where a > 2
        assert result['a'].tolist() == [3, 4]

        # Verify plan tracking
        assert isinstance(result._plan.root, Filter)

    def test_select_executes_and_tracks(self):
        """Select operation executes in pandas and tracks in plan."""
        df = DataFrame({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]})

        result = df[['a', 'c']]

        # Pandas execution
        assert list(result.columns) == ['a', 'c']
        assert 'b' not in result.columns

        # Plan tracking
        assert isinstance(result._plan.root, Select)
        assert result._plan.root.columns == ['a', 'c']

    def test_sort_executes_and_tracks(self):
        """Sort operation executes in pandas and tracks in plan."""
        df = DataFrame({'x': [3, 1, 2]})

        result = df.sort_values('x')

        # Pandas execution
        assert result['x'].tolist() == [1, 2, 3]

        # Plan tracking
        assert isinstance(result._plan.root, Sort)

    def test_head_executes_and_tracks(self):
        """head() executes in pandas and tracks in plan."""
        df = DataFrame({'a': [1, 2, 3, 4, 5]})

        result = df.head(3)

        # Pandas execution
        assert len(result) == 3
        assert result['a'].tolist() == [1, 2, 3]

        # Plan tracking
        assert isinstance(result._plan.root, Limit)
        assert result._plan.root.count == 3


class TestGroupByTracking:
    """Tests for GroupBy operation tracking."""

    def test_groupby_sum_tracks_aggregation(self):
        """groupby().sum() tracks sum aggregation."""
        df = DataFrame({'category': ['A', 'B', 'A'], 'amount': [10, 20, 30]})

        result = df.groupby('category').sum()

        assert isinstance(result._plan.root, GroupBy)
        assert result._plan.root.keys == ['category']

    def test_groupby_mean_tracks_aggregation(self):
        """groupby().mean() tracks mean aggregation."""
        df = DataFrame({'category': ['A', 'B', 'A'], 'value': [10, 20, 30]})

        result = df.groupby('category').mean()

        assert isinstance(result._plan.root, GroupBy)
        assert result._plan.root.keys == ['category']

    def test_groupby_multiple_keys(self):
        """groupby() with multiple keys captures all keys."""
        df = DataFrame({'a': [1, 1, 2], 'b': ['x', 'y', 'x'], 'c': [10, 20, 30]})

        result = df.groupby(['a', 'b']).sum()

        assert isinstance(result._plan.root, GroupBy)
        assert result._plan.root.keys == ['a', 'b']


class TestMergeTracking:
    """Tests for merge/join operation tracking."""

    def test_merge_inner_join(self):
        """merge with how='inner' tracks correctly."""
        left = DataFrame({'id': [1, 2], 'x': [10, 20]})
        right = DataFrame({'id': [1, 2], 'y': [30, 40]})

        result = left.merge(right, on='id', how='inner')

        assert isinstance(result._plan.root, Join)
        assert result._plan.root.join_type == 'inner'

    def test_merge_left_join(self):
        """merge with how='left' tracks correctly."""
        left = DataFrame({'id': [1, 2], 'x': [10, 20]})
        right = DataFrame({'id': [1, 2], 'y': [30, 40]})

        result = left.merge(right, on='id', how='left')

        assert isinstance(result._plan.root, Join)
        assert result._plan.root.join_type == 'left'

    def test_merge_with_pandas_dataframe(self):
        """merge with regular pandas DataFrame creates Source for right side."""
        left = DataFrame({'id': [1, 2], 'x': [10, 20]})
        right = pd.DataFrame({'id': [1, 2], 'y': [30, 40]})

        result = left.merge(right, on='id')

        assert isinstance(result._plan.root, Join)
        # Right input should be a Source node
        assert isinstance(result._plan.root.inputs[1], Source)


class TestAssignTracking:
    """Tests for assign operation tracking."""

    def test_assign_single_column(self):
        """assign with single column tracks WithColumn."""
        df = DataFrame({'a': [1, 2]})

        result = df.assign(b=10)

        assert isinstance(result._plan.root, WithColumn)
        assert result._plan.root.column == 'b'

    def test_assign_multiple_columns_chains_operations(self):
        """assign with multiple columns chains WithColumn operations."""
        df = DataFrame({'a': [1, 2]})

        result = df.assign(b=10, c=20)

        # Should have nested WithColumn operations
        assert isinstance(result._plan.root, WithColumn)
        # The root could be either 'b' or 'c' depending on dict ordering
        assert result._plan.root.column in ['b', 'c']


class TestConcatTracking:
    """Tests for concat operation tracking."""

    def test_concat_two_dataframes_creates_union(self):
        """concat of two DataFrames creates Union node."""
        df1 = DataFrame({'a': [1, 2]})
        df2 = DataFrame({'a': [3, 4]})

        result = fornero.concat([df1, df2])

        assert isinstance(result._plan.root, Union)
        assert len(result._plan.root.inputs) == 2

    def test_concat_preserves_data(self):
        """concat executes correctly in pandas."""
        df1 = DataFrame({'a': [1, 2]})
        df2 = DataFrame({'a': [3, 4]})

        result = fornero.concat([df1, df2])

        # Should have 4 rows
        assert len(result) == 4
        assert result['a'].tolist() == [1, 2, 3, 4]


class TestPlanExplain:
    """Tests for plan explanation (verifying plan structure)."""

    def test_plan_explain_shows_operations(self):
        """Plan explanation shows operations in readable format."""
        df = DataFrame({'a': [1, 2, 3]})
        result = df[df['a'] > 1][['a']]

        explanation = result._plan.explain()

        # Should contain operation names
        assert 'Filter' in explanation or 'filter' in explanation
        assert 'Select' in explanation or 'select' in explanation

    def test_plan_explain_includes_source(self):
        """Plan explanation includes source information."""
        df = DataFrame({'a': [1, 2, 3]})

        explanation = df._plan.explain()

        assert 'Source' in explanation or 'source' in explanation
