"""
Demo script showing the core module functionality.

This script demonstrates:
1. Creating fornero DataFrames
2. Chaining operations
3. Plan tracking
4. Dual-mode execution (pandas + plan building)
"""

import fornero as pd
from fornero import DataFrame

# Example 1: Basic DataFrame creation and operation tracking
print("=" * 60)
print("Example 1: Basic DataFrame with plan tracking")
print("=" * 60)

df = DataFrame({
    'name': ['Alice', 'Bob', 'Charlie', 'David'],
    'age': [25, 30, 35, 28],
    'salary': [50000, 60000, 70000, 55000]
})

print("\nOriginal DataFrame:")
print(df)
print("\nPlan:")
print(df._plan.explain())

# Example 2: Filtering
print("\n" + "=" * 60)
print("Example 2: Filtering")
print("=" * 60)

filtered = df[df['age'] > 28]
print("\nFiltered DataFrame (age > 28):")
print(filtered)
print("\nPlan:")
print(filtered._plan.explain())

# Example 3: Column selection
print("\n" + "=" * 60)
print("Example 3: Column selection")
print("=" * 60)

selected = df[['name', 'salary']]
print("\nSelected columns:")
print(selected)
print("\nPlan:")
print(selected._plan.explain())

# Example 4: Chaining operations
print("\n" + "=" * 60)
print("Example 4: Chaining operations")
print("=" * 60)

result = df[df['age'] > 25].sort_values('salary', ascending=False).head(2)
print("\nChained operations (filter -> sort -> head):")
print(result)
print("\nPlan:")
print(result._plan.explain())

# Example 5: GroupBy
print("\n" + "=" * 60)
print("Example 5: GroupBy aggregation")
print("=" * 60)

df2 = DataFrame({
    'category': ['A', 'B', 'A', 'B', 'A'],
    'amount': [100, 200, 150, 250, 120]
})

grouped = df2.groupby('category').sum()
print("\nGrouped by category:")
print(grouped)
print("\nPlan:")
print(grouped._plan.explain())

# Example 6: Merge/Join
print("\n" + "=" * 60)
print("Example 6: Merge/Join")
print("=" * 60)

left = DataFrame({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']})
right = DataFrame({'id': [1, 2, 4], 'city': ['NYC', 'LA', 'SF']})

merged = left.merge(right, on='id', how='left')
print("\nMerged DataFrame:")
print(merged)
print("\nPlan:")
print(merged._plan.explain())

# Example 7: Using fornero module-level functions
print("\n" + "=" * 60)
print("Example 7: Module-level functions")
print("=" * 60)

df3 = pd.DataFrame({'x': [1, 2, 3]})
df4 = pd.DataFrame({'x': [4, 5, 6]})

concatenated = pd.concat([df3, df4])
print("\nConcatenated DataFrame:")
print(concatenated)
print("\nPlan:")
print(concatenated._plan.explain())

# Example 8: Complex chain
print("\n" + "=" * 60)
print("Example 8: Complex operation chain")
print("=" * 60)

sales_df = DataFrame({
    'date': ['2023-01-01', '2023-01-02', '2023-01-01', '2023-01-02'],
    'product': ['A', 'B', 'A', 'B'],
    'quantity': [10, 20, 15, 25],
    'price': [100, 150, 100, 150]
})

# Chain: filter -> assign -> groupby -> sort
analysis = sales_df[sales_df['quantity'] > 10] \
    .assign(revenue=lambda x: 100) \
    .groupby('product').sum() \
    .sort_values('quantity', ascending=False)

print("\nAnalysis result:")
print(analysis)
print("\nPlan:")
print(analysis._plan.explain())

print("\n" + "=" * 60)
print("Demo completed successfully!")
print("=" * 60)
