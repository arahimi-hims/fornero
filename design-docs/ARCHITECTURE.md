# Dataframe-to-Spreadsheet Converter: Implementation Plan

## Project Overview

A compiler that converts dataframe-based programs to Google Sheets spreadsheets by tracing operations, building a logical plan, and translating to spreadsheet formulas.

## Architecture Overview

### High-Level Architecture

Fornero is a drop-in replacement for pandas: users write `import fornero as pd` and use the pandas API as normal. Every DataFrame produced by fornero is automatically tracked — there's no concept of untracked dataframes, no wrapper classes, no opt-in. The module re-exports the pandas API with a `DataFrame` subclass that builds a logical plan behind the scenes. This keeps user code clean and makes adoption trivial: swap one import line and everything is traced.

As operations accumulate, each DataFrame carries a tree of nodes — the **dataframe algebra** — that captures the full lineage of transformations (filters, joins, group-bys, computed columns, etc.) without executing them eagerly. When the user is ready to materialise a spreadsheet, a **translator** walks this tree and converts every dataframe-level operation into equivalent **spreadsheet algebra**: concrete cell ranges, Google Sheets formulas, and sheet layout decisions. The translator also runs an optimisation pass — collapsing redundant steps, choosing between `FILTER`, `QUERY`, or helper columns based on the operation type (never on the data values themselves). The result is an **execution plan**: a complete description of which tabs to create, which cells to populate, and which formulas to write. Finally, a **Google Sheets executor** takes that plan and applies it via the Sheets API in batched calls, producing a live, formula-driven spreadsheet that mirrors the original pandas program.

Operations are dual-mode: they execute eagerly against pandas (so `print(df)` works as expected) and simultaneously record themselves into a logical plan. This means the user gets normal pandas behavior — results, exceptions, debugging — while fornero silently builds a trace of the entire computation. When the user is ready, they call `df.to_spreadsheet_plan()` to trigger translation. At that point we have a complete logical plan that we can optimize (filter pushdown, dead column elimination) and translate to spreadsheet formulas.

```python
import fornero as pd

df = pd.read_csv('data.csv')
result = df.filter(df['age'] > 25).select(['name', 'age'])

plan = result.to_spreadsheet_plan()
```

## Component Breakdown

### Dataframe Algebra (IR Layer)

The dataframe algebra is our core intermediate representation, inspired by Polars' logical plan architecture. Rather than executing operations immediately, we build a tree of operation nodes that represents what the user wants to do. This gives us the flexibility to analyze, optimize, and eventually translate the computation into a different target system (Google Sheets).

Each operation in the tree knows its inputs and carries metadata about what it does. The root of the tree represents the final result, and we can walk backward through parent nodes to understand the full transformation pipeline. This design mirrors how modern query engines work: capture intent first, optimize second, execute third.

The following subsections define each operation in the dataframe algebra. We write $R$ for a relation (a multiset of rows), $\mathcal{S}(R)$ for its schema (the ordered list of column names), $r.c$ for the value of column $c$ in row $r$, and $|R|$ for the number of rows. Row order is significant: every relation carries an implicit positional index, and operations preserve it unless stated otherwise.

#### Select

Column projection. Given a relation $R$ and a column list $C = [c_1, \ldots, c_k]$ with $\{c_1, \ldots, c_k\} \subseteq \mathcal{S}(R)$:

$$\text{Select}(R, C) = [\, (r.c_1, \ldots, r.c_k) \mid r \in R \,]$$

Output schema is $C$. Row order and multiplicity are preserved.

#### Filter

Row selection. Given $R$ and a predicate $p : \text{Row} \to \{0, 1\}$:

$$\text{Filter}(R, p) = [\, r \mid r \in R,\; p(r) = 1 \,]$$

Output schema is $\mathcal{S}(R)$. Row order among surviving rows is preserved.

#### Join

Equi-join parameterised by join type. Given relations $R_1$, $R_2$, key columns $k_1 \in \mathcal{S}(R_1)$, $k_2 \in \mathcal{S}(R_2)$, and a join type $\tau \in \{\text{inner}, \text{left}, \text{right}, \text{outer}\}$:

$$\text{Join}(R_1, R_2, k_1, k_2, \tau)$$

For $\tau = \text{inner}$: produce all $(r_1 \| r_2)$ where $r_1.k_1 = r_2.k_2$, concatenating columns from both relations (dropping $k_2$). For $\tau = \text{left}$: as inner, but every row of $R_1$ appears at least once; unmatched $R_2$ columns are null. Right and outer are defined symmetrically. Output schema is $\mathcal{S}(R_1) \cup \mathcal{S}(R_2) \setminus \{k_2\}$.

#### GroupBy

Partitioned aggregation. Given $R$, grouping keys $K = [g_1, \ldots, g_m] \subseteq \mathcal{S}(R)$, and named aggregations $A = [(a_1, f_1, c_1), \ldots, (a_n, f_n, c_n)]$ where each $a_i$ is an output column name, $f_i$ an aggregation function, and $c_i$ the input column:

$$\text{GroupBy}(R, K, A) = \left[\, (v_1, \ldots, v_m, f_1(G.c_1), \ldots, f_n(G.c_n)) \;\middle|\; (v_1, \ldots, v_m) \in \text{distinct}_K(R),\; G = \sigma_{g_1 = v_1 \land \cdots \land g_m = v_m}(R) \,\right]$$

Output schema is $K \mathbin\| [a_1, \ldots, a_n]$. The order of groups is the order of first appearance in $R$.

#### Sort

Reordering. Given $R$ and a list of sort keys with directions $O = [(c_1, d_1), \ldots, (c_k, d_k)]$ where $d_i \in \{\text{asc}, \text{desc}\}$:

$$\text{Sort}(R, O) = \text{permute}(R, \prec_O)$$

where $\prec_O$ is the lexicographic order induced by the key–direction pairs. Ties among all keys preserve the original relative order (stable sort). Schema is unchanged.

#### Limit

Row truncation. Given $R$, a count $n \in \mathbb{N}$, and an end selector $e \in \{\text{head}, \text{tail}\}$:

$$\text{Limit}(R, n, \text{head}) = R[0 : \min(n, |R|)]$$
$$\text{Limit}(R, n, \text{tail}) = R[\max(0, |R| - n) : |R|]$$

Schema is unchanged.

#### WithColumn

Schema extension. Given $R$, a column name $c$, and an expression $e : \text{Row} \to \text{Value}$:

$$\text{WithColumn}(R, c, e) = [\, r \| (c \mapsto e(r)) \mid r \in R \,]$$

If $c \in \mathcal{S}(R)$, the existing column is replaced in place. If $c \notin \mathcal{S}(R)$, the column is appended. Row order is preserved.

#### Aggregate

Global aggregation — equivalent to GroupBy with an empty key set. Given $R$ and aggregations $A = [(a_1, f_1, c_1), \ldots, (a_n, f_n, c_n)]$:

$$\text{Aggregate}(R, A) = [(f_1(R.c_1), \ldots, f_n(R.c_n))]$$

The result is always a single row. Output schema is $[a_1, \ldots, a_n]$.

#### Union

Vertical concatenation. Given relations $R_1, R_2$ with $\mathcal{S}(R_1) = \mathcal{S}(R_2)$:

$$\text{Union}(R_1, R_2) = R_1 \mathbin\| R_2$$

All rows of $R_1$ appear first, followed by all rows of $R_2$. Schema is $\mathcal{S}(R_1)$. Duplicates are retained (multiset union).

#### Pivot

Long-to-wide reshaping. Given $R$, an index column $i$, a pivot column $p$, and a values column $v$:

$$\text{Pivot}(R, i, p, v)$$

The output relation has rows corresponding to distinct values of $i$ and columns $\{i\} \cup \text{distinct}(R.p)$. The cell at row $i = x$, column $q$ equals $r.v$ from the unique row $r$ where $r.i = x \land r.p = q$. If no such row exists the cell is null; if multiple rows match, an aggregation function must be supplied (default: first).

#### Melt

Wide-to-long reshaping (inverse of Pivot). Given $R$, identifier columns $I \subseteq \mathcal{S}(R)$, value columns $V = \mathcal{S}(R) \setminus I$, and optional name parameters $\text{var\_name}$ (default $\texttt{"variable"}$) and $\text{value\_name}$ (default $\texttt{"value"}$):

$$\text{Melt}(R, I, V, \text{var\_name}, \text{value\_name}) = \left[\, r|_I \| (\text{var\_name} \mapsto c,\; \text{value\_name} \mapsto r.c) \;\middle|\; r \in R,\; c \in V \,\right]$$

Each row of $R$ fans out to $|V|$ rows. Output schema is $I \mathbin\| [\text{var\_name}, \text{value\_name}]$.

#### Window

Windowed computation. Given $R$, a window specification $W = (\text{partition}: K,\; \text{order}: O,\; \text{frame}: F)$, a window function $f$, an input column $c$, and an output column name $a$:

$$\text{Window}(R, W, f, c, a) = \left[\, r \| (a \mapsto f(F_W(r).c)) \mid r \in R \,\right]$$

where $F_W(r)$ is the frame of rows visible to $r$ — the subset of $R$ sharing the same partition-key values as $r$, ordered by $O$, and bounded by frame $F$ (e.g., unbounded preceding to current row). Row order and the original schema are preserved; column $a$ is appended.

### Spreadsheet Algebra

While the dataframe algebra represents _what_ to compute, the spreadsheet algebra represents _how_ to lay that computation out in a spreadsheet. This is the target language of our translation. Spreadsheets have their own model: discrete cells arranged in grids, formulas that reference ranges, and tabs that organize related data.

Our spreadsheet algebra provides abstractions that match these primitives. A `Sheet` represents a tab, a `Range` identifies a rectangular region, and a `Formula` encapsulates a cell expression. By designing this layer carefully, we create a clean boundary between "what the user wants" (dataframe algebra) and "how we achieve it in sheets" (spreadsheet algebra).

**Core Abstractions:**

- `Sheet` - A single spreadsheet tab
- `Range` - A rectangular cell region (e.g., A2:C100)
- `Formula` - A cell formula expression (e.g., `=FILTER(...)`)
- `Value` - Static cell content for data that doesn't need formulas
- `Reference` - Cell/range reference that can be used in formulas

#### Coordinate Systems

The system uses different coordinate conventions at different boundaries to match idiomatic expectations:

**Internal Representation (0-indexed, Python convention):**
- All internal data structures use 0-indexed coordinates
- `Range` objects store coordinates as 0-indexed: `Range(row=0, col=0)` refers to cell A1
- `SetValues` and `SetFormula` operations use 0-indexed: `row=0, col=0` writes to cell A1
- Operations arithmetic uses 0-indexed: if a range spans rows 0-10, that's `10 - 0 + 1 = 11` rows

**External Representation (1-indexed, spreadsheet convention):**
- Spreadsheet APIs (Google Sheets) use 1-indexed A1 notation
- `Range.to_a1()` converts to 1-indexed: `Range(row=0, col=0).to_a1()` returns `"A1"`
- `Range.from_a1()` parses 1-indexed notation: `Range.from_a1("A1")` returns `Range(row=0, col=0)`
- Executors convert 0-indexed operations to 1-indexed API calls at the boundary

**Conversion Utilities:**
- `zero_to_one_indexed(row, col)` - Convert from internal (0-indexed) to API (1-indexed)
- `one_to_zero_indexed(row, col)` - Convert from API (1-indexed) to internal (0-indexed)

**Example:**
```python
# Internal: Create a range for cells A1:C10 (0-indexed)
r = Range(row=0, col=0, row_end=9, col_end=2)

# External: Convert to A1 notation for Google Sheets API (1-indexed)
a1_notation = r.to_a1()  # Returns "A1:C10"

# Operations use 0-indexed coordinates
op = SetValues(sheet="Data", row=0, col=0, values=[[...]])  # Writes to A1

# Executor converts to 1-indexed for API calls
# internally: op.row=0 -> API: row=1 (A1 notation)
```

**Rationale:** Using 0-indexed coordinates internally aligns with Python conventions, simplifies array indexing, and matches how operations are implemented. Converting to 1-indexed only at API boundaries keeps the conversion logic isolated and explicit.

The operations below are state transitions on a workbook. We write $\mathcal{W}$ for the current workbook state (a map from sheet names to grids), $\mathcal{W}.s$ for the grid of sheet $s$, $\mathcal{W}.s[r, c]$ for the cell at row $r$ and column $c$ (0-indexed internally, though the spec notation may use 1-indexed for clarity), and $\mathcal{W}.\mathcal{N}$ for the workbook's named-range registry.

#### CreateSheet

Adds an empty sheet to the workbook. Given a workbook $\mathcal{W}$, a sheet name $s \notin \text{dom}(\mathcal{W})$, and dimensions $m, n \in \mathbb{N}^+$:

$$\text{CreateSheet}(\mathcal{W}, s, m, n) = \mathcal{W}[s \mapsto \mathbf{0}_{m \times n}]$$

where $\mathbf{0}_{m \times n}$ is an $m \times n$ grid of null cells. The operation is undefined if $s$ already exists.

#### SetValues

Bulk assignment of static values. Given a workbook $\mathcal{W}$, a sheet $s \in \text{dom}(\mathcal{W})$, an anchor position $(r_0, c_0)$, and a value matrix $V \in \text{Value}^{m \times n}$:

$$\text{SetValues}(\mathcal{W}, s, (r_0, c_0), V) = \mathcal{W}\!\left[s[r_0\!:\!r_0\!+\!m,\; c_0\!:\!c_0\!+\!n] \mapsto V\right]$$

Every cell in the target rectangle is overwritten with the corresponding element of $V$. Cells outside the rectangle are unchanged. The grid of $s$ is extended if the rectangle exceeds current dimensions.

#### SetFormula

Formula assignment. Given a workbook $\mathcal{W}$, a sheet $s \in \text{dom}(\mathcal{W})$, a cell position $(r, c)$, and a formula expression $\varphi$:

$$\text{SetFormula}(\mathcal{W}, s, (r, c), \varphi) = \mathcal{W}\!\left[s[r, c] \mapsto \varphi\right]$$

A formula $\varphi$ is a tree of function applications, literal values, and references. References take the form $s'!\text{Range}$ (cross-sheet) or $\text{Range}$ (same-sheet), where a Range is either a single cell $(r, c)$ or a rectangle $(r_0, c_0):(r_1, c_1)$. Named ranges (see below) may appear anywhere a reference can. The cell's displayed value is the result of evaluating $\varphi$ against the current workbook state — this evaluation is performed by the spreadsheet engine, not by our system.

#### NamedRange

Named-range registration. Given a workbook $\mathcal{W}$, a label $\ell$, a sheet $s \in \text{dom}(\mathcal{W})$, and a rectangle $(r_0, c_0, r_1, c_1)$:

$$\text{NamedRange}(\mathcal{W}, \ell, s, (r_0, c_0, r_1, c_1)) = \mathcal{W}\!\left[\mathcal{N} \cup \{\ell \mapsto s!(r_0, c_0):(r_1, c_1)\}\right]$$

After registration, $\ell$ can be used in any formula in place of the explicit range reference. If $\ell$ already exists in $\mathcal{N}$, the binding is updated to the new range.

### Translator

The translator is the bridge between dataframe algebra and spreadsheet algebra. It walks the operation tree and deterministically maps each node to its spreadsheet representation based on the node's type and structural properties (column names, predicates, join keys, aggregation functions). It never inspects the actual data values in the dataframes.

Each operation type has a fixed translation rule. Some are straightforward (select becomes column projection), while others decompose into helper sheets — for example, a multi-step groupby might produce an intermediate sheet for the grouped calculation, with the final output sheet referencing it via formulas. When a single formula cannot express an operation, the translator deterministically breaks it into a chain of sheets, each referencing the previous one. The translator can also apply structural optimizations like pushing filters down or collapsing adjacent operations — but these too operate only on the plan tree, never on data.

The translation function $\mathcal{T}$ maps each dataframe algebra node to a sequence of spreadsheet algebra operations. For a unary node whose input has been materialised at range $\rho$ on some sheet, $\mathcal{T}$ returns a pair $(\mathcal{W}', \rho')$ — the updated workbook and the output range. Binary nodes (Join) accept two input ranges. We write $\text{col}(\rho, c)$ for the sub-range of $\rho$ covering column $c$ (data rows only, excluding the header), $\text{idx}_\rho(c)$ for its zero-based column offset, and $\rho_{\text{data}}$ for the data portion of $\rho$ (all columns, header excluded).

Every rule satisfies a uniform correctness invariant: the cells in $\rho'$, when evaluated by the spreadsheet engine against $\mathcal{W}'$, produce the same values as the corresponding dataframe algebra operation applied to the data in $\rho$.

#### Translating Source

Base case — the only rule that writes static values. Given a source relation $R$ with $m$ rows and schema $[c_1, \ldots, c_n]$:

$$\mathcal{T}(\text{Source}(R))(\mathcal{W}) = (\mathcal{W}_3,\; s!(0,0):(m, n\!-\!1))$$

where $\mathcal{W}_1 = \text{CreateSheet}(\mathcal{W}, s, m\!+\!1, n)$, then $\mathcal{W}_2 = \text{SetValues}(\mathcal{W}_1, s, (0,0), [c_1, \ldots, c_n])$ writes the header, then $\mathcal{W}_3 = \text{SetValues}(\mathcal{W}_2, s, (1,0), \text{rows}(R))$ writes data. Every subsequent translation rule produces formulas exclusively.

#### Translating Select

Given $\text{Select}(R, [c_1, \ldots, c_k])$ with input at $\rho$:

$$\mathcal{T}(\text{Select})(\mathcal{W}, \rho) = (\mathcal{W}',\; s!(0,0):(|\rho.\text{rows}|\!-\!1,\; k\!-\!1))$$

The translator creates a fresh sheet $s$, writes headers $[c_1, \ldots, c_k]$ via $\text{SetValues}$, and for each selected column installs an array formula referencing the source column:

$$\forall\, j \in [1, k]:\quad \text{SetFormula}\!\left(\mathcal{W}, s, (1, j\!-\!1),\; \texttt{=ARRAYFORMULA(}\text{col}(\rho, c_j)\texttt{)}\right)$$

The $\texttt{ARRAYFORMULA}$ wrapper is required by Google Sheets to spill a range reference across all data rows from a single cell. **Correctness**: $\text{eval}(\rho') = \pi_{c_1, \ldots, c_k}(\text{eval}(\rho))$.

#### Translating Filter

Given $\text{Filter}(R, p)$ with input at $\rho$. The predicate $p$ decomposes into atomic comparisons $c \;\theta\; v$ (where $\theta \in \{>,<,=,\geq,\leq,\neq\}$ and $v$ is a literal) joined by $\land$ / $\lor$:

$$\mathcal{T}(\text{Filter})(\mathcal{W}, \rho) = (\mathcal{W}',\; s!(0,0):(?,\; n\!-\!1))$$

The translator creates a fresh sheet $s$ and installs a single $\texttt{FILTER}$ array formula:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1, 0),\; \texttt{=FILTER(}\rho_{\text{data}}\texttt{,}\; \varphi_p\texttt{)}\right)$$

where $\varphi_p$ translates the predicate tree: an atomic comparison $c \;\theta\; v$ becomes $\text{col}(\rho, c) \;\theta\; v$; conjunction $p_1 \land p_2$ becomes $(\varphi_{p_1}) \texttt{*} (\varphi_{p_2})$; disjunction $p_1 \lor p_2$ becomes $(\varphi_{p_1}) \texttt{+} (\varphi_{p_2})$. The output row count is indeterminate at translation time (denoted $?$) — $\texttt{FILTER}$ dynamically sizes its spill range. **Correctness**: $\text{eval}(\rho') = \sigma_p(\text{eval}(\rho))$.

#### Translating Join

Given $\text{Join}(R_1, R_2, k_1, k_2, \tau)$ with inputs at $\rho_1, \rho_2$:

$$\mathcal{T}(\text{Join})(\mathcal{W}, \rho_1, \rho_2) = (\mathcal{W}',\; s!(0,0):(N_\tau\!-\!1,\; |\mathcal{S}(R_1)| + |\mathcal{S}(R_2)| - 2))$$

where $N_\tau$ is the output row count parameterised by join type: $N_\tau = |\rho_1.\text{rows}|$ for $\tau \in \{\text{inner}, \text{left}\}$, $N_\tau = |\rho_2.\text{rows}|$ for $\tau = \text{right}$, and $N_\tau = \;?$ (indeterminate) for $\tau = \text{outer}$.

The translator creates a fresh sheet $s$. For $\tau \in \{\text{inner}, \text{left}\}$, the left-side columns are array-referenced from $\rho_1$; for $\tau = \text{right}$, the roles of $\rho_1$ and $\rho_2$ are swapped so that $\rho_2$ drives the row layout. For each non-key column $c \in \mathcal{S}(R_2) \setminus \{k_2\}$, an $\texttt{XLOOKUP}$ formula brings in the matching value:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1, j),\; \texttt{=XLOOKUP(}\text{col}(\rho_1, k_1)\texttt{,}\; \text{col}(\rho_2, k_2)\texttt{,}\; \text{col}(\rho_2, c)\texttt{)}\right)$$

For $\tau = \text{left}$, $\texttt{XLOOKUP}$'s fourth argument (if-not-found) defaults to $\texttt{\#N/A}$, which is replaced with an empty string to produce null. For $\tau = \text{inner}$, a follow-up $\text{Filter}$ translation on a helper sheet removes rows where all looked-up columns are null. Right and outer joins reverse or duplicate the pattern symmetrically. **Correctness**: $\text{eval}(\rho') = R_1 \bowtie^{\tau}_{k_1 = k_2} R_2$.

#### Translating GroupBy

Given $\text{GroupBy}(R, K, A)$ with keys $K = [g_1, \ldots, g_m]$, aggregations $A = [(a_1, f_1, c_1), \ldots, (a_p, f_p, c_p)]$, and input at $\rho$:

$$\mathcal{T}(\text{GroupBy})(\mathcal{W}, \rho) = (\mathcal{W}',\; s!(0,0):(?,\; m + p - 1))$$

Google Sheets' $\texttt{QUERY ... GROUP BY}$ sorts groups alphabetically, which violates the first-appearance ordering invariant required by the dataframe algebra. The translator therefore uses a **two-sheet strategy**: a helper sheet for the aggregation and an output sheet that restores first-appearance order.

**Helper sheet** $s_q$ — performs the aggregation via $\texttt{QUERY}$:

$$\text{SetFormula}\!\left(\mathcal{W}, s_q, (0, 0),\; \texttt{=QUERY(}\rho_{\text{all}}\texttt{, "SELECT } Q(K) \texttt{, } Q(A) \texttt{ GROUP BY } Q(K) \texttt{ LABEL } Q(A_\text{labels}) \texttt{"}\texttt{)}\right)$$

where $Q(K)$ maps each key $g_i$ to its $\texttt{QUERY}$ column letter ($\texttt{Col1}, \texttt{Col2}, \ldots$) based on $\text{idx}_\rho(g_i)$, and $Q(A)$ maps each $(a_i, f_i, c_i)$ to a $\texttt{QUERY}$ aggregation clause. The function mapping is: $\text{sum} \to \texttt{SUM}$, $\text{mean} \to \texttt{AVG}$, $\text{count} \to \texttt{COUNT}$, $\text{min} \to \texttt{MIN}$, $\text{max} \to \texttt{MAX}$. $\texttt{QUERY}$ emits its own header row, so no separate $\text{SetValues}$ header is needed on the helper sheet.

**Output sheet** $s$ — re-sorts the helper sheet's rows into first-appearance order. The translator writes headers $K \mathbin\| [a_1, \ldots, a_p]$ via $\text{SetValues}$ and installs a $\texttt{SORT}$ formula that orders by the position of each group key's first occurrence in the original data:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1, 0),\; \texttt{=SORT(}s_q\texttt{!}\rho_{q,\text{data}}\texttt{,}\; \texttt{XMATCH(}s_q\texttt{!}\text{col}(\rho_q, g_1)\texttt{,}\; \texttt{UNIQUE(}\text{col}(\rho, g_1)\texttt{))}\texttt{, TRUE}\texttt{)}\right)$$

$\texttt{UNIQUE(}\text{col}(\rho, g_1)\texttt{)}$ extracts the distinct key values in first-appearance order from the original input. $\texttt{XMATCH}$ maps each alphabetically-sorted group from $s_q$ to its first-appearance position, and $\texttt{SORT}$ reorders accordingly. For multi-key group-bys, the $\texttt{XMATCH}$ expression uses a concatenated composite key.

Output row count is indeterminate. **Correctness**: $\text{eval}(\rho') = \gamma_{K, A}(\text{eval}(\rho))$, with groups in first-appearance order.

#### Translating Aggregate

Given $\text{Aggregate}(R, [(a_1, f_1, c_1), \ldots, (a_n, f_n, c_n)])$ with input at $\rho$:

$$\mathcal{T}(\text{Aggregate})(\mathcal{W}, \rho) = (\mathcal{W}',\; s!(0,0):(1,\; n\!-\!1))$$

The translator creates a fresh sheet $s$, writes headers $[a_1, \ldots, a_n]$ via $\text{SetValues}$, and for each aggregation installs a scalar formula over the corresponding source column:

$$\forall\, i \in [1, n]:\quad \text{SetFormula}\!\left(\mathcal{W}, s, (1, i\!-\!1),\; f_i^{\mathcal{G}}\!\left(\text{col}(\rho, c_i)\right)\right)$$

where $f_i^{\mathcal{G}}$ is the Google Sheets function for $f_i$: $\text{sum} \to \texttt{SUM}$, $\text{mean} \to \texttt{AVERAGE}$, $\text{count} \to \texttt{COUNTA}$, $\text{min} \to \texttt{MIN}$, $\text{max} \to \texttt{MAX}$. The result is always exactly one data row. **Correctness**: $\text{eval}(\rho') = [(f_1(R.c_1), \ldots, f_n(R.c_n))]$.

#### Translating Sort

Given $\text{Sort}(R, [(c_1, d_1), \ldots, (c_k, d_k)])$ with input at $\rho$:

$$\mathcal{T}(\text{Sort})(\mathcal{W}, \rho) = (\mathcal{W}',\; s!(0,0):(|\rho.\text{rows}|\!-\!1,\; |\mathcal{S}(R)|\!-\!1))$$

The translator creates a fresh sheet $s$, writes headers $\mathcal{S}(R)$ via $\text{SetValues}$, and installs a single $\texttt{SORT}$ array formula:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1, 0),\; \texttt{=SORT(}\rho_{\text{data}}\texttt{,}\; \text{idx}_\rho(c_1)\!+\!1\texttt{,}\; \beta(d_1)\texttt{,}\; \ldots\texttt{,}\; \text{idx}_\rho(c_k)\!+\!1\texttt{,}\; \beta(d_k)\texttt{)}\right)$$

where $\beta(\text{asc}) = \texttt{TRUE}$ and $\beta(\text{desc}) = \texttt{FALSE}$. Column indices are 1-based as required by Google Sheets' $\texttt{SORT}$ function. The $\texttt{SORT}$ function performs a stable sort — ties among all keys preserve the original relative order. Output row count equals the input row count (no rows are added or removed). Schema is unchanged. **Correctness**: $\text{eval}(\rho') = \text{permute}(\text{eval}(\rho), \prec_O)$.

#### Translating Limit

Given $\text{Limit}(R, n, e)$ with input at $\rho$:

$$\mathcal{T}(\text{Limit})(\mathcal{W}, \rho) = (\mathcal{W}',\; s!(0,0):(\min(n, |\rho.\text{rows}|\!-\!1),\; |\mathcal{S}(R)|\!-\!1))$$

The translator creates a fresh sheet $s$, writes headers $\mathcal{S}(R)$ via $\text{SetValues}$, and installs a formula depending on the end selector $e$:

For $e = \text{head}$:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1, 0),\; \texttt{=ARRAY\_CONSTRAIN(}\rho_{\text{data}}\texttt{,}\; n\texttt{,}\; |\mathcal{S}(R)|\texttt{)}\right)$$

$\texttt{ARRAY\_CONSTRAIN}$ truncates the spill range of $\rho_{\text{data}}$ to at most $n$ rows and $|\mathcal{S}(R)|$ columns, returning the first $\min(n, |R|)$ rows.

For $e = \text{tail}$:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1, 0),\; \texttt{=OFFSET(}\rho_{\text{data}}\texttt{,}\; \texttt{ROWS(}\rho_{\text{data}}\texttt{)}\!-\!n\texttt{,}\; 0\texttt{,}\; n\texttt{,}\; |\mathcal{S}(R)|\texttt{)}\right)$$

$\texttt{OFFSET}$ shifts the origin to the $n$-th row from the end and returns an $n \times |\mathcal{S}(R)|$ subrange. When $n \geq |R|$, the formula returns the entire relation. Schema is unchanged. **Correctness**: for head, $\text{eval}(\rho') = R[0 : \min(n, |R|)]$; for tail, $\text{eval}(\rho') = R[\max(0, |R| - n) : |R|]$.

#### Translating WithColumn

Given $\text{WithColumn}(R, c, e)$ with input at $\rho$:

$$\mathcal{T}(\text{WithColumn})(\mathcal{W}, \rho) = (\mathcal{W}',\; s!(0,0):(|\rho.\text{rows}|\!-\!1,\; |\mathcal{S}_{\text{out}}|\!-\!1))$$

where $\mathcal{S}_{\text{out}} = \mathcal{S}(R)$ with $c$ replaced in place if $c \in \mathcal{S}(R)$, or $\mathcal{S}(R) \mathbin\| [c]$ if $c \notin \mathcal{S}(R)$.

The translator creates a fresh sheet $s$ and writes headers $\mathcal{S}_{\text{out}}$ via $\text{SetValues}$. For each column $c_j \in \mathcal{S}_{\text{out}}$:

- If $c_j \neq c$: install an array formula referencing the source column:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1, j),\; \texttt{=ARRAYFORMULA(}\text{col}(\rho, c_j)\texttt{)}\right)$$

- If $c_j = c$: install the translated expression wrapped in $\texttt{ARRAYFORMULA}$:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1, j),\; \texttt{=ARRAYFORMULA(}\varphi_e\texttt{)}\right)$$

The expression $e$ is translated to a formula $\varphi_e$: column references $r.c_i$ become range references $\text{col}(\rho, c_i)$, arithmetic operators map directly, and unsupported Python operations raise $\texttt{UnsupportedOperationError}$. The $\texttt{ARRAYFORMULA}$ wrapper is required by Google Sheets to spill range-based expressions across all data rows. Row order is preserved. **Correctness**: $\text{eval}(\rho') = [\, r \| (c \mapsto e(r)) \mid r \in R \,]$.

#### Translating Union

Given $\text{Union}(R_1, R_2)$ with inputs at $\rho_1, \rho_2$. Precondition: $\mathcal{S}(R_1) = \mathcal{S}(R_2)$ — the translator raises $\texttt{UnsupportedOperationError}$ if schemas differ.

$$\mathcal{T}(\text{Union})(\mathcal{W}, \rho_1, \rho_2) = (\mathcal{W}',\; s!(0,0):(|\rho_1.\text{rows}|\!+\!|\rho_2.\text{rows}|\!-\!2,\; |\mathcal{S}(R_1)|\!-\!1))$$

The translator creates a fresh sheet $s$, writes headers $\mathcal{S}(R_1)$ via $\text{SetValues}$, and installs a single array formula that vertically stacks the two data ranges using Google Sheets' array literal syntax:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1, 0),\; \texttt{=\{}\rho_{1,\text{data}}\texttt{;}\; \rho_{2,\text{data}}\texttt{\}}\right)$$

The semicolon in $\texttt{\{A ; B\}}$ denotes vertical concatenation: all rows of $\rho_1$ appear first, followed by all rows of $\rho_2$. Duplicates are retained (multiset union). **Correctness**: $\text{eval}(\rho') = R_1 \mathbin\| R_2$.

#### Translating Pivot

Given $\text{Pivot}(R, i, p, v)$ with input at $\rho$. Pivot requires knowing the distinct values of the pivot column $p$ to construct the output schema. Since the translator is data-blind, it cannot enumerate these values at translation time. The translator decomposes the operation into a two-sheet strategy:

**Helper sheet** $s_d$ — extracts and transposes the distinct pivot-column values into a header row:

$$\text{SetFormula}\!\left(\mathcal{W}, s_d, (0, 0),\; \texttt{=TRANSPOSE(SORT(UNIQUE(}\text{col}(\rho, p)\texttt{)))}\right)$$

**Output sheet** $s$ — the header row has column 0 set to $i$ via $\text{SetValues}$; columns $1, 2, \ldots$ reference the transposed distinct values in $s_d$. The distinct index values in column 0 are produced by a $\texttt{UNIQUE}$ formula:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1, 0),\; \texttt{=UNIQUE(}\text{col}(\rho, i)\texttt{)}\right)$$

Each data cell at row $r$, column $q \geq 1$ uses a filtered lookup to find the value where the index and pivot columns both match:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (r, q),\; \texttt{=IFERROR(INDEX(FILTER(}\text{col}(\rho, v)\texttt{, (}\text{col}(\rho, i)\texttt{=\$A}r\texttt{)*(}\text{col}(\rho, p)\texttt{=}s_d\texttt{!}(0,q\!-\!1)\texttt{)), 1), "")}\right)$$

$\texttt{INDEX(..., 1)}$ selects the first matching value, implementing the default $\texttt{first}$ aggregation for duplicate $(i, p)$ pairs. When an explicit aggregation function $g$ is supplied, $\texttt{INDEX(..., 1)}$ is replaced with the corresponding Google Sheets aggregate applied to the filtered range (e.g., $\texttt{SUM(FILTER(...))}$ for $g = \text{sum}$). $\texttt{IFERROR(..., "")}$ produces null for missing cells.

$$\mathcal{T}(\text{Pivot})(\mathcal{W}, \rho) = (\mathcal{W}',\; s!(0,0):(?,\; ?))$$

Output dimensions are indeterminate at translation time — both the number of rows (distinct index values) and columns (distinct pivot values) depend on the data. **Correctness**: $\text{eval}(\rho')$ is the pivoted relation as defined in §Pivot.

#### Translating Melt

Given $\text{Melt}(R, I, V, \text{var\_name}, \text{value\_name})$ with $I = [i_1, \ldots, i_m]$, $V = [v_1, \ldots, v_k]$, and input at $\rho$:

$$\mathcal{T}(\text{Melt})(\mathcal{W}, \rho) = (\mathcal{W}',\; s!(0,0):(|\rho.\text{rows}| \cdot k \!-\! 1,\; m\!+\!1))$$

Each input row fans out to $k = |V|$ output rows. The translator creates a fresh sheet $s$ and writes headers $I \mathbin\| [\text{var\_name}, \text{value\_name}]$ via $\text{SetValues}$.

For each identifier column $i_j \in I$, the translator installs an array formula that repeats each source value $k$ times:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1, j\!-\!1),\; \texttt{=ARRAYFORMULA(INDEX(}\text{col}(\rho, i_j)\texttt{, INT((ROW(INDIRECT("1:"\&ROWS(}\text{col}(\rho, i_j)\texttt{)*}k\texttt{))-1)/}k\texttt{)+1))}\right)$$

The $\texttt{INT((ROW(...)-1)/}k\texttt{)+1}$ pattern maps each block of $k$ consecutive output rows back to the same source row index.

For the $\text{var\_name}$ column (position $m$), the translator generates a repeating cycle of the value-column names:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1, m),\; \texttt{=ARRAYFORMULA(CHOOSE(MOD(ROW(INDIRECT("1:"\&ROWS(}\text{col}(\rho, i_1)\texttt{)*}k\texttt{))-1,}k\texttt{)+1,}\; \texttt{"}v_1\texttt{",}\; \ldots\texttt{,}\; \texttt{"}v_k\texttt{"))}\right)$$

For the $\text{value\_name}$ column (position $m\!+\!1$), the translator selects the correct source column per output row:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1, m\!+\!1),\; \texttt{=ARRAYFORMULA(CHOOSE(MOD(ROW(INDIRECT("1:"\&ROWS(}\text{col}(\rho, i_1)\texttt{)*}k\texttt{))-1,}k\texttt{)+1,}\; \text{col}(\rho, v_1)\texttt{,}\; \ldots\texttt{,}\; \text{col}(\rho, v_k)\texttt{))}\right)$$

$\texttt{CHOOSE}$ with $\texttt{MOD}$ cycles through the $k$ value columns in order. **Correctness**: $\text{eval}(\rho') = [\, r|_I \| (\text{var\_name} \mapsto c,\; \text{value\_name} \mapsto r.c) \mid r \in R, c \in V \,]$.

#### Translating Window

Given $\text{Window}(R, W, f, c, a)$ with $W = (\text{partition}: K,\; \text{order}: O,\; \text{frame}: F)$ and input at $\rho$:

$$\mathcal{T}(\text{Window})(\mathcal{W}, \rho) = (\mathcal{W}',\; s!(0,0):(|\rho.\text{rows}|\!-\!1,\; |\mathcal{S}(R)|))$$

The translator creates a fresh sheet $s$, writes headers $\mathcal{S}(R) \mathbin\| [a]$ via $\text{SetValues}$. For all existing columns $c_j \in \mathcal{S}(R)$, $\texttt{ARRAYFORMULA}$-wrapped references to the source are installed as in WithColumn. The window column $a$ is placed at position $|\mathcal{S}(R)|$.

Window formulas are **per-row** (not array formulas) because each row's visible frame may differ. For row $i$ (0-indexed in the data), the formula depends on the window function $f$:

**Ranking functions** ($f \in \{\text{rank}, \text{row\_number}\}$): count rows in the same partition with ordering value $\leq$ the current row's:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1\!+\!i,\; |\mathcal{S}(R)|),\; \texttt{=COUNTIFS(}\text{col}(\rho, g_1)\texttt{, }s\texttt{!}G_{1+i}\texttt{, }\ldots\texttt{, }\text{col}(\rho, o_1)\texttt{, "<="\&}s\texttt{!}O_{1+i}\texttt{)}\right)$$

where $G_{1+i}$ and $O_{1+i}$ are the cells in sheet $s$ holding the current row's partition-key and order-key values respectively, and the $\texttt{COUNTIFS}$ ranges span all data rows in $\rho$.

**Running aggregates** ($f \in \{\text{sum}, \text{mean}, \text{min}, \text{max}, \text{count}\}$ with $F = \text{unbounded preceding to current row}$): conditional aggregation over the partition up to the current row:

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1\!+\!i,\; |\mathcal{S}(R)|),\; f^{\mathcal{C}}\texttt{(}\text{col}(\rho, c)\texttt{, }\text{col}(\rho, g_1)\texttt{, }s\texttt{!}G_{1+i}\texttt{, }\ldots\texttt{, }\text{col}(\rho, o_1)\texttt{, "<="\&}s\texttt{!}O_{1+i}\texttt{)}\right)$$

where $f^{\mathcal{C}}$ is the conditional variant: $\text{sum} \to \texttt{SUMIFS}$, $\text{mean} \to \texttt{AVERAGEIFS}$, $\text{count} \to \texttt{COUNTIFS}$, $\text{min} \to \texttt{MINIFS}$, $\text{max} \to \texttt{MAXIFS}$.

**Lag/lead** ($f \in \{\text{lag}, \text{lead}\}$ with offset $o$):

$$\text{SetFormula}\!\left(\mathcal{W}, s, (1\!+\!i,\; |\mathcal{S}(R)|),\; \texttt{=IFERROR(OFFSET(}s\texttt{!}C_{1+i}\texttt{, }\delta\texttt{, 0), "")}\right)$$

where $C_{1+i}$ is the cell holding column $c$'s value for the current row, $\delta = +o$ for lead and $\delta = -o$ for lag, and $\texttt{IFERROR}$ produces null when the offset falls outside the data range.

For window specifications that cannot be expressed with available Google Sheets formulas (e.g., custom frame bounds beyond unbounded-preceding-to-current-row, or partition-aware lag/lead that must not cross partition boundaries), the translator raises $\texttt{UnsupportedOperationError}$.

Row order and the original schema are preserved; column $a$ is appended. **Correctness**: $\text{eval}(\rho') = [\, r \| (a \mapsto f(F_W(r).c)) \mid r \in R \,]$.

Translation is a pure function of the operation node — **source data becomes static values; every derived computation becomes a formula.** Source nodes (e.g., `read_csv`) are leaf nodes with no operation to translate, so their data is written as cell values. Every other node in the plan tree is a transformation and maps to a formula that references upstream ranges. There is no heuristic, no fallback — if an operation cannot be expressed as a formula, the translator raises `UnsupportedOperationError`.

### Execution Plan

The execution plan is the final, concrete representation before we hit the API. This separation exists because the spreadsheet algebra can be reused for different backends (Excel, Google Sheets, CSV export), while the execution plan is Google Sheets-specific.

The plan groups operations into batches to minimize API round-trips and encodes their dependency order: sheets are created before data is written, source data lands before formulas that reference it, and formatting is applied last.

### Google Sheets Executor

The executor is the only component that talks to Google Sheets. It takes an execution plan and applies it via `gspread`, owning authentication, rate limiting, error recovery, and API quirks so the rest of the system can stay focused on logic. Transient failures (rate limits, network blips, service errors) are handled with exponential backoff and post-operation validation (e.g., confirming a sheet exists before writing to it).

**`gspread` Operations:**

- `gc.create(title)` - Create new spreadsheet
- `spreadsheet.add_worksheet(title, rows, cols)` - Add tabs
- `worksheet.update(range, values)` - Set cell values in bulk (static values and formulas)
- `worksheet.get_all_values()` / `worksheet.get(range)` - Read data back for validation
- `worksheet.format(range, format_dict)` - Apply formatting (colors, borders, number formats)
- `spreadsheet.batch_update(body)` - Low-level batch API for complex operations (merging, conditional formatting)
