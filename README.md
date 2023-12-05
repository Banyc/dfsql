# `dfsql`

![](img/terminal.png)

## Install

```bash
cargo install dfsql
```

## REPL

- `exit`/`quit`: exit the REPL loop.
  ```py
  exit
  ```
- `undo`: undo the previous successful operation.
  ```py
  undo
  ```
- `reset`: reset all the changes and go back to the original data frame.
  ```py
  reset
  ```
- `schema`: show column names and types of the data frame.
  ```sql
  schema
  ```

## Statements

- `select`
  ```py
  select <expr>*
  ```
  ```sql
  select last_name first_name
  ```
  - Select columns "last_name" and "first_name" and collect them into a data frame.
- Group by
  ```py
  group (<col> | <var>)* agg <expr>*
  ```
  ```sql
  group first_name agg (count)
  ```
  - Group the data frame by column "first_name" and then aggregate each group with the count of the members.
- `filter`
  ```py
  filter <expr>
  ```
  ```sql
  filter first_name = "John"
  ```
- `limit`
  ```py
  limit <int>
  ```
  ```sql
  limit 5
  ```
- `reverse`
  ```sql
  reverse
  ```
- `sort`
  ```py
  sort <col>
  ```
  ```sql
  sort icpsr_id
  ```

## Expressions

- `col`: reference to a column.
  ```py
  col : (<str> | <var>) -> <expr>
  ```
  ```sql
  select col first_name
  ```
- `exclude`: remove columns from the data frame.
  ```py
  exclude : <expr>* -> <expr>
  ```
  ```sql
  select exclude last_name first_name
  ```
- literal: literal values like `42`, `"John"`, `1.0`, and `null`.
- binary operations
  ```sql
  select a * b
  ```
  - Calculate the product of columns "a" and "b" and collect the result.
- unary operations
  ```sql
  select -a
  ```
- aggregate
  ```py
  <aggregate> : <expr>? -> <expr>
  ```
  ```sql
  select sum a
  ```
  - Sum all values in column "a" and collect the scalar result.
- `alias`: assign a name to a column.
  ```py
  alias : (<col> | <var>) <expr> -> <expr>
  ```
  ```sql
  select alias product a * b
  ```
  - Assign the name "product" to the product and collect the new column.
- conditional
  ```py
  <conditional> : if <expr> then <expr> (if <expr> then <expr>)* otherwise <expr> -> <expr>
  ```
  ```sql
  select if class = 0 then "A" if class = 1 then "B" else null
  ```
- `cast`: cast a column to either type `str`, `int`, or `float`.
  ```py
  cast : <type> <expr> -> <expr>
  ```
  ```sql
  select cast str id
  ```
  - Cast the column "id" to type `str` and collect the result.
