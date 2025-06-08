# Query Optimization Guidelines

This project stores analytical data in DuckDB. Complex queries can become slow as data grows. Use the following guidelines when adding or modifying queries.

## Profiling

- Use `EXPLAIN` or `EXPLAIN ANALYZE` to inspect query plans.
- Log the plan and execution time through `DatabaseRepository.explain_query` and `execute_query`.

## Indexes

- Frequently joined or filtered columns should have indexes.
- The repository provides `ensure_common_indexes()` which creates indexes for common columns such as `PersonID` and `KnessetNum`.
- Add additional `create_index()` calls when new queries introduce heavy filters.

## JOIN Best Practices

- Prefer explicit `JOIN` clauses with clear conditions rather than old-style comma joins.
- Ensure columns used in joins are indexed.
- When joining large tables, filter them in subqueries before the join if possible.

## Logging

- `DatabaseRepository.execute_query` logs how long each query takes.
- Use these logs to identify slow queries and iterate on improvements.

## Testing

- Always verify that optimized queries return the same results as before.
- Add regression tests for any new query functions.
