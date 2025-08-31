"""Filter expression system for database queries.

This package provides a flexible system for creating and evaluating filter expressions
that can be translated into different database query languages (AQL, Cypher, Python).

Key Components:
    - LogicalOperator: Logical operations (AND, OR, NOT, IMPLICATION)
    - ComparisonOperator: Comparison operations (==, !=, >, <, etc.)
    - Clause: Filter clause implementation
    - Expression: Filter expression factory

Example:
    >>> from graflo.filter import Expression
    >>> expr = Expression.from_dict({
    ...     "AND": [
    ...         {"field": "age", "cmp_operator": ">=", "value": 18},
    ...         {"field": "status", "cmp_operator": "==", "value": "active"}
    ...     ]
    ... })
    >>> # Converts to: "age >= 18 AND status == 'active'"
"""
