from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from typing import Any, Callable, Dict, List, Optional

from .connection import MySQLConnectionConfig, connect


@dataclass
class RewriteChange:
    description: str


@dataclass
class RewriteResult:
    original_sql: str
    rewritten_sql: str
    mode: str
    changes: List[RewriteChange]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "original_sql": self.original_sql,
            "rewritten_sql": self.rewritten_sql,
            "mode": self.mode,
            "changes": [asdict(c) for c in self.changes],
        }


def _rewrite_null_comparisons(sql: str, changes: List[RewriteChange]) -> str:
    pattern_not_eq = re.compile(r"!=\s*NULL", re.IGNORECASE)
    if pattern_not_eq.search(sql):
        sql = pattern_not_eq.sub("IS NOT NULL", sql)
        changes.append(RewriteChange(description="Replaced `!= NULL` with `IS NOT NULL`"))
    pattern_eq = re.compile(r"=\s*NULL", re.IGNORECASE)
    if pattern_eq.search(sql):
        sql = pattern_eq.sub("IS NULL", sql)
        changes.append(RewriteChange(description="Replaced `= NULL` with `IS NULL`"))
    return sql


def _rewrite_or_to_in(sql: str, changes: List[RewriteChange]) -> str:
    # Very simple heuristic: WHERE col = 'a' OR col = 'b' OR col = 'c' -> WHERE col IN (...)
    pattern = re.compile(
        r"WHERE\s+([\w\.]+)\s*=\s*([^\s]+)\s+OR\s+\1\s*=\s*([^\s]+(?:\s+OR\s+\1\s*=\s*[^\s]+)*)",
        re.IGNORECASE,
    )

    def repl(match: re.Match) -> str:
        column = match.group(1)
        first_value = match.group(2)
        rest = match.group(3)
        values = [first_value]
        for part in re.split(r"\s+OR\s+", rest, flags=re.IGNORECASE):
            _col, _eq, _val = part.partition("=")
            values.append(_val.strip())
        in_list = ", ".join(values)
        changes.append(RewriteChange(description="Converted OR chain to IN() list"))
        return f"WHERE {column} IN ({in_list})"

    return pattern.sub(repl, sql)


def _rewrite_select_star_with_columns(
    sql: str,
    changes: List[RewriteChange],
    get_columns_for_table: Callable[[str], List[str]],
) -> str:
    """Rewrite SELECT * FROM table to an explicit column list.

    This helper is designed to be used with either a real database-backed
    column resolver or a stub in tests.
    """

    pattern = re.compile(
        r"^\s*SELECT\s+\*\s+FROM\s+([A-Za-z0-9_]+)\b(.*)$",
        re.IGNORECASE | re.DOTALL,
    )

    match = pattern.match(sql)
    if not match:
        return sql

    table = match.group(1)
    tail = match.group(2)

    try:
        columns = get_columns_for_table(table)
    except Exception:
        return sql

    if not columns:
        return sql

    explicit_list = ", ".join(columns)
    changes.append(RewriteChange(description="Replaced SELECT * with explicit column list"))
    return f"SELECT {explicit_list} FROM {table}{tail}"


def _rewrite_year_function_on_column(sql: str, changes: List[RewriteChange]) -> str:
    """Rewrite YEAR(date_col) = 2024 to a sargable range predicate.

    Example:
      WHERE YEAR(order_date) = 2024
      -> WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01'

    This is a conservative, safe transformation that preserves
    correctness for DATE/DATETIME/TIMESTAMP columns and allows
    MySQL to use an index on the underlying column.
    """

    pattern = re.compile(
        r"YEAR\s*\(\s*([\w\.]+)\s*\)\s*=\s*(\d{4})",
        re.IGNORECASE,
    )

    def repl(match: re.Match) -> str:
        column = match.group(1)
        year_str = match.group(2)
        year = int(year_str)
        start = f"'{year:04d}-01-01'"
        end = f"'{year + 1:04d}-01-01'"
        changes.append(
            RewriteChange(
                description=(
                    "Rewrote YEAR() equality to date range predicate "
                    "to allow index usage"
                )
            )
        )
        return f"{column} >= {start} AND {column} < {end}"

    return pattern.sub(repl, sql)


def rewrite_query(sql: str, mode: str = "safe") -> RewriteResult:
    """Apply a set of heuristic rewrite rules to a SQL query.

    For the MVP we focus on a small, safe subset of transformations
    (NULL handling and OR->IN). More complex rules can be layered in
    future versions and gated by the `mode`.
    """

    mode = mode.lower()
    changes: List[RewriteChange] = []
    rewritten = sql

    # Safe rules
    rewritten = _rewrite_null_comparisons(rewritten, changes)
    rewritten = _rewrite_or_to_in(rewritten, changes)
    rewritten = _rewrite_year_function_on_column(rewritten, changes)

    # Placeholders for future moderate/aggressive rules.
    # In MVP they intentionally do nothing but allow the CLI flag.
    if mode in {"moderate", "aggressive"}:
        # TODO: implement additional rules like subquery-to-JOIN, JOIN reordering, etc.
        pass

    return RewriteResult(
        original_sql=sql,
        rewritten_sql=rewritten,
        mode=mode,
        changes=changes,
    )


def _get_columns_for_table_from_db(
    config: MySQLConnectionConfig,
    table: str,
) -> List[str]:
    # Protect against obviously unsafe identifiers.
    if not re.match(r"^[A-Za-z0-9_]+$", table):
        return []

    with connect(config) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SHOW COLUMNS FROM `{table}`")
        return [row[0] for row in cursor.fetchall()]


def rewrite_query_with_connection(
    sql: str,
    mode: str,
    host: Optional[str],
    port: Optional[int],
    user: Optional[str],
    password: Optional[str],
    database: Optional[str],
) -> RewriteResult:
    """Rewrite a query using both pure-SQL rules and schema-aware rules.

    This variant enables additional transformations (like SELECT * ->
    explicit columns) when a database connection and schema information
    are available. It is intended to be used by the CLI when DSN
    parameters are provided or available via config.
    """

    base_result = rewrite_query(sql=sql, mode=mode)
    if mode.lower() not in {"moderate", "aggressive"}:
        return base_result

    if not database:
        return base_result

    config = MySQLConnectionConfig(
        host=host or "localhost",
        port=port or 3306,
        user=user,
        password=password,
        database=database,
    )

    changes = list(base_result.changes)
    rewritten = _rewrite_select_star_with_columns(
        base_result.rewritten_sql,
        changes,
        lambda table: _get_columns_for_table_from_db(config, table),
    )

    return RewriteResult(
        original_sql=sql,
        rewritten_sql=rewritten,
        mode=mode.lower(),
        changes=changes,
    )
