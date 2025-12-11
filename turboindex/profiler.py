from __future__ import annotations

import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from .connection import MySQLConnectionConfig, connect


@dataclass
class QueryExecutionSample:
    iteration: int
    execution_time_ms: float
    rows_returned: Optional[int]


@dataclass
class ExplainRow:
    raw: Dict[str, Any]


@dataclass
class QueryProfileResult:
    query: str
    samples: List[QueryExecutionSample]
    explain_rows: List[ExplainRow]
    mysql_version: Optional[str]
    server_version: Optional[str]

    @property
    def average_time_ms(self) -> float:
        if not self.samples:
            return 0.0
        return sum(s.execution_time_ms for s in self.samples) / len(self.samples)

    @property
    def estimated_rows_examined(self) -> int:
        total = 0
        for row in self.explain_rows:
            try:
                total += int(row.raw.get("rows") or 0)
            except (TypeError, ValueError):
                continue
        return total

    @property
    def uses_filesort(self) -> bool:
        for row in self.explain_rows:
            extra = str(row.raw.get("Extra") or "").lower()
            if "filesort" in extra:
                return True
        return False

    @property
    def uses_temporary(self) -> bool:
        for row in self.explain_rows:
            extra = str(row.raw.get("Extra") or "").lower()
            if "temporary" in extra:
                return True
        return False

    @property
    def average_rows_returned(self) -> Optional[float]:
        values = [s.rows_returned for s in self.samples if s.rows_returned is not None]
        if not values:
            return None
        return float(sum(values) / len(values))

    @property
    def index_usage_summary(self) -> List[Dict[str, Any]]:
        summary: List[Dict[str, Any]] = []
        for row in self.explain_rows:
            key = row.raw.get("key")
            if not key:
                continue
            summary.append(
                {
                    "index": key,
                    "type": row.raw.get("type"),
                    "rows": row.raw.get("rows"),
                }
            )
        return summary

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "samples": [asdict(s) for s in self.samples],
            "average_time_ms": self.average_time_ms,
            "average_rows_returned": self.average_rows_returned,
            "estimated_rows_examined": self.estimated_rows_examined,
            "uses_filesort": self.uses_filesort,
            "uses_temporary": self.uses_temporary,
            "index_usage": self.index_usage_summary,
            "query_metrics": {
                "execution_time_ms": self.average_time_ms,
                "rows_examined": self.estimated_rows_examined,
                "rows_returned": self.average_rows_returned,
                "temp_tables_created": 1 if self.uses_temporary else 0,
                "filesort_operations": 1 if self.uses_filesort else 0,
                "index_usage": self.index_usage_summary,
            },
            "mysql_version": self.mysql_version,
            "server_version": self.server_version,
            "explain": [r.raw for r in self.explain_rows],
        }


def _run_explain(cursor, query: str) -> List[ExplainRow]:
    cursor.execute(f"EXPLAIN {query}")
    columns = [col[0] for col in cursor.description]
    rows = []
    for db_row in cursor.fetchall():
        rows.append(ExplainRow(raw={col: value for col, value in zip(columns, db_row)}))
    return rows


def profile_query(
    query: str,
    host: str,
    port: int,
    user: Optional[str],
    password: Optional[str],
    database: Optional[str],
    iterations: int = 3,
    mysql_version: Optional[str] = None,
) -> QueryProfileResult:
    """Profile a query by executing it multiple times and collecting timings.

    This function intentionally keeps metrics simple for the MVP: execution
    time, rows returned, and raw EXPLAIN output.
    """

    config = MySQLConnectionConfig(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )

    samples: List[QueryExecutionSample] = []

    with connect(config) as conn:
        cursor = conn.cursor()

        # Detect server version if possible
        server_version: Optional[str] = None
        try:
            cursor.execute("SELECT VERSION()")
            row = cursor.fetchone()
            if row:
                server_version = str(row[0])
        except Exception:
            server_version = None

        # Run EXPLAIN once
        explain_rows = _run_explain(cursor, query)

        # Execute the query multiple times
        for i in range(iterations):
            start = time.perf_counter()
            cursor.execute(query)
            try:
                rows = cursor.fetchall()
                rows_returned = len(rows)
            except Exception:
                rows_returned = None
            end = time.perf_counter()

            samples.append(
                QueryExecutionSample(
                    iteration=i + 1,
                    execution_time_ms=(end - start) * 1000.0,
                    rows_returned=rows_returned,
                )
            )

    return QueryProfileResult(
        query=query,
        samples=samples,
        explain_rows=explain_rows,
        mysql_version=mysql_version,
        server_version=server_version,
    )
