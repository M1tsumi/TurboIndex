from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

from .connection import MySQLConnectionConfig, connect


@dataclass
class IndexRecommendation:
    table: str
    suggested_index_name: str
    columns: List[str]
    reason: str


@dataclass
class IndexAnalysisResult:
    query: str
    recommendations: List[IndexRecommendation]
    explain_rows: List[Dict[str, Any]]
    mysql_version: Optional[str]
    server_version: Optional[str]
    health_score: int
    issues: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "recommendations": [asdict(r) for r in self.recommendations],
            "mysql_version": self.mysql_version,
            "server_version": self.server_version,
            "health_score": self.health_score,
            "issues": self.issues,
            "explain": self.explain_rows,
        }


def _collect_explain(cursor, query: str) -> List[Dict[str, Any]]:
    cursor.execute(f"EXPLAIN {query}")
    columns = [col[0] for col in cursor.description]
    results: List[Dict[str, Any]] = []
    for row in cursor.fetchall():
        results.append({col: value for col, value in zip(columns, row)})
    return results


def _suggest_index_name(table: str, columns: List[str]) -> str:
    cols_part = "_".join(columns[:3])
    return f"idx_{table}_{cols_part}"


def _analyze_explain_for_indexes(explain_rows: List[Dict[str, Any]]) -> List[IndexRecommendation]:
    recommendations: List[IndexRecommendation] = []

    for row in explain_rows:
        table = row.get("table")
        access_type = str(row.get("type") or "").lower()
        possible_keys = row.get("possible_keys")
        key = row.get("key")
        extra = str(row.get("Extra") or "")

        # Very simple heuristic for MVP:
        # - full table scan (type=ALL)
        # - no index chosen
        # - has WHERE filtering (Using where)
        if access_type == "all" and not key and "Using where" in extra:
            where_columns: List[str] = []
            # We do not yet attempt to fully parse the query; instead we
            # just flag the table for review.
            if possible_keys:
                continue

            if not table:
                continue

            # Assume a simple single-column index on the primary filter
            # column would help; we cannot know the column name without
            # parsing the SQL, so we emit a generic recommendation.
            where_columns = ["<choose_filter_column>"]

            recommendations.append(
                IndexRecommendation(
                    table=table,
                    suggested_index_name=_suggest_index_name(table, where_columns),
                    columns=where_columns,
                    reason=(
                        "Full table scan detected with WHERE filtering and no index; "
                        "consider adding an index on the main filter column."
                    ),
                )
            )

    return recommendations


def _compute_index_health(
    explain_rows: List[Dict[str, Any]],
    recommendations: List[IndexRecommendation],
) -> Tuple[int, List[str]]:
    """Compute a simple 0-100 index health score and list of issues.

    This is intentionally conservative and based only on EXPLAIN output
    for the current query plus the generated recommendations.
    """

    score = 100
    issues: List[str] = []

    for row in explain_rows:
        table = row.get("table") or "?"
        access_type = str(row.get("type") or "").lower()
        extra = str(row.get("Extra") or "").lower()

        if access_type == "all":
            score -= 20
            issues.append(f"Full table scan on {table} (type=ALL)")
        elif access_type == "index":
            score -= 5
            issues.append(f"Sequential index scan on {table} (type=index)")

        if "filesort" in extra:
            score -= 10
            issues.append(f"Filesort required for {table}")

        if "temporary" in extra:
            score -= 10
            issues.append(f"Temporary table used for {table}")

    if recommendations:
        penalty = min(5 * len(recommendations), 20)
        score -= penalty
        issues.append(f"{len(recommendations)} index recommendation(s) suggested")

    score = max(0, min(100, score))
    return score, issues


def analyze_query_indexes(
    query: str,
    host: str,
    port: int,
    user: Optional[str],
    password: Optional[str],
    database: Optional[str],
    mysql_version: Optional[str] = None,
) -> IndexAnalysisResult:
    config = MySQLConnectionConfig(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )

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

        explain_rows = _collect_explain(cursor, query)

    recommendations = _analyze_explain_for_indexes(explain_rows)
    health_score, issues = _compute_index_health(explain_rows, recommendations)

    return IndexAnalysisResult(
        query=query,
        recommendations=recommendations,
        explain_rows=explain_rows,
        mysql_version=mysql_version,
        server_version=server_version,
        health_score=health_score,
        issues=issues,
    )
