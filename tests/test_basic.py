from turboindex import __version__
from turboindex import rewriter
from turboindex import index_recommender
from turboindex import profiler


def test_version_string():
    assert isinstance(__version__, str)
    assert __version__


def test_rewriter_null_handling():
    sql = "SELECT * FROM users WHERE deleted != NULL"
    result = rewriter.rewrite_query(sql, mode="safe")
    assert "IS NOT NULL" in result.rewritten_sql


def test_rewriter_or_to_in():
    sql = "SELECT * FROM t WHERE status = 'a' OR status = 'b' OR status = 'c'"
    result = rewriter.rewrite_query(sql, mode="safe")
    assert "IN (" in result.rewritten_sql


def test_rewriter_year_function_to_range():
    sql = "SELECT * FROM orders WHERE YEAR(created_at) = 2024"
    result = rewriter.rewrite_query(sql, mode="safe")
    assert "created_at >= '2024-01-01'" in result.rewritten_sql
    assert "created_at < '2025-01-01'" in result.rewritten_sql


def test_rewriter_select_star_with_columns_helper():
    def get_columns(table: str) -> list[str]:
        assert table == "users"
        return ["id", "name", "email"]

    sql = "SELECT * FROM users WHERE active = 1"
    changes: list[rewriter.RewriteChange] = []
    rewritten = rewriter._rewrite_select_star_with_columns(sql, changes, get_columns)
    assert "SELECT id, name, email FROM users" in rewritten
    assert any("SELECT *" in c.description or "explicit column" in c.description for c in changes)


def test_index_health_penalizes_full_scan():
    explain_rows = [
        {"table": "orders", "type": "ALL", "Extra": "Using where"},
    ]
    recommendations: list[index_recommender.IndexRecommendation] = []
    score, issues = index_recommender._compute_index_health(explain_rows, recommendations)
    assert score < 100
    assert any("Full table scan" in issue for issue in issues)


def test_query_profile_result_metrics():
    explain_rows = [
        profiler.ExplainRow(raw={"rows": 42, "Extra": "Using filesort; Using temporary"}),
    ]
    samples = [
        profiler.QueryExecutionSample(iteration=1, execution_time_ms=10.0, rows_returned=5),
        profiler.QueryExecutionSample(iteration=2, execution_time_ms=20.0, rows_returned=15),
    ]
    result = profiler.QueryProfileResult(
        query="SELECT * FROM t",
        samples=samples,
        explain_rows=explain_rows,
        mysql_version=None,
        server_version=None,
    )

    data = result.to_dict()
    assert data["estimated_rows_examined"] == 42
    assert data["uses_filesort"] is True
    assert data["uses_temporary"] is True
    assert data["query_metrics"]["filesort_operations"] == 1
