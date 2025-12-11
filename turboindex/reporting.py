from __future__ import annotations

import csv
import io
import json
from typing import Any, Dict, List

from rich.console import Console
from rich.table import Table

from .profiler import QueryProfileResult
from .rewriter import RewriteResult
from .index_recommender import IndexAnalysisResult


_console = Console()


def output_profile_result(result: QueryProfileResult, fmt: str = "table") -> None:
    if fmt == "json":
        print(json.dumps(result.to_dict(), indent=2))
        return

    if fmt == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["iteration", "execution_time_ms", "rows_returned"])
        for sample in result.samples:
            writer.writerow(
                [
                    sample.iteration,
                    f"{sample.execution_time_ms:.4f}",
                    "" if sample.rows_returned is None else sample.rows_returned,
                ]
            )
        print(output.getvalue().rstrip("\n"))
        return

    if fmt == "html":
        rows_html = []
        for sample in result.samples:
            rows_html.append(
                "<tr>"
                f"<td>{sample.iteration}</td>"
                f"<td>{sample.execution_time_ms:.4f}</td>"
                f"<td>{'' if sample.rows_returned is None else sample.rows_returned}</td>"
                "</tr>"
            )
        html = (
            "<table>"\
            "<thead><tr><th>Iteration</th><th>Time (ms)</th><th>Rows Returned</th></tr></thead>"\
            f"<tbody>{''.join(rows_html)}</tbody>"\
            "</table>"\
        )
        print(html)
        return

    table = Table(title="Query Profile")
    table.add_column("Iteration", justify="right")
    table.add_column("Time (ms)", justify="right")
    table.add_column("Rows Returned", justify="right")

    for sample in result.samples:
        table.add_row(
            str(sample.iteration),
            f"{sample.execution_time_ms:.2f}",
            "-" if sample.rows_returned is None else str(sample.rows_returned),
        )

    _console.print(table)
    _console.print(f"Average time: {result.average_time_ms:.2f} ms")
    _console.print(
        f"Estimated rows examined (from EXPLAIN): {result.estimated_rows_examined}"
    )
    if result.uses_filesort or result.uses_temporary:
        flags = []
        if result.uses_filesort:
            flags.append("filesort")
        if result.uses_temporary:
            flags.append("temporary table")
        _console.print("Execution flags: " + ", ".join(flags))


def output_rewrite_result(result: RewriteResult, fmt: str = "diff") -> None:
    if fmt == "json":
        print(json.dumps(result.to_dict(), indent=2))
        return

    # Simple textual diff-like output for MVP
    _console.print("[bold]Original SQL:[/bold]")
    _console.print(result.original_sql)
    _console.print()
    _console.print("[bold]Rewritten SQL:[/bold]")
    _console.print(result.rewritten_sql)
    _console.print()

    if result.changes:
        _console.print("[bold]Changes applied:[/bold]")
        for change in result.changes:
            _console.print(f"- {change.description}")
    else:
        _console.print("No changes were applied; query already conforms to rules for this mode.")


def output_index_recommendations(result: IndexAnalysisResult, fmt: str = "table") -> None:
    if fmt == "json":
        print(json.dumps(result.to_dict(), indent=2))
        return

    if not result.recommendations:
        _console.print("No index recommendations based on current heuristics.")
        return

    if fmt == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["table", "index_name", "columns", "reason"])
        for rec in result.recommendations:
            writer.writerow(
                [
                    rec.table,
                    rec.suggested_index_name,
                    ", ".join(rec.columns),
                    rec.reason,
                ]
            )
        print(output.getvalue().rstrip("\n"))
        return

    if fmt == "html":
        rows_html = []
        for rec in result.recommendations:
            rows_html.append(
                "<tr>"
                f"<td>{rec.table}</td>"
                f"<td>{rec.suggested_index_name}</td>"
                f"<td>{', '.join(rec.columns)}</td>"
                f"<td>{rec.reason}</td>"
                "</tr>"
            )
        html = (
            "<table>"\
            "<thead><tr><th>Table</th><th>Index Name</th><th>Columns</th><th>Reason</th></tr></thead>"\
            f"<tbody>{''.join(rows_html)}</tbody>"\
            "</table>"\
        )
        print(html)
        return

    table = Table(title="Index Recommendations")
    table.add_column("Table")
    table.add_column("Index Name")
    table.add_column("Columns")
    table.add_column("Reason")

    for rec in result.recommendations:
        table.add_row(
            rec.table,
            rec.suggested_index_name,
            ", ".join(rec.columns),
            rec.reason,
        )

    _console.print(table)
    _console.print(f"Index Health: {result.health_score}/100")
    if result.issues:
        _console.print("Issues detected:")
        for issue in result.issues:
            _console.print(f"- {issue}")
