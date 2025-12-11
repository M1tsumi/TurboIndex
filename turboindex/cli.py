import argparse
import sys
from typing import Optional

from . import __version__
from . import profiler
from . import rewriter
from . import index_recommender
from . import reporting
from . import config as app_config


def _add_connection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--host", required=False, help="MySQL host")
    parser.add_argument("--port", type=int, required=False, help="MySQL port")
    parser.add_argument("--user", required=False, help="MySQL user")
    parser.add_argument("--password", required=False, help="MySQL password")
    parser.add_argument("--database", required=False, help="MySQL database name")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="turboindex",
        description="TurboIndex - MySQL query optimization toolkit",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"turboindex {__version__}",
        help="Show TurboIndex version and exit",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # profile
    profile_p = subparsers.add_parser(
        "profile",
        help="Profile a SQL query against a MySQL database",
    )
    _add_connection_arguments(profile_p)
    profile_p.add_argument("--query", required=True, help="SQL query to profile")
    profile_p.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Number of times to run the query for timing (default: 3)",
    )
    profile_p.add_argument(
        "--format",
        choices=["table", "json", "csv", "html"],
        default="table",
        help="Output format for profiling results (default: table)",
    )
    profile_p.add_argument(
        "--mysql-version",
        dest="mysql_version",
        required=False,
        help=(
            "Logical MySQL/MariaDB version label, e.g. mysql_5.5, "
            "mysql_8.0, mariadb_10.x (used for version-aware analysis)"
        ),
    )

    # rewrite
    rewrite_p = subparsers.add_parser(
        "rewrite",
        help="Rewrite a SQL query using optimization rules",
    )
    _add_connection_arguments(rewrite_p)
    rewrite_p.add_argument("--query", required=True, help="SQL query to rewrite")
    rewrite_p.add_argument(
        "--mode",
        choices=["safe", "moderate", "aggressive"],
        default="safe",
        help="Rewrite safety mode (default: safe)",
    )
    rewrite_p.add_argument(
        "--format",
        choices=["diff", "json"],
        default="diff",
        help="Output format for rewrite results (default: diff)",
    )

    # recommend-indexes
    rec_p = subparsers.add_parser(
        "recommend-indexes",
        help="Analyze a query and recommend indexes",
    )
    _add_connection_arguments(rec_p)
    rec_p.add_argument("--query", required=True, help="SQL query to analyze")
    rec_p.add_argument(
        "--format",
        choices=["table", "json", "csv", "html"],
        default="table",
        help="Output format for index recommendations (default: table)",
    )
    rec_p.add_argument(
        "--mysql-version",
        dest="mysql_version",
        required=False,
        help=(
            "Logical MySQL/MariaDB version label, e.g. mysql_5.5, "
            "mysql_8.0, mariadb_10.x (used for version-aware analysis)"
        ),
    )

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "profile":
        cfg = app_config.load_config()
        result = profiler.profile_query(
            query=args.query,
            host=args.host or cfg.host or "localhost",
            port=args.port or cfg.port or 3306,
            user=args.user or cfg.user,
            password=args.password or cfg.password,
            database=args.database or cfg.database,
            iterations=args.iterations,
            mysql_version=args.mysql_version or cfg.mysql_version,
        )
        reporting.output_profile_result(result, fmt=args.format)
        return 0

    if args.command == "rewrite":
        # If connection/config is available and mode is moderate/aggressive,
        # enable schema-aware rewrites like SELECT * -> explicit columns.
        cfg = app_config.load_config()
        have_dsn = any([args.host, args.port, args.user, args.password, args.database,
                        cfg.host, cfg.port, cfg.user, cfg.password, cfg.database])

        if args.mode in {"moderate", "aggressive"} and have_dsn:
            result = rewriter.rewrite_query_with_connection(
                sql=args.query,
                mode=args.mode,
                host=args.host or cfg.host,
                port=args.port or cfg.port,
                user=args.user or cfg.user,
                password=args.password or cfg.password,
                database=args.database or cfg.database,
            )
        else:
            result = rewriter.rewrite_query(
                sql=args.query,
                mode=args.mode,
            )
        reporting.output_rewrite_result(result, fmt=args.format)
        return 0

    if args.command == "recommend-indexes":
        cfg = app_config.load_config()
        result = index_recommender.analyze_query_indexes(
            query=args.query,
            host=args.host or cfg.host or "localhost",
            port=args.port or cfg.port or 3306,
            user=args.user or cfg.user,
            password=args.password or cfg.password,
            database=args.database or cfg.database,
            mysql_version=args.mysql_version or cfg.mysql_version,
        )
        reporting.output_index_recommendations(result, fmt=args.format)
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
