from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:  # Python 3.11+
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


@dataclass
class TurboIndexConfig:
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    mysql_version: Optional[str] = None


def _load_toml_file(path: Path) -> dict:
    if not path.is_file():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def load_config() -> TurboIndexConfig:
    """Load TurboIndex configuration from turboindex.toml and environment.

    Precedence (lowest to highest):
      1. turboindex.toml in the current working directory
      2. Environment variables (TURBOINDEX_*)
    Command-line flags still take ultimate precedence in the CLI.
    """

    cfg = TurboIndexConfig()

    # 1) File-based config: ./turboindex.toml
    data: dict = {}
    cwd_path = Path.cwd() / "turboindex.toml"
    data = _load_toml_file(cwd_path)

    mysql_section = data.get("mysql", {})
    if isinstance(mysql_section, dict):
        cfg.host = mysql_section.get("host") or cfg.host
        port = mysql_section.get("port")
        if isinstance(port, int):
            cfg.port = port
        cfg.user = mysql_section.get("user") or cfg.user
        cfg.password = mysql_section.get("password") or cfg.password
        cfg.database = mysql_section.get("database") or cfg.database
        cfg.mysql_version = mysql_section.get("version") or cfg.mysql_version

    # 2) Environment variables override file
    host = os.getenv("TURBOINDEX_HOST")
    if host:
        cfg.host = host

    port_env = os.getenv("TURBOINDEX_PORT")
    if port_env:
        try:
            cfg.port = int(port_env)
        except ValueError:
            pass

    user = os.getenv("TURBOINDEX_USER")
    if user:
        cfg.user = user

    password = os.getenv("TURBOINDEX_PASSWORD")
    if password is not None:
        cfg.password = password

    database = os.getenv("TURBOINDEX_DATABASE")
    if database:
        cfg.database = database

    mysql_version = os.getenv("TURBOINDEX_MYSQL_VERSION")
    if mysql_version:
        cfg.mysql_version = mysql_version

    return cfg
