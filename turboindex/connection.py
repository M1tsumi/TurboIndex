from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import mysql.connector


@dataclass
class MySQLConnectionConfig:
    host: str = "localhost"
    port: int = 3306
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None


def connect(config: MySQLConnectionConfig) -> mysql.connector.MySQLConnection:
    """Create a new MySQL connection from the given configuration.

    The caller is responsible for closing the returned connection.
    """

    conn = mysql.connector.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        database=config.database,
    )
    return conn
