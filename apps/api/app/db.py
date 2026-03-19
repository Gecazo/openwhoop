from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import DEFAULT_SQLITE_DB


def resolve_db_path() -> Path:
    configured = os.getenv("OPENWHOOP_SQLITE_PATH")
    return Path(configured).expanduser().resolve() if configured else DEFAULT_SQLITE_DB


@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    db_path = resolve_db_path()
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row

    try:
        yield connection
    finally:
        connection.close()
