from __future__ import annotations

from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[3]
DEFAULT_SQLITE_DB = ROOT_DIR / "db.sqlite"
