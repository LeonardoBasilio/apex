"""Applies this lane's own DDL, and nothing else's -- mirrors upstream's
rule that the bronze/store lane owns all schema application
(`create_schema: false` everywhere downstream). `engine/`, `serve/`,
`memory/`, `verify/` only SELECT/INSERT into tables this module creates.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, List, Protocol

DDL_DIR = Path(__file__).resolve().parent.parent.parent / "ddl"


class DDLConnection(Protocol):
    def execute(self, sql: str) -> Any: ...

    def commit(self) -> None: ...


def migration_files(ddl_dir: Path = DDL_DIR) -> List[Path]:
    """Sorted by filename, so `001_`, `002_`, ... apply in order -- the same
    numbered-migration convention as upstream's infra/clickhouse/init.d/."""
    return sorted(ddl_dir.glob("*.sql"))


def apply_ddl(connection: DDLConnection, ddl_dir: Path = DDL_DIR) -> List[str]:
    """Executes each migration file in order and commits after each one, so
    a failure partway through leaves every prior migration applied rather
    than rolling all of them back together. Returns the filenames applied,
    for the caller to log."""
    applied = []
    for sql_file in migration_files(ddl_dir):
        connection.execute(sql_file.read_text(encoding="utf-8"))
        connection.commit()
        applied.append(sql_file.name)
    return applied
