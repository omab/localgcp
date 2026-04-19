"""DuckDB-backed Cloud Spanner engine.

Each Spanner instance maps to engine metadata only.
Each Spanner database maps to a DuckDB schema named "{instance_id}__{database_id}".
Tables within a database map to DuckDB tables inside that schema.

DDL rewriting: Spanner DDL uses Spanner-specific types (INT64, STRING(MAX), etc.)
and a trailing PRIMARY KEY clause. _rewrite_ddl() converts these to DuckDB form.

SQL rewriting: Spanner SQL uses @param_name parameters and unqualified table names.
_rewrite_spanner_sql() converts these to DuckDB-compatible form.
"""

from __future__ import annotations

import base64
import json
import re
import threading
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb

from cloudbox.config import settings

# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------

_SPANNER_TO_DUCK: dict[str, str] = {
    "STRING": "VARCHAR",
    "BYTES": "BLOB",
    "INT64": "BIGINT",
    "FLOAT64": "DOUBLE",
    "FLOAT32": "FLOAT",
    "BOOL": "BOOLEAN",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMPTZ",
    "DATE": "DATE",
    "JSON": "JSON",
    "NUMERIC": "DECIMAL(38,9)",
}

_DUCK_TO_SPANNER: dict[str, str] = {
    "VARCHAR": "STRING",
    "TEXT": "STRING",
    "BLOB": "BYTES",
    "BIGINT": "INT64",
    "INTEGER": "INT64",
    "INT": "INT64",
    "INT64": "INT64",
    "INT32": "INT64",
    "HUGEINT": "INT64",
    "DOUBLE": "FLOAT64",
    "FLOAT": "FLOAT32",
    "REAL": "FLOAT32",
    "BOOLEAN": "BOOL",
    "BOOL": "BOOL",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP",
    "TIMESTAMPTZ": "TIMESTAMP",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "JSON": "JSON",
    "DECIMAL": "NUMERIC",
}


def _spanner_type_to_duck(type_str: str) -> str:
    """Convert a Spanner column type string to a DuckDB type string."""
    t = type_str.strip()
    # ARRAY<T> → JSON
    if re.match(r"ARRAY\s*<", t, re.IGNORECASE):
        return "JSON"
    # Strip size/precision: STRING(MAX), STRING(100), BYTES(MAX)
    base = re.sub(r"\s*\([^)]*\)", "", t).strip().upper()
    return _SPANNER_TO_DUCK.get(base, "VARCHAR")


def _duck_type_to_spanner(duck_type: str) -> str:
    t = duck_type.upper().strip()
    if "(" in t:
        t = t[: t.index("(")].strip()
    return _DUCK_TO_SPANNER.get(t, "STRING")


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _serialize_spanner(v: Any, spanner_type_code: str = "STRING") -> Any:
    """Serialize a Python value to Spanner wire format."""
    if v is None:
        return None
    code = spanner_type_code.upper()
    if code in ("INT64",):
        return str(int(v)) if not isinstance(v, str) else v
    if code in ("FLOAT64", "FLOAT32"):
        return str(float(v)) if not isinstance(v, str) else v
    if code == "BOOL":
        return bool(v)
    if code == "BYTES":
        if isinstance(v, (bytes, bytearray, memoryview)):
            return base64.b64encode(bytes(v)).decode()
        return str(v)
    if code == "NUMERIC":
        return str(v)
    if code in ("TIMESTAMP",):
        if isinstance(v, datetime):
            return v.isoformat().replace("+00:00", "Z")
        return str(v)
    if code == "DATE":
        if isinstance(v, date):
            return v.isoformat()
        return str(v)
    if code == "JSON":
        if isinstance(v, str):
            return v
        return json.dumps(v)
    # STRING and fallback
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).decode("utf-8", errors="replace")
    return str(v) if not isinstance(v, str) else v


# ---------------------------------------------------------------------------
# DDL rewriter helpers
# ---------------------------------------------------------------------------


def _split_col_defs(section: str) -> list[str]:
    """Split column definitions by commas, respecting nested parens and <> brackets."""
    parts: list[str] = []
    depth = 0
    angle = 0
    buf: list[str] = []
    for ch in section:
        if ch == "(":
            depth += 1
            buf.append(ch)
        elif ch == ")":
            depth -= 1
            buf.append(ch)
        elif ch == "<":
            angle += 1
            buf.append(ch)
        elif ch == ">":
            angle -= 1
            buf.append(ch)
        elif ch == "," and depth == 0 and angle == 0:
            s = "".join(buf).strip()
            if s:
                parts.append(s)
            buf = []
        else:
            buf.append(ch)
    s = "".join(buf).strip()
    if s:
        parts.append(s)
    return parts


def _find_matching_paren(s: str, start: int) -> int:
    """Return index of the closing ')' matching the '(' at s[start]."""
    depth = 0
    for i in range(start, len(s)):
        if s[i] == "(":
            depth += 1
        elif s[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    return len(s) - 1


def _parse_col_def(col_def: str) -> tuple[str, str, bool] | None:
    """Parse a single column definition. Returns (col_name, spanner_type, not_null) or None."""
    # Strip OPTIONS (...)
    d = re.sub(r"\s+OPTIONS\s*\([^)]*\)", "", col_def, flags=re.IGNORECASE).strip()
    # Remove trailing NOT NULL
    not_null = False
    if re.search(r"\bNOT\s+NULL\s*$", d, re.IGNORECASE):
        not_null = True
        d = re.sub(r"\s+NOT\s+NULL\s*$", "", d, flags=re.IGNORECASE).strip()
    # Split into name and type (name is first word, possibly backtick-quoted)
    m = re.match(r"`?(\w+)`?\s+(.+)", d)
    if not m:
        return None
    return m.group(1), m.group(2).strip(), not_null


def _rewrite_create_table(stmt: str, schema: str) -> tuple[str, list[str]]:
    """Rewrite a Spanner CREATE TABLE statement to DuckDB.
    Returns (duckdb_sql, pk_columns).
    """
    # Normalize whitespace while preserving structure
    stmt = re.sub(r"\s+", " ", stmt.strip())

    # Extract table name
    m = re.match(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s*\(", stmt, re.IGNORECASE)
    if not m:
        raise ValueError(f"Cannot parse CREATE TABLE: {stmt[:100]}")
    table_name = m.group(1)

    # Find the opening ( of the column list
    open_idx = stmt.index("(", m.start())
    # Find matching closing )
    close_idx = _find_matching_paren(stmt, open_idx)

    col_section = stmt[open_idx + 1 : close_idx]
    remainder = stmt[close_idx + 1 :].strip()

    # Extract PRIMARY KEY columns from remainder (before any INTERLEAVE clause)
    pk_cols: list[str] = []
    pk_match = re.match(r"PRIMARY\s+KEY\s*\(([^)]+)\)", remainder, re.IGNORECASE)
    if pk_match:
        raw_pk = pk_match.group(1)
        # Each PK entry may have ASC/DESC suffix
        for part in raw_pk.split(","):
            col = part.strip().split()[0].strip("`")
            if col:
                pk_cols.append(col)

    # Parse and rewrite each column definition
    col_defs = _split_col_defs(col_section)
    rewritten: list[str] = []

    for col_def in col_defs:
        cd = col_def.strip()
        if not cd:
            continue
        # Skip inline PRIMARY KEY / FOREIGN KEY / CHECK constraints
        if re.match(r"PRIMARY\s+KEY", cd, re.IGNORECASE):
            continue
        if re.match(r"(CONSTRAINT\s+\w+\s+)?FOREIGN\s+KEY", cd, re.IGNORECASE):
            continue
        if re.match(r"(CONSTRAINT\s+\w+\s+)?CHECK\s*\(", cd, re.IGNORECASE):
            continue

        parsed = _parse_col_def(cd)
        if parsed is None:
            continue
        col_name, spanner_type, not_null = parsed
        duck_type = _spanner_type_to_duck(spanner_type)
        nn = " NOT NULL" if not_null else ""
        rewritten.append(f'"{col_name}" {duck_type}{nn}')

    # Append PRIMARY KEY constraint inside column list
    if pk_cols:
        pk_quoted = ", ".join(f'"{c}"' for c in pk_cols)
        rewritten.append(f"PRIMARY KEY ({pk_quoted})")

    cols_str = ", ".join(rewritten)
    duckdb_sql = f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" ({cols_str})'
    return duckdb_sql, pk_cols


def _rewrite_ddl(stmt: str, schema: str) -> tuple[str, list[str]]:
    """Rewrite a single Spanner DDL statement to DuckDB SQL.
    Returns (duckdb_sql, pk_cols) where pk_cols is non-empty only for CREATE TABLE.
    """
    s = stmt.strip()
    upper = s.upper().lstrip()

    if upper.startswith("CREATE TABLE"):
        return _rewrite_create_table(s, schema)

    if upper.startswith("DROP TABLE"):
        m = re.match(r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?`?(\w+)`?", s, re.IGNORECASE)
        if m:
            return f'DROP TABLE IF EXISTS "{schema}"."{m.group(1)}"', []
        return s, []

    if upper.startswith("ALTER TABLE"):
        # Handle ADD COLUMN
        m = re.match(
            r"ALTER\s+TABLE\s+`?(\w+)`?\s+ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s+(.+)",
            s,
            re.IGNORECASE,
        )
        if m:
            table_name = m.group(1)
            col_name = m.group(2)
            rest = m.group(3).strip()
            not_null = False
            if re.search(r"\bNOT\s+NULL\s*", rest, re.IGNORECASE):
                not_null = True
                rest = re.sub(r"\s+NOT\s+NULL", "", rest, flags=re.IGNORECASE).strip()
            rest = re.sub(r"\s+OPTIONS\s*\([^)]*\)", "", rest, flags=re.IGNORECASE).strip()
            duck_type = _spanner_type_to_duck(rest)
            nn = " NOT NULL" if not_null else ""
            return (
                f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN IF NOT EXISTS'
                f' "{col_name}" {duck_type}{nn}',
                [],
            )
        # Handle DROP COLUMN
        m = re.match(
            r"ALTER\s+TABLE\s+`?(\w+)`?\s+DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?`?(\w+)`?",
            s,
            re.IGNORECASE,
        )
        if m:
            return f'ALTER TABLE "{schema}"."{m.group(1)}" DROP COLUMN IF EXISTS "{m.group(2)}"', []
        return s, []

    if (
        upper.startswith("CREATE INDEX")
        or upper.startswith("CREATE UNIQUE INDEX")
        or upper.startswith("DROP INDEX")
    ):
        # DuckDB supports CREATE INDEX but not all Spanner index syntax; best-effort
        # Strip STORING (...) and INTERLEAVE clauses
        s = re.sub(r"\s+STORING\s*\([^)]*\)", "", s, flags=re.IGNORECASE)
        s = re.sub(r",?\s*INTERLEAVE\s+IN\s+\w+", "", s, flags=re.IGNORECASE)
        # Rewrite table/index identifiers to schema-qualified
        m = re.match(
            r"(CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s+ON\s+)`?(\w+)`?\s*\((.+)\)",
            s,
            re.IGNORECASE | re.DOTALL,
        )
        if m:
            idx_name = m.group(2)
            tbl_name = m.group(3)
            cols = m.group(4).strip()
            # Quote each column name in the index
            cols_q = ", ".join(
                f'"{c.strip().strip("`").split()[0]}"'
                + (" " + " ".join(c.strip().split()[1:]) if len(c.strip().split()) > 1 else "")
                for c in cols.split(",")
            )
            return (
                f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{schema}"."{tbl_name}" ({cols_q})',
                [],
            )
        return s, []

    # Fallback: return as-is
    return s, []


# ---------------------------------------------------------------------------
# SQL rewriter
# ---------------------------------------------------------------------------

_SELECT_KEYWORDS = ("SELECT", "WITH", "VALUES", "SHOW", "DESCRIBE", "EXPLAIN", "PRAGMA")


def _is_select(sql: str) -> bool:
    stripped = sql.strip().lstrip("(").upper()
    return any(stripped.startswith(kw) for kw in _SELECT_KEYWORDS)


def _rewrite_spanner_sql(sql: str, schema: str) -> tuple[str, list[str]]:
    """Rewrite Spanner SQL to DuckDB SQL.
    Returns (duckdb_sql, param_names_in_order).
    """
    param_names: list[str] = []

    def _replace_param(m: re.Match) -> str:
        param_names.append(m.group(1))
        return "?"

    # @param_name → ?
    sql = re.sub(r"@(\w+)", _replace_param, sql)

    # Backtick identifiers → double-quoted
    sql = re.sub(r"`([^`]+)`", lambda m: f'"{m.group(1)}"', sql)

    # Table name qualification: FROM/JOIN/UPDATE/INSERT INTO/DELETE FROM <table>
    # Only qualify simple identifiers (not already qualified with a dot or already quoted)
    def _qualify(m: re.Match) -> str:
        kw = m.group(1)
        tbl = m.group(2)
        return f'{kw} "{schema}"."{tbl}"'

    sql = re.sub(
        r"\b(FROM|JOIN|INNER\s+JOIN|LEFT\s+(?:OUTER\s+)?JOIN|RIGHT\s+(?:OUTER\s+)?JOIN"
        r"|FULL\s+(?:OUTER\s+)?JOIN|CROSS\s+JOIN)\s+([A-Za-z_]\w*)(?!\s*[.\"])",
        _qualify,
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\b(UPDATE)\s+([A-Za-z_]\w*)(?!\s*[.\"])",
        _qualify,
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\b(INSERT\s+INTO)\s+([A-Za-z_]\w*)(?!\s*[.\"])",
        _qualify,
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\b(DELETE\s+FROM)\s+([A-Za-z_]\w*)(?!\s*[.\"])",
        _qualify,
        sql,
        flags=re.IGNORECASE,
    )

    return sql, param_names


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class SpannerEngine:
    """Single DuckDB connection backing the Cloud Spanner emulator."""

    def __init__(self) -> None:
        if settings.data_dir:
            self._db_path = str(Path(settings.data_dir) / "spanner.duckdb")
        else:
            self._db_path = ":memory:"
        self._conn = duckdb.connect(self._db_path)
        self._lock = threading.Lock()

        # Metadata dicts
        self._instances: dict[str, dict] = {}  # keyed by "{project}/{instance}"
        self._databases: dict[str, dict] = {}  # keyed by "{project}/{instance}/{database}"
        self._sessions: dict[str, dict] = {}  # keyed by session full resource name
        self._transactions: dict[str, dict] = {}  # keyed by transaction id
        self._operations: dict[str, dict] = {}  # keyed by operation name

        # DDL tracking per database (for getDatabaseDdl)
        self._ddl_statements: dict[str, list[str]] = {}  # key: "{project}/{instance}/{database}"

        # Primary key columns per table, keyed by "{schema}"."{table}"
        self._table_pks: dict[str, list[str]] = {}

    # -------------------------------------------------------------------------
    # Low-level helpers
    # -------------------------------------------------------------------------

    def _exec(self, sql: str, params: list | None = None) -> None:
        with self._lock:
            self._conn.execute(sql, params or [])

    def _select(
        self, sql: str, params: list | None = None
    ) -> tuple[list[tuple[str, str]], list[tuple]]:
        """Execute a SELECT and return ([(col_name, duck_type), ...], rows)."""
        with self._lock:
            self._conn.execute(sql, params or [])
            desc = self._conn.description or []
            rows = self._conn.fetchall()
        columns = [(d[0], str(d[1]) if d[1] is not None else "VARCHAR") for d in desc]
        return columns, rows

    def _schema_name(self, instance_id: str, database_id: str) -> str:
        return f"{instance_id}__{database_id}"

    def reset(self) -> None:
        """Wipe all state — used by tests and the admin UI."""
        with self._lock:
            self._conn.close()
            self._conn = duckdb.connect(self._db_path)
        self._instances.clear()
        self._databases.clear()
        self._sessions.clear()
        self._transactions.clear()
        self._operations.clear()
        self._ddl_statements.clear()
        self._table_pks.clear()

    # -------------------------------------------------------------------------
    # Instance configs (stub — SDK sometimes queries these)
    # -------------------------------------------------------------------------

    def list_instance_configs(self, project: str) -> list[dict]:
        return [
            {
                "name": f"projects/{project}/instanceConfigs/regional-us-central1",
                "displayName": "US Central1",
                "replicas": [{"location": "us-central1", "type": "READ_WRITE"}],
            }
        ]

    # -------------------------------------------------------------------------
    # Instances
    # -------------------------------------------------------------------------

    def create_instance(self, project: str, instance_id: str, body: dict) -> dict:
        key = f"{project}/{instance_id}"
        if key in self._instances:
            raise ValueError(f"Instance already exists: {instance_id}")
        meta = {
            "name": f"projects/{project}/instances/{instance_id}",
            "config": body.get(
                "config", f"projects/{project}/instanceConfigs/regional-us-central1"
            ),
            "displayName": body.get("displayName", instance_id),
            "nodeCount": body.get("nodeCount", 1),
            "processingUnits": body.get("processingUnits", 1000),
            "state": "READY",
            "labels": body.get("labels", {}),
            "createTime": _now(),
            "updateTime": _now(),
        }
        self._instances[key] = meta
        op_name = f"projects/{project}/instances/{instance_id}/operations/{uuid.uuid4()}"
        op = {
            "name": op_name,
            "done": True,
            "response": {
                "@type": "type.googleapis.com/google.spanner.admin.instance.v1.Instance",
                **meta,
            },
        }
        self._operations[op_name] = op
        return op

    def get_instance(self, project: str, instance_id: str) -> dict | None:
        return self._instances.get(f"{project}/{instance_id}")

    def list_instances(self, project: str) -> list[dict]:
        prefix = f"{project}/"
        return [v for k, v in self._instances.items() if k.startswith(prefix)]

    def update_instance(self, project: str, instance_id: str, body: dict) -> dict:
        key = f"{project}/{instance_id}"
        meta = self._instances.get(key)
        if meta is None:
            raise ValueError(f"Instance not found: {instance_id}")
        instance = body.get("instance", body)
        for field in ("displayName", "nodeCount", "processingUnits", "labels"):
            if field in instance:
                meta[field] = instance[field]
        meta["updateTime"] = _now()
        return meta

    def delete_instance(self, project: str, instance_id: str) -> bool:
        key = f"{project}/{instance_id}"
        if key not in self._instances:
            return False
        del self._instances[key]
        # Also delete all databases under this instance
        db_prefix = f"{project}/{instance_id}/"
        for k in list(self._databases.keys()):
            if k.startswith(db_prefix):
                self._drop_database_schema(k)
                del self._databases[k]
        return True

    # -------------------------------------------------------------------------
    # Databases
    # -------------------------------------------------------------------------

    def _drop_database_schema(self, db_key: str) -> None:
        """Drop the DuckDB schema for a database key."""
        db = self._databases.get(db_key)
        if db is None:
            return
        parts = db_key.split("/")
        if len(parts) >= 4:
            instance_id = parts[1]
            database_id = parts[3]
            schema = self._schema_name(instance_id, database_id)
            try:
                self._exec(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            except Exception:
                pass
            # Clean up PK tracking
            prefix = f'"{schema}".'
            for k in list(self._table_pks.keys()):
                if k.startswith(prefix):
                    del self._table_pks[k]

    def create_database(
        self, project: str, instance_id: str, database_id: str, extra_statements: list[str]
    ) -> dict:
        inst_key = f"{project}/{instance_id}"
        if inst_key not in self._instances:
            raise ValueError(f"Instance not found: {instance_id}")
        db_key = f"{project}/{instance_id}/databases/{database_id}"
        if db_key in self._databases:
            raise ValueError(f"Database already exists: {database_id}")

        schema = self._schema_name(instance_id, database_id)
        self._exec(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

        now = _now()
        meta = {
            "name": f"projects/{project}/instances/{instance_id}/databases/{database_id}",
            "state": "READY",
            "createTime": now,
            "earliestVersionTime": now,
            "versionRetentionPeriod": "1h",
        }
        self._databases[db_key] = meta
        self._ddl_statements[db_key] = []

        # Execute any extra DDL statements included in the create request
        if extra_statements:
            for stmt in extra_statements:
                stmt = stmt.strip()
                if stmt:
                    try:
                        duckdb_sql, pk_cols = _rewrite_ddl(stmt, schema)
                        self._exec(duckdb_sql)
                        if pk_cols:
                            m2 = re.search(
                                r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?",
                                stmt,
                                re.IGNORECASE,
                            )
                            if m2:
                                self._table_pks[f'"{schema}"."{m2.group(1)}"'] = pk_cols
                        self._ddl_statements[db_key].append(stmt)
                    except Exception:
                        pass

        op_name = (
            f"projects/{project}/instances/{instance_id}"
            f"/databases/{database_id}/operations/{uuid.uuid4()}"
        )
        op = {
            "name": op_name,
            "done": True,
            "response": {
                "@type": "type.googleapis.com/google.spanner.admin.database.v1.Database",
                **meta,
            },
        }
        self._operations[op_name] = op
        return op

    def get_database(self, project: str, instance_id: str, database_id: str) -> dict | None:
        return self._databases.get(f"{project}/{instance_id}/databases/{database_id}")

    def list_databases(self, project: str, instance_id: str) -> list[dict]:
        prefix = f"{project}/{instance_id}/databases/"
        return [v for k, v in self._databases.items() if k.startswith(prefix)]

    def delete_database(self, project: str, instance_id: str, database_id: str) -> bool:
        db_key = f"{project}/{instance_id}/databases/{database_id}"
        if db_key not in self._databases:
            return False
        self._drop_database_schema(db_key)
        del self._databases[db_key]
        self._ddl_statements.pop(db_key, None)
        return True

    def execute_ddl(
        self, project: str, instance_id: str, database_id: str, statements: list[str]
    ) -> dict:
        db_key = f"{project}/{instance_id}/databases/{database_id}"
        if db_key not in self._databases:
            raise ValueError(f"Database not found: {database_id}")

        schema = self._schema_name(instance_id, database_id)
        errors = []
        executed = []
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt:
                continue
            try:
                duckdb_sql, pk_cols = _rewrite_ddl(stmt, schema)
                self._exec(duckdb_sql)
                if pk_cols:
                    m2 = re.search(
                        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?", stmt, re.IGNORECASE
                    )
                    if m2:
                        self._table_pks[f'"{schema}"."{m2.group(1)}"'] = pk_cols
                executed.append(stmt)
            except Exception as exc:
                errors.append(str(exc))

        self._ddl_statements.setdefault(db_key, []).extend(executed)

        if errors:
            raise ValueError("; ".join(errors))

        op_name = (
            f"projects/{project}/instances/{instance_id}"
            f"/databases/{database_id}/operations/{uuid.uuid4()}"
        )
        op = {"name": op_name, "done": True, "metadata": {"statements": statements}}
        self._operations[op_name] = op
        return op

    def get_database_ddl(self, project: str, instance_id: str, database_id: str) -> list[str]:
        db_key = f"{project}/{instance_id}/databases/{database_id}"
        return self._ddl_statements.get(db_key, [])

    # -------------------------------------------------------------------------
    # Sessions
    # -------------------------------------------------------------------------

    def _session_resource(
        self, project: str, instance_id: str, database_id: str, session_id: str
    ) -> str:
        return (
            f"projects/{project}/instances/{instance_id}"
            f"/databases/{database_id}/sessions/{session_id}"
        )

    def create_session(
        self, project: str, instance_id: str, database_id: str, labels: dict | None = None
    ) -> dict:
        db_key = f"{project}/{instance_id}/databases/{database_id}"
        if db_key not in self._databases:
            raise ValueError(f"Database not found: {database_id}")
        session_id = str(uuid.uuid4())
        name = self._session_resource(project, instance_id, database_id, session_id)
        now = _now()
        meta = {
            "name": name,
            "labels": labels or {},
            "createTime": now,
            "approximateLastUseTime": now,
            "_project": project,
            "_instance": instance_id,
            "_database": database_id,
        }
        self._sessions[name] = meta
        return {k: v for k, v in meta.items() if not k.startswith("_")}

    def get_session(self, session_name: str) -> dict | None:
        meta = self._sessions.get(session_name)
        if meta is None:
            return None
        return {k: v for k, v in meta.items() if not k.startswith("_")}

    def list_sessions(self, project: str, instance_id: str, database_id: str) -> list[dict]:
        prefix = f"projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/"
        return [
            {k: v for k, v in meta.items() if not k.startswith("_")}
            for name, meta in self._sessions.items()
            if name.startswith(prefix)
        ]

    def delete_session(self, session_name: str) -> bool:
        if session_name not in self._sessions:
            return False
        del self._sessions[session_name]
        return True

    def batch_create_sessions(
        self,
        project: str,
        instance_id: str,
        database_id: str,
        count: int,
        labels: dict | None = None,
    ) -> list[dict]:
        return [
            self.create_session(project, instance_id, database_id, labels) for _ in range(count)
        ]

    def _session_schema(self, session_name: str) -> str:
        """Get the DuckDB schema for a session's database."""
        meta = self._sessions.get(session_name)
        if meta is None:
            raise ValueError(f"Session not found: {session_name}")
        return self._schema_name(meta["_instance"], meta["_database"])

    def _session_db_key(self, session_name: str) -> tuple[str, str, str]:
        meta = self._sessions.get(session_name)
        if meta is None:
            raise ValueError(f"Session not found: {session_name}")
        return meta["_project"], meta["_instance"], meta["_database"]

    # -------------------------------------------------------------------------
    # Transactions
    # -------------------------------------------------------------------------

    def begin_transaction(self, session_name: str, options: dict) -> dict:
        if session_name not in self._sessions:
            raise ValueError(f"Session not found: {session_name}")
        txn_id = base64.b64encode(str(uuid.uuid4()).encode()).decode()
        txn: dict = {"id": txn_id}
        if options.get("readOnly"):
            txn["readTimestamp"] = _now()
        self._transactions[txn_id] = {"session": session_name, "options": options}
        return txn

    def rollback(self, session_name: str, transaction_id: str) -> None:
        self._transactions.pop(transaction_id, None)

    # -------------------------------------------------------------------------
    # Mutations (commit)
    # -------------------------------------------------------------------------

    def _coerce_value(self, v: Any) -> Any:
        """Try to coerce a Spanner API value to a Python type DuckDB can accept."""
        if v is None:
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return v
        if isinstance(v, list):
            return json.dumps(v)
        if isinstance(v, dict):
            return json.dumps(v)
        # String — try numeric coercion
        if isinstance(v, str):
            # Leave as string; DuckDB will coerce when inserting into typed columns
            return v
        return v

    def _apply_insert(
        self, schema: str, table: str, columns: list[str], values: list[list]
    ) -> None:
        col_names = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        for row in values:
            params = [self._coerce_value(v) for v in row]
            self._exec(
                f'INSERT INTO "{schema}"."{table}" ({col_names}) VALUES ({placeholders})',
                params,
            )

    def _apply_update(
        self, schema: str, table: str, columns: list[str], values: list[list]
    ) -> None:
        pk_key = f'"{schema}"."{table}"'
        pk_cols = self._table_pks.get(pk_key, [])

        if not pk_cols:
            # Fallback: treat all columns as non-PK and use DELETE + INSERT
            for row in values:
                params = [self._coerce_value(v) for v in row]
                col_names = ", ".join(f'"{c}"' for c in columns)
                placeholders = ", ".join("?" for _ in columns)
                self._exec(
                    f'INSERT OR REPLACE INTO "{schema}"."{table}"'
                    f" ({col_names}) VALUES ({placeholders})",
                    params,
                )
            return

        pk_set = set(pk_cols)
        for row in values:
            row_dict = dict(zip(columns, row, strict=False))
            pk_vals = [self._coerce_value(row_dict[c]) for c in pk_cols if c in row_dict]
            non_pk_cols = [c for c in columns if c not in pk_set]

            if not non_pk_cols:
                continue  # nothing to update

            set_clause = ", ".join(f'"{c}" = ?' for c in non_pk_cols)
            where_clause = " AND ".join(f'"{c}" = ?' for c in pk_cols if c in row_dict)
            non_pk_vals = [self._coerce_value(row_dict[c]) for c in non_pk_cols]
            params = non_pk_vals + pk_vals
            self._exec(
                f'UPDATE "{schema}"."{table}" SET {set_clause} WHERE {where_clause}',
                params,
            )

    def _apply_insert_or_update(
        self, schema: str, table: str, columns: list[str], values: list[list]
    ) -> None:
        pk_key = f'"{schema}"."{table}"'
        pk_cols = self._table_pks.get(pk_key, columns[:1])  # fallback: first col as PK

        col_names = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join("?" for _ in columns)

        non_pk_cols = [c for c in columns if c not in set(pk_cols)]
        if non_pk_cols:
            update_set = ", ".join(f'"{c}" = excluded."{c}"' for c in non_pk_cols)
            conflict_clause = f"ON CONFLICT DO UPDATE SET {update_set}"
        else:
            conflict_clause = "ON CONFLICT DO NOTHING"

        for row in values:
            params = [self._coerce_value(v) for v in row]
            self._exec(
                f'INSERT INTO "{schema}"."{table}"'
                f" ({col_names}) VALUES ({placeholders}) {conflict_clause}",
                params,
            )

    def _apply_delete(self, schema: str, table: str, key_set: dict) -> None:
        pk_key = f'"{schema}"."{table}"'
        pk_cols = self._table_pks.get(pk_key, [])

        if key_set.get("all"):
            self._exec(f'DELETE FROM "{schema}"."{table}"')
            return

        keys = key_set.get("keys", [])
        if keys and pk_cols:
            if len(pk_cols) == 1:
                placeholders = ", ".join("?" for _ in keys)
                params = [self._coerce_value(k[0] if isinstance(k, list) else k) for k in keys]
                self._exec(
                    f'DELETE FROM "{schema}"."{table}" WHERE "{pk_cols[0]}" IN ({placeholders})',
                    params,
                )
            else:
                # Multi-column PK: delete each key separately
                where = " AND ".join(f'"{c}" = ?' for c in pk_cols)
                for key in keys:
                    key_list = key if isinstance(key, list) else [key]
                    params = [self._coerce_value(v) for v in key_list]
                    self._exec(f'DELETE FROM "{schema}"."{table}" WHERE {where}', params)
        elif keys:
            # No PK info — try first-column deletion
            for key in keys:
                # Can't safely delete without knowing PKs; skip
                pass

        # Handle ranges
        ranges = key_set.get("ranges", [])
        if ranges and pk_cols:
            pk_col = pk_cols[0]  # simplified: use first PK column for range
            for r in ranges:
                conditions = []
                params: list[Any] = []
                if "startClosed" in r:
                    conditions.append(f'"{pk_col}" >= ?')
                    v = r["startClosed"]
                    params.append(self._coerce_value(v[0] if isinstance(v, list) else v))
                elif "startOpen" in r:
                    conditions.append(f'"{pk_col}" > ?')
                    v = r["startOpen"]
                    params.append(self._coerce_value(v[0] if isinstance(v, list) else v))
                if "endClosed" in r:
                    conditions.append(f'"{pk_col}" <= ?')
                    v = r["endClosed"]
                    params.append(self._coerce_value(v[0] if isinstance(v, list) else v))
                elif "endOpen" in r:
                    conditions.append(f'"{pk_col}" < ?')
                    v = r["endOpen"]
                    params.append(self._coerce_value(v[0] if isinstance(v, list) else v))
                if conditions:
                    where = " AND ".join(conditions)
                    self._exec(f'DELETE FROM "{schema}"."{table}" WHERE {where}', params)

    def commit(
        self,
        session_name: str,
        mutations: list[dict],
        transaction_id: str | None = None,
    ) -> dict:
        if session_name not in self._sessions:
            raise ValueError(f"Session not found: {session_name}")

        schema = self._session_schema(session_name)
        for mutation in mutations:
            if "insert" in mutation:
                m = mutation["insert"]
                self._apply_insert(schema, m["table"], m["columns"], m["values"])
            elif "update" in mutation:
                m = mutation["update"]
                self._apply_update(schema, m["table"], m["columns"], m["values"])
            elif "insertOrUpdate" in mutation:
                m = mutation["insertOrUpdate"]
                self._apply_insert_or_update(schema, m["table"], m["columns"], m["values"])
            elif "replace" in mutation:
                m = mutation["replace"]
                self._apply_insert_or_update(schema, m["table"], m["columns"], m["values"])
            elif "delete" in mutation:
                m = mutation["delete"]
                self._apply_delete(schema, m["table"], m.get("keySet", {}))

        if transaction_id:
            self._transactions.pop(transaction_id, None)

        return {"commitTimestamp": _now()}

    # -------------------------------------------------------------------------
    # Read
    # -------------------------------------------------------------------------

    def read(
        self,
        session_name: str,
        table: str,
        columns: list[str],
        key_set: dict,
        limit: int = 0,
        index: str = "",
    ) -> dict:
        schema = self._session_schema(session_name)
        col_str = ", ".join(f'"{c}"' for c in columns)
        limit_clause = f" LIMIT {limit}" if limit > 0 else ""

        all_keys = key_set.get("all", False)
        keys = key_set.get("keys", [])
        ranges = key_set.get("ranges", [])

        pk_key = f'"{schema}"."{table}"'
        pk_cols = self._table_pks.get(pk_key, [])

        if all_keys:
            sql = f'SELECT {col_str} FROM "{schema}"."{table}"{limit_clause}'
            duck_cols, rows = self._select(sql)
        elif keys and pk_cols:
            pk_col = pk_cols[0]
            if len(pk_cols) == 1:
                placeholders = ", ".join("?" for _ in keys)
                params = [self._coerce_value(k[0] if isinstance(k, list) else k) for k in keys]
                sql = (
                    f'SELECT {col_str} FROM "{schema}"."{table}"'
                    f' WHERE "{pk_col}" IN ({placeholders}){limit_clause}'
                )
                duck_cols, rows = self._select(sql, params)
            else:
                # Multi-PK: fetch all and filter (simplified)
                sql = f'SELECT {col_str} FROM "{schema}"."{table}"{limit_clause}'
                duck_cols, rows = self._select(sql)
        elif ranges and pk_cols:
            pk_col = pk_cols[0]
            conditions = []
            params: list[Any] = []
            for r in ranges:
                if "startClosed" in r:
                    conditions.append(f'"{pk_col}" >= ?')
                    v = r["startClosed"]
                    params.append(self._coerce_value(v[0] if isinstance(v, list) else v))
                elif "startOpen" in r:
                    conditions.append(f'"{pk_col}" > ?')
                    v = r["startOpen"]
                    params.append(self._coerce_value(v[0] if isinstance(v, list) else v))
                if "endClosed" in r:
                    conditions.append(f'"{pk_col}" <= ?')
                    v = r["endClosed"]
                    params.append(self._coerce_value(v[0] if isinstance(v, list) else v))
                elif "endOpen" in r:
                    conditions.append(f'"{pk_col}" < ?')
                    v = r["endOpen"]
                    params.append(self._coerce_value(v[0] if isinstance(v, list) else v))
            where = " AND ".join(conditions) if conditions else "1=1"
            sql = f'SELECT {col_str} FROM "{schema}"."{table}" WHERE {where}{limit_clause}'
            duck_cols, rows = self._select(sql, params)
        else:
            sql = f'SELECT {col_str} FROM "{schema}"."{table}"{limit_clause}'
            duck_cols, rows = self._select(sql)

        return self._build_result_set(duck_cols, rows, columns)

    # -------------------------------------------------------------------------
    # SQL execution
    # -------------------------------------------------------------------------

    def _build_result_set(
        self,
        duck_cols: list[tuple[str, str]],
        rows: list[tuple],
        col_names: list[str] | None = None,
    ) -> dict:
        """Build a Spanner ResultSet from DuckDB result columns and rows."""
        fields = []
        spanner_types = []
        for i, (name, duck_type) in enumerate(duck_cols):
            display_name = col_names[i] if col_names and i < len(col_names) else name
            sp_type = _duck_type_to_spanner(duck_type)
            spanner_types.append(sp_type)
            fields.append({"name": display_name, "type": {"code": sp_type}})

        spanner_rows = []
        for row in rows:
            serialized = [
                _serialize_spanner(v, spanner_types[i] if i < len(spanner_types) else "STRING")
                for i, v in enumerate(row)
            ]
            spanner_rows.append(serialized)

        return {
            "metadata": {"rowType": {"fields": fields}},
            "rows": spanner_rows,
        }

    def _resolve_params(self, param_names: list[str], params: dict, param_types: dict) -> list[Any]:
        """Convert @param_name params to positional list for DuckDB."""
        result = []
        for name in param_names:
            v = params.get(name)
            pt = param_types.get(name, {})
            code = pt.get("code", "STRING").upper()
            if v is None:
                result.append(None)
            elif code in ("INT64",) and isinstance(v, str):
                try:
                    result.append(int(v))
                except ValueError:
                    result.append(v)
            elif code in ("FLOAT64", "FLOAT32") and isinstance(v, str):
                try:
                    result.append(float(v))
                except ValueError:
                    result.append(v)
            elif code == "BOOL":
                if isinstance(v, str):
                    result.append(v.lower() == "true")
                else:
                    result.append(bool(v))
            else:
                result.append(v)
        return result

    def execute_sql(
        self,
        session_name: str,
        sql: str,
        params: dict | None = None,
        param_types: dict | None = None,
        transaction: dict | None = None,
    ) -> dict:
        schema = self._session_schema(session_name)
        rewritten, param_names = _rewrite_spanner_sql(sql, schema)
        positional = self._resolve_params(param_names, params or {}, param_types or {})

        if _is_select(rewritten):
            duck_cols, rows = self._select(rewritten, positional)
            return self._build_result_set(duck_cols, rows)
        else:
            duck_cols, rows = self._select(rewritten, positional)
            count = rows[0][0] if rows and duck_cols and duck_cols[0][0] == "Count" else 0
            return {
                "metadata": {"rowType": {"fields": []}},
                "rows": [],
                "stats": {"rowCountExact": str(count)},
            }

    def execute_sql_streaming(
        self,
        session_name: str,
        sql: str,
        params: dict | None = None,
        param_types: dict | None = None,
    ) -> Iterator[str]:
        """Yield newline-delimited JSON PartialResultSet objects."""
        result = self.execute_sql(session_name, sql, params, param_types)
        fields = result.get("metadata", {}).get("rowType", {}).get("fields", [])
        rows = result.get("rows", [])

        # First chunk: metadata + all values flattened
        flat_values = [v for row in rows for v in row]
        chunk = {
            "metadata": {"rowType": {"fields": fields}},
            "values": flat_values,
            "chunkedValue": False,
            "resumeToken": "",
        }
        yield json.dumps(chunk) + "\n"

    def execute_batch_dml(
        self,
        session_name: str,
        statements: list[dict],
    ) -> dict:
        schema = self._session_schema(session_name)
        result_sets = []
        for stmt_body in statements:
            sql = stmt_body.get("sql", "")
            params = stmt_body.get("params", {})
            param_types = stmt_body.get("paramTypes", {})
            try:
                rewritten, param_names = _rewrite_spanner_sql(sql, schema)
                positional = self._resolve_params(param_names, params, param_types)
                duck_cols, rows = self._select(rewritten, positional)
                count = rows[0][0] if rows and duck_cols and duck_cols[0][0] == "Count" else 0
                result_sets.append(
                    {
                        "metadata": {"rowType": {"fields": []}},
                        "stats": {"rowCountExact": str(count)},
                    }
                )
            except Exception as exc:
                result_sets.append(
                    {
                        "metadata": {"rowType": {"fields": []}},
                        "stats": {"rowCountExact": "0"},
                        "error": {"code": 3, "message": str(exc)},
                    }
                )
        return {"resultSets": result_sets, "status": {}}

    # -------------------------------------------------------------------------
    # Operations
    # -------------------------------------------------------------------------

    def get_operation(self, op_name: str) -> dict | None:
        return self._operations.get(op_name)

    # -------------------------------------------------------------------------
    # Admin helpers
    # -------------------------------------------------------------------------

    def list_tables(self, project: str, instance_id: str, database_id: str) -> list[str]:
        db_key = f"{project}/{instance_id}/databases/{database_id}"
        if db_key not in self._databases:
            return []
        schema = self._schema_name(instance_id, database_id)
        try:
            _, rows = self._select(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                [schema],
            )
            return [row[0] for row in rows]
        except Exception:
            return []


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_engine = SpannerEngine()


def get_engine() -> SpannerEngine:
    return _engine
