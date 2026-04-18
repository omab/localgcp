"""DuckDB-backed BigQuery engine.

Each dataset maps to a DuckDB schema; each table is a DuckDB table inside
that schema. Rich metadata (labels, description, etc.) is kept in plain
Python dicts — the same pattern the rest of Cloudbox uses.

SQL rewriting: BigQuery uses backtick-quoted, project-qualified names
(`project.dataset.table`). _rewrite_sql() converts these to DuckDB-
compatible double-quoted identifiers ("dataset"."table").
"""
from __future__ import annotations

import re
import threading
import uuid
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any

import duckdb

from cloudbox.config import settings

# ------------------------------------------------------------------
# Type mapping
# ------------------------------------------------------------------

_BQ_TO_DUCK: dict[str, str] = {
    "STRING": "VARCHAR",
    "BYTES": "BLOB",
    "INTEGER": "BIGINT",
    "INT64": "BIGINT",
    "FLOAT": "DOUBLE",
    "FLOAT64": "DOUBLE",
    "BOOLEAN": "BOOLEAN",
    "BOOL": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMPTZ",
    "DATE": "DATE",
    "TIME": "TIME",
    "DATETIME": "TIMESTAMP",
    "NUMERIC": "DECIMAL(38, 9)",
    "BIGNUMERIC": "DECIMAL(76, 38)",
    "RECORD": "JSON",
    "STRUCT": "JSON",
    "JSON": "JSON",
}

_DUCK_TO_BQ: dict[str, str] = {
    "VARCHAR": "STRING",
    "TEXT": "STRING",
    "BLOB": "BYTES",
    "BIGINT": "INTEGER",
    "INTEGER": "INTEGER",
    "INT": "INTEGER",
    "INT32": "INTEGER",
    "INT64": "INTEGER",
    "HUGEINT": "INTEGER",
    "DOUBLE": "FLOAT",
    "FLOAT": "FLOAT",
    "REAL": "FLOAT",
    "BOOLEAN": "BOOLEAN",
    "BOOL": "BOOLEAN",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP",
    "TIMESTAMPTZ": "TIMESTAMP",
    "TIMESTAMP": "DATETIME",
    "DATE": "DATE",
    "TIME": "TIME",
    "DECIMAL": "NUMERIC",
    "JSON": "JSON",
}


def _duck_type_to_bq(duck_type: str) -> str:
    t = duck_type.upper().strip()
    if "(" in t:
        t = t[: t.index("(")].strip()
    return _DUCK_TO_BQ.get(t, "STRING")


def _fields_to_ddl(fields: list[dict]) -> str:
    """Convert a list of BigQuery field defs to a DuckDB column definition string."""
    parts = []
    for f in fields:
        name = f["name"]
        bq_type = f.get("type", "STRING").upper()
        duck_type = _BQ_TO_DUCK.get(bq_type, "VARCHAR")
        mode = f.get("mode", "NULLABLE").upper()
        null_clause = " NOT NULL" if mode == "REQUIRED" else ""
        parts.append(f'"{name}" {duck_type}{null_clause}')
    return ", ".join(parts)


def _serialize_value(v: Any) -> Any:
    """Convert a Python value to its BigQuery wire representation (string or None)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, (datetime, date, time)):
        return v.isoformat()
    return str(v)


def _now_ms() -> str:
    return str(int(datetime.now(timezone.utc).timestamp() * 1000))


# ------------------------------------------------------------------
# SQL rewriter
# ------------------------------------------------------------------

# ------------------------------------------------------------------
# Query parameter helpers
# ------------------------------------------------------------------

def _bq_scalar_value(type_name: str, value: str) -> Any:
    t = type_name.upper()
    if t in ("INT64", "INTEGER", "INT"):
        return int(value) if value else 0
    if t in ("FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC"):
        return float(value) if value else 0.0
    if t in ("BOOL", "BOOLEAN"):
        return value.lower() in ("true", "1")
    if t == "BYTES":
        import base64 as _b64
        return _b64.b64decode(value) if value else b""
    return value  # STRING, TIMESTAMP, DATE, TIME, DATETIME, JSON


def _bq_param_value(param: dict) -> Any:
    """Convert a single BigQuery queryParameter object to a Python value."""
    ptype = param.get("parameterType", {})
    pval = param.get("parameterValue", {})
    type_name = ptype.get("type", "STRING").upper()
    if type_name == "ARRAY":
        item_type = ptype.get("arrayType", {}).get("type", "STRING")
        return [_bq_scalar_value(item_type, v.get("value", "")) for v in pval.get("arrayValues", [])]
    return _bq_scalar_value(type_name, pval.get("value", ""))


def _apply_query_params(
    sql: str, query_parameters: list[dict], parameter_mode: str
) -> tuple[str, list]:
    """Rewrite BQ parameterized SQL to DuckDB positional form.

    Named mode: replaces @name with ? in order of appearance (handles
    repeated uses of the same parameter).
    Positional mode: ? is already DuckDB-compatible; values passed as-is.
    """
    if not query_parameters:
        return sql, []

    if parameter_mode.upper() == "NAMED":
        by_name = {p["name"]: _bq_param_value(p) for p in query_parameters if p.get("name")}
        duck_params: list = []

        def _replace(m: re.Match) -> str:
            duck_params.append(by_name.get(m.group(1)))
            return "?"

        sql = re.sub(r"@([A-Za-z_][A-Za-z0-9_]*)", _replace, sql)
    else:
        duck_params = [_bq_param_value(p) for p in query_parameters]

    return sql, duck_params


# ------------------------------------------------------------------

_SELECT_KEYWORDS = ("SELECT", "WITH", "VALUES", "SHOW", "DESCRIBE", "EXPLAIN", "PRAGMA")


def _is_select(sql: str) -> bool:
    """Return True if the SQL produces a result set (SELECT-like statements)."""
    stripped = sql.strip().lstrip("(").upper()
    return any(stripped.startswith(kw) for kw in _SELECT_KEYWORDS)


_INFORMATION_SCHEMA_VIEWS = {
    "TABLES", "COLUMNS", "SCHEMATA", "VIEWS",
    "TABLE_OPTIONS", "COLUMN_FIELD_PATHS", "PARTITIONS",
}

# Map BQ INFORMATION_SCHEMA view names to DuckDB information_schema equivalents.
# Views not in this map are passed through unchanged (DuckDB may support them natively).
_IS_VIEW_MAP: dict[str, str] = {
    "TABLES": "tables",
    "COLUMNS": "columns",
    "SCHEMATA": "schemata",
    "VIEWS": "views",
    "TABLE_OPTIONS": "tables",   # best-effort fallback
}


def _rewrite_information_schema(sql: str) -> str:
    """Rewrite BigQuery INFORMATION_SCHEMA references to DuckDB information_schema.

    Handles:
      `project.dataset.INFORMATION_SCHEMA.TABLES`
      `dataset.INFORMATION_SCHEMA.TABLES`
      project.dataset.INFORMATION_SCHEMA.TABLES  (unquoted)
      INFORMATION_SCHEMA.TABLES
    """
    def _replacement(m: re.Match) -> str:
        view = m.group("view").upper()
        duck_view = _IS_VIEW_MAP.get(view, view.lower())
        return f"information_schema.{duck_view}"

    # Backtick forms: `anything.INFORMATION_SCHEMA.VIEW`
    sql = re.sub(
        r'`[^`]*\bINFORMATION_SCHEMA\b\.(?P<view>\w+)`',
        _replacement,
        sql,
        flags=re.IGNORECASE,
    )
    # Unquoted dotted forms: word.INFORMATION_SCHEMA.VIEW or just INFORMATION_SCHEMA.VIEW
    sql = re.sub(
        r'\b(?:\w+\.)*INFORMATION_SCHEMA\.(?P<view>\w+)\b',
        _replacement,
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _rewrite_sql(sql: str, project: str) -> str:
    """Rewrite BigQuery SQL identifiers to DuckDB-compatible form.

    - `project.dataset.table`  →  "dataset"."table"
    - `dataset.table`          →  "dataset"."table"
    - `identifier`             →  "identifier"
    - INFORMATION_SCHEMA.*     →  information_schema.*
    """
    # Rewrite INFORMATION_SCHEMA references before identifier quoting
    sql = _rewrite_information_schema(sql)
    # 3-part backtick: `project.dataset.table`
    sql = re.sub(
        r'`[^`]*\.([^`.\s]+)\.([^`.\s]+)`',
        lambda m: f'"{m.group(1)}"."{m.group(2)}"',
        sql,
    )
    # 2-part backtick: `dataset.table`
    sql = re.sub(
        r'`([^`.\s]+)\.([^`.\s]+)`',
        lambda m: f'"{m.group(1)}"."{m.group(2)}"',
        sql,
    )
    # Single backtick identifier: `name`
    sql = re.sub(r'`([^`]+)`', lambda m: f'"{m.group(1)}"', sql)
    return sql


# ------------------------------------------------------------------
# Engine
# ------------------------------------------------------------------

class BigQueryEngine:
    """Single DuckDB connection backing the BigQuery emulator."""

    def __init__(self) -> None:
        if settings.data_dir:
            self._db_path = str(Path(settings.data_dir) / "bigquery.duckdb")
        else:
            self._db_path = ":memory:"
        self._conn = duckdb.connect(self._db_path)
        self._lock = threading.Lock()
        # Metadata dicts — keyed by "project.dataset" or "project.dataset.table"
        self._datasets: dict[str, dict] = {}
        self._tables: dict[str, dict] = {}
        self._jobs: dict[str, dict] = {}  # keyed by "project.jobId"

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _exec(self, sql: str, params: list | None = None) -> None:
        """Execute a DDL or DML statement (no result needed)."""
        with self._lock:
            self._conn.execute(sql, params or [])

    def _select(
        self, sql: str, params: list | None = None
    ) -> tuple[list[tuple[str, str]], list[tuple]]:
        """Execute a SELECT and return ([(col_name, bq_type), ...], rows)."""
        with self._lock:
            self._conn.execute(sql, params or [])
            desc = self._conn.description or []
            rows = self._conn.fetchall()
        columns = [
            (d[0], _duck_type_to_bq(str(d[1])) if d[1] is not None else "STRING")
            for d in desc
        ]
        return columns, rows

    def reset(self) -> None:
        """Wipe all state — used by tests and the admin UI."""
        with self._lock:
            self._conn.close()
            self._conn = duckdb.connect(self._db_path)
        self._datasets.clear()
        self._tables.clear()
        self._jobs.clear()

    # ------------------------------------------------------------------
    # Datasets
    # ------------------------------------------------------------------

    def create_dataset(self, project: str, dataset_id: str, body: dict) -> dict:
        key = f"{project}.{dataset_id}"
        if key in self._datasets:
            raise ValueError(f"Already exists: dataset {dataset_id}")
        self._exec(f'CREATE SCHEMA IF NOT EXISTS "{dataset_id}"')
        now = _now_ms()
        meta = {
            "kind": "bigquery#dataset",
            "id": f"{project}:{dataset_id}",
            "datasetReference": {"projectId": project, "datasetId": dataset_id},
            "location": body.get("location", "US"),
            "labels": body.get("labels", {}),
            "description": body.get("description", ""),
            "creationTime": now,
            "lastModifiedTime": now,
        }
        self._datasets[key] = meta
        return meta

    def get_dataset(self, project: str, dataset_id: str) -> dict | None:
        return self._datasets.get(f"{project}.{dataset_id}")

    def list_datasets(self, project: str) -> list[dict]:
        prefix = f"{project}."
        return [v for k, v in self._datasets.items() if k.startswith(prefix)]

    def delete_dataset(
        self, project: str, dataset_id: str, delete_contents: bool = False
    ) -> bool:
        key = f"{project}.{dataset_id}"
        if key not in self._datasets:
            return False
        table_prefix = f"{project}.{dataset_id}."
        has_tables = any(k.startswith(table_prefix) for k in self._tables)
        if has_tables and not delete_contents:
            raise ValueError(
                f"Dataset {dataset_id} is not empty. "
                "Set deleteContents=true to delete it and its tables."
            )
        self._exec(f'DROP SCHEMA IF EXISTS "{dataset_id}" CASCADE')
        for k in list(self._tables.keys()):
            if k.startswith(table_prefix):
                del self._tables[k]
        del self._datasets[key]
        return True

    # ------------------------------------------------------------------
    # Tables
    # ------------------------------------------------------------------

    def create_table(
        self, project: str, dataset_id: str, table_id: str, body: dict
    ) -> dict:
        ds_key = f"{project}.{dataset_id}"
        if ds_key not in self._datasets:
            raise ValueError(f"Dataset not found: {dataset_id}")
        tbl_key = f"{project}.{dataset_id}.{table_id}"
        if tbl_key in self._tables:
            raise ValueError(f"Already exists: table {table_id}")

        fields = (body.get("schema") or {}).get("fields", [])
        if not fields:
            raise ValueError("Table schema must contain at least one field")

        ddl = f'CREATE TABLE "{dataset_id}"."{table_id}" ({_fields_to_ddl(fields)})'
        self._exec(ddl)

        now = _now_ms()
        location = self._datasets[ds_key].get("location", "US")
        meta = {
            "kind": "bigquery#table",
            "id": f"{project}:{dataset_id}.{table_id}",
            "tableReference": {
                "projectId": project,
                "datasetId": dataset_id,
                "tableId": table_id,
            },
            "schema": {"fields": fields},
            "location": location,
            "labels": body.get("labels", {}),
            "description": body.get("description", ""),
            "numRows": "0",
            "numBytes": "0",
            "creationTime": now,
            "lastModifiedTime": now,
            "type": "TABLE",
        }
        self._tables[tbl_key] = meta
        return meta

    def update_table(
        self, project: str, dataset_id: str, table_id: str, body: dict
    ) -> dict:
        tbl_key = f"{project}.{dataset_id}.{table_id}"
        meta = self._tables.get(tbl_key)
        if meta is None:
            raise ValueError(f"Not found: {table_id}")
        if meta.get("type") == "VIEW":
            raise ValueError(f"{table_id} is a view; use update_view instead")

        new_fields = (body.get("schema") or {}).get("fields", [])
        if new_fields:
            existing_names = {f["name"] for f in meta["schema"]["fields"]}
            for field in new_fields:
                if field["name"] not in existing_names:
                    duck_type = _BQ_TO_DUCK.get(field["type"].upper(), "VARCHAR")
                    self._exec(
                        f'ALTER TABLE "{dataset_id}"."{table_id}" '
                        f'ADD COLUMN "{field["name"]}" {duck_type}'
                    )
                    meta["schema"]["fields"].append(field)

        if "description" in body:
            meta["description"] = body["description"]
        if "labels" in body:
            meta["labels"] = {**meta.get("labels", {}), **body["labels"]}
        meta["lastModifiedTime"] = _now_ms()
        return meta

    def get_table(self, project: str, dataset_id: str, table_id: str) -> dict | None:
        return self._tables.get(f"{project}.{dataset_id}.{table_id}")

    def list_tables(self, project: str, dataset_id: str) -> list[dict]:
        prefix = f"{project}.{dataset_id}."
        return [v for k, v in self._tables.items() if k.startswith(prefix)]

    def delete_table(self, project: str, dataset_id: str, table_id: str) -> bool:
        key = f"{project}.{dataset_id}.{table_id}"
        if key not in self._tables:
            return False
        meta = self._tables[key]
        if meta.get("type") == "VIEW":
            self._exec(f'DROP VIEW IF EXISTS "{dataset_id}"."{table_id}"')
        else:
            self._exec(f'DROP TABLE IF EXISTS "{dataset_id}"."{table_id}"')
        del self._tables[key]
        return True

    def create_view(
        self, project: str, dataset_id: str, table_id: str, body: dict
    ) -> dict:
        ds_key = f"{project}.{dataset_id}"
        if ds_key not in self._datasets:
            raise ValueError(f"Dataset not found: {dataset_id}")
        tbl_key = f"{project}.{dataset_id}.{table_id}"
        if tbl_key in self._tables:
            raise ValueError(f"Already exists: table {table_id}")

        view_def = body.get("view", {})
        query = view_def.get("query", "")
        if not query:
            raise ValueError("view.query is required")

        rewritten = _rewrite_sql(query, project)
        self._exec(f'CREATE VIEW "{dataset_id}"."{table_id}" AS {rewritten}')

        now = _now_ms()
        location = self._datasets[ds_key].get("location", "US")
        meta = {
            "kind": "bigquery#table",
            "id": f"{project}:{dataset_id}.{table_id}",
            "tableReference": {
                "projectId": project,
                "datasetId": dataset_id,
                "tableId": table_id,
            },
            "view": {"query": query, "useLegacySql": view_def.get("useLegacySql", False)},
            "location": location,
            "labels": body.get("labels", {}),
            "description": body.get("description", ""),
            "creationTime": now,
            "lastModifiedTime": now,
            "type": "VIEW",
        }
        self._tables[tbl_key] = meta
        return meta

    def update_view(
        self, project: str, dataset_id: str, table_id: str, body: dict
    ) -> dict:
        tbl_key = f"{project}.{dataset_id}.{table_id}"
        meta = self._tables.get(tbl_key)
        if meta is None:
            raise ValueError(f"Not found: {table_id}")
        if meta.get("type") != "VIEW":
            raise ValueError(f"{table_id} is not a view")

        view_def = body.get("view", {})
        query = view_def.get("query", "")
        if not query:
            raise ValueError("view.query is required")

        rewritten = _rewrite_sql(query, project)
        self._exec(f'CREATE OR REPLACE VIEW "{dataset_id}"."{table_id}" AS {rewritten}')

        meta["view"] = {"query": query, "useLegacySql": view_def.get("useLegacySql", False)}
        meta["lastModifiedTime"] = _now_ms()
        if "description" in body:
            meta["description"] = body["description"]
        if "labels" in body:
            meta["labels"] = body["labels"]
        return meta

    # ------------------------------------------------------------------
    # Jobs / queries
    # ------------------------------------------------------------------

    def run_query(
        self,
        project: str,
        job_id: str,
        query: str,
        use_legacy_sql: bool = False,
        query_parameters: list | None = None,
        parameter_mode: str = "NONE",
    ) -> dict:
        rewritten = _rewrite_sql(query, project)
        if query_parameters:
            rewritten, duck_params = _apply_query_params(
                rewritten, query_parameters, parameter_mode or "POSITIONAL"
            )
        else:
            duck_params = None
        now = _now_ms()
        job_ref = {"projectId": project, "jobId": job_id, "location": "US"}

        try:
            if _is_select(rewritten):
                columns, rows = self._select(rewritten, duck_params)
                schema_fields = [
                    {"name": name, "type": bq_type, "mode": "NULLABLE"}
                    for name, bq_type in columns
                ]
                bq_rows = [
                    {"f": [{"v": _serialize_value(v)} for v in row]}
                    for row in rows
                ]
                num_dml_rows = None
            else:
                # DML (INSERT/UPDATE/DELETE) or DDL (CREATE/DROP/ALTER)
                # DuckDB returns a Count row after DML; capture it.
                columns, rows = self._select(rewritten, duck_params)
                num_dml_rows = rows[0][0] if rows and columns and columns[0][0] == "Count" else 0
                schema_fields = []
                bq_rows = []

            stats: dict = {
                "creationTime": now,
                "startTime": now,
                "endTime": now,
                "totalBytesProcessed": "0",
                "totalSlotMs": "0",
            }
            if num_dml_rows is not None:
                stats["query"] = {"numDmlAffectedRows": str(num_dml_rows)}

            job = {
                "kind": "bigquery#job",
                "id": f"{project}:{job_id}",
                "jobReference": job_ref,
                "status": {"state": "DONE", "errorResult": None},
                "configuration": {
                    "query": {"query": query, "useLegacySql": use_legacy_sql},
                    "jobType": "QUERY",
                },
                "statistics": stats,
                "_result": {
                    "schema": {"fields": schema_fields},
                    "rows": bq_rows,
                    "totalRows": str(len(bq_rows)),
                },
            }
        except Exception as exc:
            job = {
                "kind": "bigquery#job",
                "id": f"{project}:{job_id}",
                "jobReference": job_ref,
                "status": {
                    "state": "DONE",
                    "errorResult": {
                        "reason": "invalidQuery",
                        "message": str(exc),
                        "location": "query",
                    },
                    "errors": [
                        {
                            "reason": "invalidQuery",
                            "message": str(exc),
                            "location": "query",
                        }
                    ],
                },
                "configuration": {
                    "query": {"query": query, "useLegacySql": use_legacy_sql},
                    "jobType": "QUERY",
                },
                "statistics": {
                    "creationTime": now,
                    "startTime": now,
                    "endTime": now,
                },
                "_result": None,
            }

        self._jobs[f"{project}.{job_id}"] = job
        return job

    def get_job(self, project: str, job_id: str) -> dict | None:
        return self._jobs.get(f"{project}.{job_id}")

    def get_query_results(self, project: str, job_id: str) -> dict | None:
        job = self._jobs.get(f"{project}.{job_id}")
        if job is None:
            return None
        result = job.get("_result")
        base = {
            "kind": "bigquery#queryResponse",
            "jobComplete": True,
            "jobReference": job["jobReference"],
            "totalBytesProcessed": "0",
        }
        if result is None:
            return {**base, "totalRows": "0", "schema": {"fields": []}, "rows": []}
        return {
            **base,
            "totalRows": result["totalRows"],
            "schema": result["schema"],
            "rows": result["rows"],
        }

    # ------------------------------------------------------------------
    # Streaming inserts
    # ------------------------------------------------------------------

    def insert_rows(
        self,
        project: str,
        dataset_id: str,
        table_id: str,
        rows: list[dict],
    ) -> list[dict]:
        """Insert rows via the tabledata.insertAll path.

        Returns a list of per-row error dicts (empty on full success).
        """
        tbl_key = f"{project}.{dataset_id}.{table_id}"
        if tbl_key not in self._tables:
            raise ValueError(f"Table not found: {table_id}")

        fields = (self._tables[tbl_key].get("schema") or {}).get("fields", [])
        errors: list[dict] = []

        for i, envelope in enumerate(rows):
            json_row: dict = envelope.get("json") or {}
            if not json_row:
                continue
            try:
                if fields:
                    col_names = ", ".join(f'"{f["name"]}"' for f in fields)
                    vals = [json_row.get(f["name"]) for f in fields]
                else:
                    col_names = ", ".join(f'"{k}"' for k in json_row)
                    vals = list(json_row.values())
                placeholders = ", ".join("?" for _ in vals)
                self._exec(
                    f'INSERT INTO "{dataset_id}"."{table_id}" ({col_names}) VALUES ({placeholders})',
                    vals,
                )
                self._tables[tbl_key]["numRows"] = str(
                    int(self._tables[tbl_key]["numRows"]) + 1
                )
            except Exception as exc:
                errors.append(
                    {"index": i, "errors": [{"reason": "invalid", "message": str(exc)}]}
                )

        return errors

    def list_rows(
        self,
        project: str,
        dataset_id: str,
        table_id: str,
        max_results: int = 1000,
        page_token: str = "",
    ) -> dict:
        tbl_key = f"{project}.{dataset_id}.{table_id}"
        if tbl_key not in self._tables:
            raise ValueError(f"Table not found: {table_id}")

        offset = int(page_token) if page_token else 0
        columns, rows = self._select(
            f'SELECT * FROM "{dataset_id}"."{table_id}" LIMIT ? OFFSET ?',
            [max_results, offset],
        )
        schema_fields = [
            {"name": name, "type": bq_type, "mode": "NULLABLE"}
            for name, bq_type in columns
        ]
        bq_rows = [
            {"f": [{"v": _serialize_value(v)} for v in row]}
            for row in rows
        ]
        total = int(self._tables[tbl_key].get("numRows", "0"))
        next_token = str(offset + max_results) if offset + max_results < total else None
        return {
            "kind": "bigquery#tableDataList",
            "rows": bq_rows,
            "totalRows": str(total),
            "schema": {"fields": schema_fields},
            "pageToken": next_token,
        }


# Module-level singleton, matching the pattern of other Cloudbox services.
_engine = BigQueryEngine()


def get_engine() -> BigQueryEngine:
    return _engine
