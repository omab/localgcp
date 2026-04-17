"""Tests for BigQuery emulator."""

PROJECT = "local-project"
BASE = f"/bigquery/v2/projects/{PROJECT}"

SCHEMA = {
    "fields": [
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "score", "type": "FLOAT", "mode": "NULLABLE"},
    ]
}


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------


def test_create_and_get_dataset(bq_client):
    r = bq_client.post(
        f"{BASE}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": "ds1"}},
    )
    assert r.status_code == 200
    assert r.json()["datasetReference"]["datasetId"] == "ds1"

    r = bq_client.get(f"{BASE}/datasets/ds1")
    assert r.status_code == 200
    assert r.json()["id"] == f"{PROJECT}:ds1"


def test_list_datasets(bq_client):
    for ds in ("alpha", "beta"):
        bq_client.post(
            f"{BASE}/datasets",
            json={"datasetReference": {"projectId": PROJECT, "datasetId": ds}},
        )
    r = bq_client.get(f"{BASE}/datasets")
    assert r.status_code == 200
    ids = [d["datasetReference"]["datasetId"] for d in r.json()["datasets"]]
    assert "alpha" in ids and "beta" in ids


def test_duplicate_dataset_returns_409(bq_client):
    body = {"datasetReference": {"projectId": PROJECT, "datasetId": "dup"}}
    bq_client.post(f"{BASE}/datasets", json=body)
    r = bq_client.post(f"{BASE}/datasets", json=body)
    assert r.status_code == 409


def test_delete_dataset(bq_client):
    bq_client.post(
        f"{BASE}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": "to-delete"}},
    )
    r = bq_client.delete(f"{BASE}/datasets/to-delete")
    assert r.status_code == 204
    r = bq_client.get(f"{BASE}/datasets/to-delete")
    assert r.status_code == 404


def test_delete_nonempty_dataset_requires_flag(bq_client):
    bq_client.post(
        f"{BASE}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": "nonempty"}},
    )
    bq_client.post(
        f"{BASE}/datasets/nonempty/tables",
        json={"tableReference": {"projectId": PROJECT, "datasetId": "nonempty", "tableId": "t"}, "schema": SCHEMA},
    )
    r = bq_client.delete(f"{BASE}/datasets/nonempty")
    assert r.status_code == 400

    r = bq_client.delete(f"{BASE}/datasets/nonempty?deleteContents=true")
    assert r.status_code == 204


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------


def _setup_dataset(client, ds="myds"):
    client.post(
        f"{BASE}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": ds}},
    )


def test_create_and_get_table(bq_client):
    _setup_dataset(bq_client)
    r = bq_client.post(
        f"{BASE}/datasets/myds/tables",
        json={
            "tableReference": {"projectId": PROJECT, "datasetId": "myds", "tableId": "users"},
            "schema": SCHEMA,
        },
    )
    assert r.status_code == 200
    assert r.json()["tableReference"]["tableId"] == "users"

    r = bq_client.get(f"{BASE}/datasets/myds/tables/users")
    assert r.status_code == 200
    assert r.json()["schema"]["fields"][0]["name"] == "id"


def test_list_tables(bq_client):
    _setup_dataset(bq_client)
    for tbl in ("t1", "t2"):
        bq_client.post(
            f"{BASE}/datasets/myds/tables",
            json={
                "tableReference": {"projectId": PROJECT, "datasetId": "myds", "tableId": tbl},
                "schema": SCHEMA,
            },
        )
    r = bq_client.get(f"{BASE}/datasets/myds/tables")
    assert r.status_code == 200
    tids = [t["tableReference"]["tableId"] for t in r.json()["tables"]]
    assert "t1" in tids and "t2" in tids


def test_delete_table(bq_client):
    _setup_dataset(bq_client)
    bq_client.post(
        f"{BASE}/datasets/myds/tables",
        json={
            "tableReference": {"projectId": PROJECT, "datasetId": "myds", "tableId": "gone"},
            "schema": SCHEMA,
        },
    )
    r = bq_client.delete(f"{BASE}/datasets/myds/tables/gone")
    assert r.status_code == 204
    r = bq_client.get(f"{BASE}/datasets/myds/tables/gone")
    assert r.status_code == 404


def test_create_table_without_schema_returns_400(bq_client):
    _setup_dataset(bq_client)
    r = bq_client.post(
        f"{BASE}/datasets/myds/tables",
        json={
            "tableReference": {"projectId": PROJECT, "datasetId": "myds", "tableId": "noschema"},
            "schema": {"fields": []},
        },
    )
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# Jobs — query execution
# ---------------------------------------------------------------------------


def _setup_table(client, ds="myds", table="users"):
    _setup_dataset(client, ds)
    client.post(
        f"{BASE}/datasets/{ds}/tables",
        json={
            "tableReference": {"projectId": PROJECT, "datasetId": ds, "tableId": table},
            "schema": SCHEMA,
        },
    )


def test_run_query_empty_table(bq_client):
    _setup_table(bq_client)
    r = bq_client.post(
        f"{BASE}/jobs",
        json={
            "configuration": {
                "query": {
                    "query": 'SELECT * FROM "myds"."users"',
                    "useLegacySql": False,
                }
            }
        },
    )
    assert r.status_code == 200
    job = r.json()
    assert job["status"]["state"] == "DONE"
    assert job["status"]["errorResult"] is None

    r2 = bq_client.get(f"{BASE}/queries/{job['jobReference']['jobId']}")
    assert r2.status_code == 200
    assert r2.json()["jobComplete"] is True
    assert r2.json()["totalRows"] == "0"


def test_run_query_with_backtick_identifiers(bq_client):
    _setup_table(bq_client)
    r = bq_client.post(
        f"{BASE}/jobs",
        json={
            "configuration": {
                "query": {
                    "query": f"SELECT * FROM `{PROJECT}.myds.users`",
                    "useLegacySql": False,
                }
            }
        },
    )
    assert r.status_code == 200
    assert r.json()["status"]["state"] == "DONE"


def test_run_query_syntax_error_returns_done_with_error(bq_client):
    r = bq_client.post(
        f"{BASE}/jobs",
        json={
            "configuration": {
                "query": {"query": "SELECT * FROM nonexistent_table_xyz", "useLegacySql": False}
            }
        },
    )
    assert r.status_code == 200
    job = r.json()
    assert job["status"]["state"] == "DONE"
    assert job["status"]["errorResult"] is not None


def test_get_job(bq_client):
    _setup_table(bq_client)
    r = bq_client.post(
        f"{BASE}/jobs",
        json={
            "jobReference": {"projectId": PROJECT, "jobId": "my-job-123"},
            "configuration": {
                "query": {"query": 'SELECT 1 AS n', "useLegacySql": False}
            },
        },
    )
    assert r.status_code == 200
    r2 = bq_client.get(f"{BASE}/jobs/my-job-123")
    assert r2.status_code == 200
    assert r2.json()["jobReference"]["jobId"] == "my-job-123"


def test_sync_query(bq_client):
    r = bq_client.post(
        f"{BASE}/queries",
        json={"query": "SELECT 42 AS answer", "useLegacySql": False},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["jobComplete"] is True
    assert body["totalRows"] == "1"
    assert body["rows"][0]["f"][0]["v"] == "42"


# ---------------------------------------------------------------------------
# Streaming inserts + row reads
# ---------------------------------------------------------------------------


def test_insert_and_query_rows(bq_client):
    _setup_table(bq_client)

    r = bq_client.post(
        f"{BASE}/datasets/myds/tables/users/insertAll",
        json={
            "rows": [
                {"insertId": "1", "json": {"id": 1, "name": "Alice", "score": 9.5}},
                {"insertId": "2", "json": {"id": 2, "name": "Bob", "score": 7.0}},
            ]
        },
    )
    assert r.status_code == 200
    assert r.json()["insertErrors"] == []

    r2 = bq_client.post(
        f"{BASE}/jobs",
        json={
            "configuration": {
                "query": {
                    "query": 'SELECT * FROM "myds"."users" ORDER BY id',
                    "useLegacySql": False,
                }
            }
        },
    )
    result = bq_client.get(f"{BASE}/queries/{r2.json()['jobReference']['jobId']}")
    rows = result.json()["rows"]
    assert len(rows) == 2
    # first row, first field = id = "1"
    assert rows[0]["f"][0]["v"] == "1"
    assert rows[1]["f"][1]["v"] == "Bob"


def test_list_tabledata(bq_client):
    _setup_table(bq_client)
    bq_client.post(
        f"{BASE}/datasets/myds/tables/users/insertAll",
        json={"rows": [{"insertId": "r1", "json": {"id": 10, "name": "Carol", "score": 8.0}}]},
    )
    r = bq_client.get(f"{BASE}/datasets/myds/tables/users/data")
    assert r.status_code == 200
    assert r.json()["totalRows"] == "1"
    assert len(r.json()["rows"]) == 1


def test_dml_insert_via_jobs(bq_client):
    """INSERT via the jobs API returns DONE with numDmlAffectedRows."""
    _setup_table(bq_client)
    r = bq_client.post(
        f"{BASE}/jobs",
        json={
            "configuration": {
                "query": {
                    "query": 'INSERT INTO "myds"."users" VALUES (99, \'Eve\', 5.5)',
                    "useLegacySql": False,
                }
            }
        },
    )
    assert r.status_code == 200
    job = r.json()
    assert job["status"]["state"] == "DONE"
    assert job["status"]["errorResult"] is None
    assert job["statistics"]["query"]["numDmlAffectedRows"] == "1"

    # Row should be queryable
    r2 = bq_client.post(
        f"{BASE}/jobs",
        json={"configuration": {"query": {"query": 'SELECT name FROM "myds"."users"', "useLegacySql": False}}},
    )
    result = bq_client.get(f"{BASE}/queries/{r2.json()['jobReference']['jobId']}")
    assert result.json()["rows"][0]["f"][0]["v"] == "Eve"


def test_dml_update_via_jobs(bq_client):
    """UPDATE via the jobs API returns the affected row count."""
    _setup_table(bq_client)
    # Seed a row via streaming insert
    bq_client.post(
        f"{BASE}/datasets/myds/tables/users/insertAll",
        json={"rows": [{"insertId": "u1", "json": {"id": 1, "name": "Alice", "score": 9.0}}]},
    )
    r = bq_client.post(
        f"{BASE}/jobs",
        json={
            "configuration": {
                "query": {
                    "query": 'UPDATE "myds"."users" SET score = 10.0 WHERE id = 1',
                    "useLegacySql": False,
                }
            }
        },
    )
    assert r.status_code == 200
    assert r.json()["statistics"]["query"]["numDmlAffectedRows"] == "1"


def test_dml_delete_via_jobs(bq_client):
    """DELETE via the jobs API returns the affected row count."""
    _setup_table(bq_client)
    bq_client.post(
        f"{BASE}/datasets/myds/tables/users/insertAll",
        json={"rows": [{"insertId": "d1", "json": {"id": 2, "name": "Bob", "score": 7.0}}]},
    )
    r = bq_client.post(
        f"{BASE}/jobs",
        json={
            "configuration": {
                "query": {
                    "query": 'DELETE FROM "myds"."users" WHERE id = 2',
                    "useLegacySql": False,
                }
            }
        },
    )
    assert r.status_code == 200
    assert r.json()["statistics"]["query"]["numDmlAffectedRows"] == "1"

    # Row should be gone
    r2 = bq_client.post(
        f"{BASE}/jobs",
        json={"configuration": {"query": {"query": 'SELECT * FROM "myds"."users"', "useLegacySql": False}}},
    )
    result = bq_client.get(f"{BASE}/queries/{r2.json()['jobReference']['jobId']}")
    assert result.json()["totalRows"] == "0"


def test_ddl_create_table_as_select(bq_client):
    """CREATE TABLE AS SELECT creates a new table from query results."""
    _setup_table(bq_client)
    bq_client.post(
        f"{BASE}/datasets/myds/tables/users/insertAll",
        json={"rows": [{"insertId": "c1", "json": {"id": 3, "name": "Carol", "score": 8.0}}]},
    )
    # Create a derived table via CTAS
    r = bq_client.post(
        f"{BASE}/jobs",
        json={
            "configuration": {
                "query": {
                    "query": 'CREATE TABLE "myds"."top_users" AS SELECT id, name FROM "myds"."users" WHERE score > 7',
                    "useLegacySql": False,
                }
            }
        },
    )
    assert r.status_code == 200
    assert r.json()["status"]["state"] == "DONE"

    # New table should be queryable
    r2 = bq_client.post(
        f"{BASE}/jobs",
        json={"configuration": {"query": {"query": 'SELECT * FROM "myds"."top_users"', "useLegacySql": False}}},
    )
    result = bq_client.get(f"{BASE}/queries/{r2.json()['jobReference']['jobId']}")
    assert result.json()["totalRows"] == "1"


def test_insert_rows_missing_table_returns_404(bq_client):
    _setup_dataset(bq_client)
    r = bq_client.post(
        f"{BASE}/datasets/myds/tables/ghost/insertAll",
        json={"rows": [{"insertId": "x", "json": {"id": 1}}]},
    )
    assert r.status_code == 404


def test_get_missing_dataset_returns_404(bq_client):
    r = bq_client.get(f"{BASE}/datasets/no-such-dataset")
    assert r.status_code == 404


def test_get_missing_table_returns_404(bq_client):
    _setup_dataset(bq_client)
    r = bq_client.get(f"{BASE}/datasets/myds/tables/phantom")
    assert r.status_code == 404


def test_get_missing_job_returns_404(bq_client):
    r = bq_client.get(f"{BASE}/jobs/nonexistent-job")
    assert r.status_code == 404


def test_cancel_job(bq_client):
    """cancel is a no-op but returns the job."""
    r = bq_client.post(
        f"{BASE}/jobs",
        json={
            "jobReference": {"projectId": PROJECT, "jobId": "cancel-job"},
            "configuration": {"query": {"query": "SELECT 1", "useLegacySql": False}},
        },
    )
    assert r.status_code == 200
    r2 = bq_client.post(f"{BASE}/jobs/cancel-job/cancel")
    assert r2.status_code == 200
    assert r2.json()["job"]["jobReference"]["jobId"] == "cancel-job"


def test_query_with_where_clause(bq_client):
    _setup_table(bq_client)
    bq_client.post(
        f"{BASE}/datasets/myds/tables/users/insertAll",
        json={"rows": [
            {"insertId": "1", "json": {"id": 1, "name": "Alice", "score": 9.0}},
            {"insertId": "2", "json": {"id": 2, "name": "Bob", "score": 4.0}},
            {"insertId": "3", "json": {"id": 3, "name": "Carol", "score": 7.0}},
        ]},
    )
    r = bq_client.post(
        f"{BASE}/queries",
        json={"query": 'SELECT name FROM "myds"."users" WHERE score >= 7', "useLegacySql": False},
    )
    assert r.status_code == 200
    assert r.json()["totalRows"] == "2"
    names = [row["f"][0]["v"] for row in r.json()["rows"]]
    assert set(names) == {"Alice", "Carol"}


def test_duplicate_table_returns_409(bq_client):
    _setup_dataset(bq_client)
    body = {
        "tableReference": {"projectId": PROJECT, "datasetId": "myds", "tableId": "dup-tbl"},
        "schema": SCHEMA,
    }
    bq_client.post(f"{BASE}/datasets/myds/tables", json=body)
    r = bq_client.post(f"{BASE}/datasets/myds/tables", json=body)
    assert r.status_code == 409


def test_list_tabledata_pagination(bq_client):
    _setup_table(bq_client)
    bq_client.post(
        f"{BASE}/datasets/myds/tables/users/insertAll",
        json={"rows": [{"insertId": str(i), "json": {"id": i, "name": f"u{i}", "score": float(i)}} for i in range(5)]},
    )
    r = bq_client.get(f"{BASE}/datasets/myds/tables/users/data?maxResults=3")
    assert r.status_code == 200
    body = r.json()
    assert len(body["rows"]) == 3
    assert body.get("pageToken")  # more pages available


def test_create_dataset_no_id_returns_400(bq_client):
    r = bq_client.post(f"{BASE}/datasets", json={})
    assert r.status_code == 400


def test_delete_missing_dataset_returns_404(bq_client):
    r = bq_client.delete(f"{BASE}/datasets/no-such-ds")
    assert r.status_code == 404


def test_create_table_no_id_returns_400(bq_client):
    bq_client.post(f"{BASE}/datasets", json={"datasetReference": {"datasetId": "myds2"}})
    r = bq_client.post(f"{BASE}/datasets/myds2/tables", json={})
    assert r.status_code == 400


def test_delete_missing_table_returns_404(bq_client):
    bq_client.post(f"{BASE}/datasets", json={"datasetReference": {"datasetId": "myds3"}})
    r = bq_client.delete(f"{BASE}/datasets/myds3/tables/no-table")
    assert r.status_code == 404


def test_insert_job_no_sql_returns_400(bq_client):
    r = bq_client.post(f"{BASE}/jobs", json={"configuration": {"query": {}}})
    assert r.status_code == 400


def test_sync_query_no_sql_returns_400(bq_client):
    r = bq_client.post(f"{BASE}/queries", json={})
    assert r.status_code == 400


def test_sync_query_errored_returns_empty_result(bq_client):
    """An invalid SQL in sync query returns a jobComplete response with no rows."""
    r = bq_client.post(f"{BASE}/queries", json={"query": "SELECT * FROM nonexistent_table_xyz"})
    assert r.status_code == 200
    body = r.json()
    assert body["jobComplete"] is True
    assert body["rows"] == []


# ---------------------------------------------------------------------------
# Parameterized queries
# ---------------------------------------------------------------------------


def _make_table(bq_client, dataset_id, table_id, rows):
    """Helper: create dataset+table and insert rows."""
    bq_client.post(
        f"{BASE}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": dataset_id}},
    )
    bq_client.post(
        f"{BASE}/datasets/{dataset_id}/tables",
        json={
            "tableReference": {"projectId": PROJECT, "datasetId": dataset_id, "tableId": table_id},
            "schema": {"fields": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "STRING"},
                {"name": "score", "type": "FLOAT"},
            ]},
        },
    )
    bq_client.post(
        f"{BASE}/datasets/{dataset_id}/tables/{table_id}/insertAll",
        json={"rows": [{"json": row} for row in rows]},
    )


def test_named_params_via_jobs(bq_client):
    _make_table(bq_client, "pds1", "pt1", [
        {"id": 1, "name": "Alice", "score": 9.5},
        {"id": 2, "name": "Bob", "score": 7.0},
    ])
    r = bq_client.post(
        f"{BASE}/jobs",
        json={
            "configuration": {
                "query": {
                    "query": "SELECT name FROM pds1.pt1 WHERE id = @user_id",
                    "parameterMode": "NAMED",
                    "queryParameters": [
                        {"name": "user_id", "parameterType": {"type": "INT64"}, "parameterValue": {"value": "1"}},
                    ],
                }
            }
        },
    )
    assert r.status_code == 200
    job_id = r.json()["jobReference"]["jobId"]

    r2 = bq_client.get(f"{BASE}/queries/{job_id}")
    rows = r2.json()["rows"]
    assert len(rows) == 1
    assert rows[0]["f"][0]["v"] == "Alice"


def test_positional_params_via_sync_query(bq_client):
    _make_table(bq_client, "pds2", "pt2", [
        {"id": 10, "name": "Carol", "score": 8.0},
        {"id": 20, "name": "Dave", "score": 6.5},
    ])
    r = bq_client.post(
        f"{BASE}/queries",
        json={
            "query": "SELECT name FROM pds2.pt2 WHERE score > ?",
            "parameterMode": "POSITIONAL",
            "queryParameters": [
                {"parameterType": {"type": "FLOAT64"}, "parameterValue": {"value": "7.0"}},
            ],
        },
    )
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert len(rows) == 1
    assert rows[0]["f"][0]["v"] == "Carol"


def test_named_param_repeated(bq_client):
    """Same @param used twice in one query."""
    _make_table(bq_client, "pds3", "pt3", [
        {"id": 5, "name": "Eve", "score": 5.0},
        {"id": 6, "name": "Frank", "score": 6.0},
    ])
    r = bq_client.post(
        f"{BASE}/queries",
        json={
            "query": "SELECT name FROM pds3.pt3 WHERE id >= @lo AND id <= @lo",
            "parameterMode": "NAMED",
            "queryParameters": [
                {"name": "lo", "parameterType": {"type": "INT64"}, "parameterValue": {"value": "5"}},
            ],
        },
    )
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert len(rows) == 1
    assert rows[0]["f"][0]["v"] == "Eve"


def test_bool_and_string_params(bq_client):
    bq_client.post(
        f"{BASE}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": "pds4"}},
    )
    bq_client.post(
        f"{BASE}/datasets/pds4/tables",
        json={
            "tableReference": {"projectId": PROJECT, "datasetId": "pds4", "tableId": "pt4"},
            "schema": {"fields": [
                {"name": "active", "type": "BOOLEAN"},
                {"name": "label", "type": "STRING"},
            ]},
        },
    )
    bq_client.post(
        f"{BASE}/datasets/pds4/tables/pt4/insertAll",
        json={"rows": [
            {"json": {"active": True, "label": "yes"}},
            {"json": {"active": False, "label": "no"}},
        ]},
    )
    r = bq_client.post(
        f"{BASE}/queries",
        json={
            "query": "SELECT label FROM pds4.pt4 WHERE active = @flag AND label = @lbl",
            "parameterMode": "NAMED",
            "queryParameters": [
                {"name": "flag", "parameterType": {"type": "BOOL"}, "parameterValue": {"value": "true"}},
                {"name": "lbl", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "yes"}},
            ],
        },
    )
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert len(rows) == 1
    assert rows[0]["f"][0]["v"] == "yes"


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------


def test_create_and_query_view(bq_client):
    ds = "vds1"
    bq_client.post(f"{BASE}/datasets", json={"datasetReference": {"projectId": PROJECT, "datasetId": ds}})
    bq_client.post(f"{BASE}/datasets/{ds}/tables", json={
        "tableReference": {"projectId": PROJECT, "datasetId": ds, "tableId": "base"},
        "schema": {"fields": [{"name": "x", "type": "INTEGER"}, {"name": "y", "type": "STRING"}]},
    })
    bq_client.post(f"{BASE}/datasets/{ds}/tables/base/insertAll", json={
        "rows": [{"json": {"x": 1, "y": "a"}}, {"json": {"x": 2, "y": "b"}}, {"json": {"x": 3, "y": "c"}}],
    })

    # Create view over the table
    r = bq_client.post(f"{BASE}/datasets/{ds}/tables", json={
        "tableReference": {"projectId": PROJECT, "datasetId": ds, "tableId": "view1"},
        "view": {"query": f"SELECT x, y FROM `{PROJECT}.{ds}.base` WHERE x > 1", "useLegacySql": False},
    })
    assert r.status_code == 200
    assert r.json()["type"] == "VIEW"

    # GET table returns view metadata
    r = bq_client.get(f"{BASE}/datasets/{ds}/tables/view1")
    assert r.status_code == 200
    assert r.json()["type"] == "VIEW"
    assert "query" in r.json()["view"]

    # List tables includes the view
    r = bq_client.get(f"{BASE}/datasets/{ds}/tables")
    types = {t["tableReference"]["tableId"]: t["type"] for t in r.json()["tables"]}
    assert types["base"] == "TABLE"
    assert types["view1"] == "VIEW"

    # Query the view
    r = bq_client.post(f"{BASE}/queries", json={
        "query": f"SELECT x, y FROM `{PROJECT}.{ds}.view1` ORDER BY x",
        "useLegacySql": False,
    })
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert len(rows) == 2
    assert rows[0]["f"][0]["v"] == "2"
    assert rows[1]["f"][0]["v"] == "3"


def test_update_view(bq_client):
    ds = "vds2"
    bq_client.post(f"{BASE}/datasets", json={"datasetReference": {"projectId": PROJECT, "datasetId": ds}})
    bq_client.post(f"{BASE}/datasets/{ds}/tables", json={
        "tableReference": {"projectId": PROJECT, "datasetId": ds, "tableId": "src"},
        "schema": {"fields": [{"name": "n", "type": "INTEGER"}]},
    })
    bq_client.post(f"{BASE}/datasets/{ds}/tables/src/insertAll", json={
        "rows": [{"json": {"n": 10}}, {"json": {"n": 20}}, {"json": {"n": 30}}],
    })
    bq_client.post(f"{BASE}/datasets/{ds}/tables", json={
        "tableReference": {"projectId": PROJECT, "datasetId": ds, "tableId": "vw"},
        "view": {"query": f"SELECT n FROM `{PROJECT}.{ds}.src` WHERE n < 20", "useLegacySql": False},
    })

    # Original view returns n=10 only
    r = bq_client.post(f"{BASE}/queries", json={"query": f"SELECT n FROM `{PROJECT}.{ds}.vw`", "useLegacySql": False})
    assert len(r.json()["rows"]) == 1

    # Update view to a wider filter
    bq_client.patch(f"{BASE}/datasets/{ds}/tables/vw", json={
        "view": {"query": f"SELECT n FROM `{PROJECT}.{ds}.src` WHERE n <= 20", "useLegacySql": False},
    })

    r = bq_client.post(f"{BASE}/queries", json={"query": f"SELECT n FROM `{PROJECT}.{ds}.vw` ORDER BY n", "useLegacySql": False})
    rows = r.json()["rows"]
    assert len(rows) == 2
    assert rows[0]["f"][0]["v"] == "10"
    assert rows[1]["f"][0]["v"] == "20"


def test_delete_view(bq_client):
    ds = "vds3"
    bq_client.post(f"{BASE}/datasets", json={"datasetReference": {"projectId": PROJECT, "datasetId": ds}})
    bq_client.post(f"{BASE}/datasets/{ds}/tables", json={
        "tableReference": {"projectId": PROJECT, "datasetId": ds, "tableId": "t"},
        "schema": {"fields": [{"name": "v", "type": "INTEGER"}]},
    })
    bq_client.post(f"{BASE}/datasets/{ds}/tables", json={
        "tableReference": {"projectId": PROJECT, "datasetId": ds, "tableId": "vw"},
        "view": {"query": f"SELECT v FROM `{PROJECT}.{ds}.t`", "useLegacySql": False},
    })
    r = bq_client.delete(f"{BASE}/datasets/{ds}/tables/vw")
    assert r.status_code == 204
    assert bq_client.get(f"{BASE}/datasets/{ds}/tables/vw").status_code == 404


# ---------------------------------------------------------------------------
# Table schema evolution
# ---------------------------------------------------------------------------


def test_add_column(bq_client):
    ds = "seds1"
    bq_client.post(f"{BASE}/datasets", json={"datasetReference": {"projectId": PROJECT, "datasetId": ds}})
    bq_client.post(f"{BASE}/datasets/{ds}/tables", json={
        "tableReference": {"projectId": PROJECT, "datasetId": ds, "tableId": "t"},
        "schema": {"fields": [{"name": "id", "type": "INTEGER"}]},
    })
    bq_client.post(f"{BASE}/datasets/{ds}/tables/t/insertAll", json={
        "rows": [{"json": {"id": 1}}, {"json": {"id": 2}}],
    })

    # Add a new column via PATCH
    r = bq_client.patch(f"{BASE}/datasets/{ds}/tables/t", json={
        "schema": {"fields": [
            {"name": "id", "type": "INTEGER"},
            {"name": "label", "type": "STRING"},
        ]},
    })
    assert r.status_code == 200
    schema_fields = {f["name"] for f in r.json()["schema"]["fields"]}
    assert "id" in schema_fields
    assert "label" in schema_fields

    # Insert a row using the new column and query it back
    bq_client.post(f"{BASE}/datasets/{ds}/tables/t/insertAll", json={
        "rows": [{"json": {"id": 3, "label": "hello"}}],
    })
    r = bq_client.post(f"{BASE}/queries", json={
        "query": f"SELECT id, label FROM `{PROJECT}.{ds}.t` WHERE id = 3",
        "useLegacySql": False,
    })
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert len(rows) == 1
    vals = {f["v"] for f in rows[0]["f"]}
    assert "3" in vals
    assert "hello" in vals


def test_update_table_description_and_labels(bq_client):
    ds = "seds2"
    bq_client.post(f"{BASE}/datasets", json={"datasetReference": {"projectId": PROJECT, "datasetId": ds}})
    bq_client.post(f"{BASE}/datasets/{ds}/tables", json={
        "tableReference": {"projectId": PROJECT, "datasetId": ds, "tableId": "t"},
        "schema": {"fields": [{"name": "x", "type": "INTEGER"}]},
        "description": "original",
        "labels": {"env": "dev"},
    })

    r = bq_client.patch(f"{BASE}/datasets/{ds}/tables/t", json={
        "description": "updated",
        "labels": {"env": "prod", "team": "data"},
    })
    assert r.status_code == 200
    assert r.json()["description"] == "updated"
    assert r.json()["labels"]["env"] == "prod"
    assert r.json()["labels"]["team"] == "data"


def test_add_existing_column_is_noop(bq_client):
    """PATCHing with an already-existing field name does not raise."""
    ds = "seds3"
    bq_client.post(f"{BASE}/datasets", json={"datasetReference": {"projectId": PROJECT, "datasetId": ds}})
    bq_client.post(f"{BASE}/datasets/{ds}/tables", json={
        "tableReference": {"projectId": PROJECT, "datasetId": ds, "tableId": "t"},
        "schema": {"fields": [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "STRING"}]},
    })
    r = bq_client.patch(f"{BASE}/datasets/{ds}/tables/t", json={
        "schema": {"fields": [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "STRING"}]},
    })
    assert r.status_code == 200
    assert len(r.json()["schema"]["fields"]) == 2
