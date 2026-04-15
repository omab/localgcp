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
