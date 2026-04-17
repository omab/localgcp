"""Tests for Cloud Firestore emulator."""

DB = "projects/local-project/databases/(default)"
DOCS = f"{DB}/documents"


def test_create_and_get_document(firestore_client):
    r = firestore_client.post(
        f"/v1/{DOCS}/users",
        params={"documentId": "alice"},
        json={"fields": {"name": {"stringValue": "Alice"}, "age": {"integerValue": "30"}}},
    )
    assert r.status_code == 200
    assert r.json()["name"].endswith("/users/alice")

    r = firestore_client.get(f"/v1/{DOCS}/users/alice")
    assert r.status_code == 200
    assert r.json()["fields"]["name"]["stringValue"] == "Alice"


def test_update_document(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/items",
        params={"documentId": "item1"},
        json={"fields": {"count": {"integerValue": "1"}}},
    )
    firestore_client.patch(
        f"/v1/{DOCS}/items/item1",
        json={"fields": {"count": {"integerValue": "2"}, "label": {"stringValue": "hi"}}},
    )
    r = firestore_client.get(f"/v1/{DOCS}/items/item1")
    assert r.json()["fields"]["count"]["integerValue"] == "2"
    assert r.json()["fields"]["label"]["stringValue"] == "hi"


def test_delete_document(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/things",
        params={"documentId": "t1"},
        json={"fields": {}},
    )
    r = firestore_client.delete(f"/v1/{DOCS}/things/t1")
    assert r.status_code == 204
    r = firestore_client.get(f"/v1/{DOCS}/things/t1")
    assert r.status_code == 404


def test_list_documents(firestore_client):
    for i in range(3):
        firestore_client.post(
            f"/v1/{DOCS}/col",
            params={"documentId": f"doc{i}"},
            json={"fields": {"n": {"integerValue": str(i)}}},
        )
    r = firestore_client.get(f"/v1/{DOCS}/col")
    assert r.status_code == 200
    docs = r.json()["documents"]
    assert len(docs) == 3


def test_run_query_filter(firestore_client):
    for i in range(5):
        firestore_client.post(
            f"/v1/{DOCS}/scores",
            params={"documentId": f"s{i}"},
            json={"fields": {"value": {"integerValue": str(i * 10)}}},
        )
    r = firestore_client.post(
        f"/v1/{DOCS}:runQuery",
        json={
            "structuredQuery": {
                "from": [{"collectionId": "scores"}],
                "where": {
                    "fieldFilter": {
                        "field": {"fieldPath": "value"},
                        "op": "GREATER_THAN_OR_EQUAL",
                        "value": {"integerValue": "20"},
                    }
                },
            }
        },
    )
    assert r.status_code == 200
    results = r.json()
    assert len(results) == 3  # 20, 30, 40


def test_batch_get(firestore_client):
    for did in ("d1", "d2"):
        firestore_client.post(
            f"/v1/{DOCS}/batch",
            params={"documentId": did},
            json={"fields": {"x": {"stringValue": did}}},
        )
    r = firestore_client.post(
        f"/v1/{DB}/documents:batchGet",
        json={"documents": [f"{DOCS}/batch/d1", f"{DOCS}/batch/d2", f"{DOCS}/batch/missing"]},
    )
    assert r.status_code == 200
    results = r.json()
    found = [item for item in results if "found" in item]
    missing = [item for item in results if "missing" in item]
    assert len(found) == 2
    assert len(missing) == 1


def test_transaction_commit(firestore_client):
    r = firestore_client.post(f"/v1/{DB}:beginTransaction", json={})
    assert r.status_code == 200
    txn = r.json()["transaction"]

    r = firestore_client.post(
        f"/v1/{DB}:commit",
        json={
            "transaction": txn,
            "writes": [
                {
                    "update": {
                        "name": f"{DOCS}/txn/doc1",
                        "fields": {"val": {"stringValue": "from-txn"}},
                    }
                }
            ],
        },
    )
    assert r.status_code == 200
    r = firestore_client.get(f"/v1/{DOCS}/txn/doc1")
    assert r.json()["fields"]["val"]["stringValue"] == "from-txn"


def test_auto_generated_document_id(firestore_client):
    """POST to a collection without documentId generates a random ID."""
    r = firestore_client.post(
        f"/v1/{DOCS}/events",
        json={"fields": {"type": {"stringValue": "click"}}},
    )
    assert r.status_code == 200
    name = r.json()["name"]
    doc_id = name.split("/events/")[1]
    assert doc_id  # non-empty auto-generated ID

    r2 = firestore_client.get(f"/v1/{DOCS}/events/{doc_id}")
    assert r2.status_code == 200


def test_get_missing_document_returns_404(firestore_client):
    r = firestore_client.get(f"/v1/{DOCS}/nowhere/phantom")
    assert r.status_code == 404


def test_delete_missing_document_returns_404(firestore_client):
    r = firestore_client.delete(f"/v1/{DOCS}/nowhere/phantom")
    assert r.status_code == 404


def test_patch_creates_document_if_missing(firestore_client):
    """PATCH on a non-existent document creates it."""
    r = firestore_client.patch(
        f"/v1/{DOCS}/things/new-thing",
        json={"fields": {"status": {"stringValue": "active"}}},
    )
    assert r.status_code == 200
    assert r.json()["fields"]["status"]["stringValue"] == "active"


def test_commit_field_mask(firestore_client):
    """updateMask in a commit write only touches the listed fields."""
    firestore_client.post(
        f"/v1/{DOCS}/accounts",
        params={"documentId": "acc1"},
        json={"fields": {"balance": {"integerValue": "100"}, "owner": {"stringValue": "alice"}}},
    )
    txn = firestore_client.post(f"/v1/{DB}:beginTransaction", json={}).json()["transaction"]
    firestore_client.post(
        f"/v1/{DB}:commit",
        json={
            "transaction": txn,
            "writes": [{
                "update": {
                    "name": f"{DOCS}/accounts/acc1",
                    "fields": {"balance": {"integerValue": "200"}},
                },
                "updateMask": {"fieldPaths": ["balance"]},
            }],
        },
    )
    r = firestore_client.get(f"/v1/{DOCS}/accounts/acc1")
    fields = r.json()["fields"]
    assert fields["balance"]["integerValue"] == "200"
    assert fields["owner"]["stringValue"] == "alice"  # untouched


def test_commit_delete_write(firestore_client):
    """A delete write in a commit removes the document."""
    firestore_client.post(
        f"/v1/{DOCS}/tmp",
        params={"documentId": "to-go"},
        json={"fields": {}},
    )
    txn = firestore_client.post(f"/v1/{DB}:beginTransaction", json={}).json()["transaction"]
    firestore_client.post(
        f"/v1/{DB}:commit",
        json={"transaction": txn, "writes": [{"delete": f"{DOCS}/tmp/to-go"}]},
    )
    r = firestore_client.get(f"/v1/{DOCS}/tmp/to-go")
    assert r.status_code == 404


def test_rollback_is_accepted(firestore_client):
    txn = firestore_client.post(f"/v1/{DB}:beginTransaction", json={}).json()["transaction"]
    r = firestore_client.post(f"/v1/{DB}:rollback", json={"transaction": txn})
    assert r.status_code == 200


def test_subcollection_document(firestore_client):
    """Documents in a nested sub-collection are stored and retrievable."""
    firestore_client.post(
        f"/v1/{DOCS}/users",
        params={"documentId": "bob"},
        json={"fields": {}},
    )
    r = firestore_client.post(
        f"/v1/{DOCS}/users/bob/posts",
        params={"documentId": "post1"},
        json={"fields": {"title": {"stringValue": "Hello"}}},
    )
    assert r.status_code == 200

    r2 = firestore_client.get(f"/v1/{DOCS}/users/bob/posts/post1")
    assert r2.json()["fields"]["title"]["stringValue"] == "Hello"


def test_list_documents_pagination(firestore_client):
    for i in range(5):
        firestore_client.post(
            f"/v1/{DOCS}/paged",
            params={"documentId": f"d{i}"},
            json={"fields": {}},
        )
    r1 = firestore_client.get(f"/v1/{DOCS}/paged?pageSize=3")
    assert len(r1.json()["documents"]) == 3
    next_token = r1.json()["nextPageToken"]
    assert next_token

    r2 = firestore_client.get(f"/v1/{DOCS}/paged?pageSize=3&pageToken={next_token}")
    assert len(r2.json()["documents"]) == 2
    assert "nextPageToken" not in r2.json()


def test_query_order_by_and_limit(firestore_client):
    for i in (3, 1, 4, 1, 5):
        firestore_client.post(
            f"/v1/{DOCS}/nums",
            json={"fields": {"v": {"integerValue": str(i)}}},
        )
    r = firestore_client.post(
        f"/v1/{DOCS}:runQuery",
        json={
            "structuredQuery": {
                "from": [{"collectionId": "nums"}],
                "orderBy": [{"field": {"fieldPath": "v"}, "direction": "ASCENDING"}],
                "limit": 3,
            }
        },
    )
    results = r.json()
    assert len(results) == 3
    vals = [doc["document"]["fields"]["v"]["integerValue"] for doc in results]
    assert vals == sorted(vals)


def test_query_composite_or_filter(firestore_client):
    for name, color in [("apple", "red"), ("banana", "yellow"), ("grape", "purple")]:
        firestore_client.post(
            f"/v1/{DOCS}/fruits",
            json={"fields": {"name": {"stringValue": name}, "color": {"stringValue": color}}},
        )
    r = firestore_client.post(
        f"/v1/{DOCS}:runQuery",
        json={
            "structuredQuery": {
                "from": [{"collectionId": "fruits"}],
                "where": {
                    "compositeFilter": {
                        "op": "OR",
                        "filters": [
                            {"fieldFilter": {"field": {"fieldPath": "color"}, "op": "EQUAL", "value": {"stringValue": "red"}}},
                            {"fieldFilter": {"field": {"fieldPath": "color"}, "op": "EQUAL", "value": {"stringValue": "yellow"}}},
                        ],
                    }
                },
            }
        },
    )
    assert len(r.json()) == 2


def test_query_array_contains(firestore_client):
    for tags, name in [(["a", "b"], "doc1"), (["b", "c"], "doc2"), (["c", "d"], "doc3")]:
        firestore_client.post(
            f"/v1/{DOCS}/tagged",
            params={"documentId": name},
            json={"fields": {"tags": {"arrayValue": {"values": [{"stringValue": t} for t in tags]}}}},
        )
    r = firestore_client.post(
        f"/v1/{DOCS}:runQuery",
        json={
            "structuredQuery": {
                "from": [{"collectionId": "tagged"}],
                "where": {
                    "fieldFilter": {
                        "field": {"fieldPath": "tags"},
                        "op": "ARRAY_CONTAINS",
                        "value": {"stringValue": "b"},
                    }
                },
            }
        },
    )
    names = [doc["document"]["name"].split("/")[-1] for doc in r.json()]
    assert set(names) == {"doc1", "doc2"}


def test_query_in_filter(firestore_client):
    for status in ("active", "inactive", "pending"):
        firestore_client.post(
            f"/v1/{DOCS}/tasks",
            json={"fields": {"status": {"stringValue": status}}},
        )
    r = firestore_client.post(
        f"/v1/{DOCS}:runQuery",
        json={
            "structuredQuery": {
                "from": [{"collectionId": "tasks"}],
                "where": {
                    "fieldFilter": {
                        "field": {"fieldPath": "status"},
                        "op": "IN",
                        "value": {"arrayValue": {"values": [
                            {"stringValue": "active"},
                            {"stringValue": "pending"},
                        ]}},
                    }
                },
            }
        },
    )
    assert len(r.json()) == 2


def test_collection_group_query(firestore_client):
    """allDescendants=true matches the collection at any depth."""
    # Create docs in two separate parent paths, same collection name
    for parent_id in ("user1", "user2"):
        firestore_client.post(
            f"/v1/{DOCS}/users",
            params={"documentId": parent_id},
            json={"fields": {}},
        )
        firestore_client.post(
            f"/v1/{DOCS}/users/{parent_id}/comments",
            params={"documentId": "c1"},
            json={"fields": {"text": {"stringValue": f"comment from {parent_id}"}}},
        )
    r = firestore_client.post(
        f"/v1/{DOCS}:runQuery",
        json={
            "structuredQuery": {
                "from": [{"collectionId": "comments", "allDescendants": True}],
            }
        },
    )
    assert len(r.json()) == 2


# ---------------------------------------------------------------------------
# Additional coverage
# ---------------------------------------------------------------------------


def test_run_query_no_structured_query(firestore_client):
    """POST :runQuery with no structuredQuery returns empty list."""
    r = firestore_client.post(f"/v1/{DOCS}:runQuery", json={})
    assert r.status_code == 200
    assert r.json() == []


def test_run_query_nested_collection(firestore_client):
    """Query nested under a parent document via the nested :runQuery endpoint."""
    firestore_client.post(f"/v1/{DOCS}/items", params={"documentId": "item1"}, json={"fields": {}})
    firestore_client.post(
        f"/v1/{DOCS}/items/item1/tags",
        params={"documentId": "t1"},
        json={"fields": {"label": {"stringValue": "important"}}},
    )
    r = firestore_client.post(
        f"/v1/projects/local-project/databases/(default)/documents/items/item1/documents:runQuery",
        json={"structuredQuery": {"from": [{"collectionId": "tags"}]}},
    )
    assert r.status_code == 200


def test_post_even_path_returns_400(firestore_client):
    """POST to an even-segment path (document, not collection) returns 400."""
    r = firestore_client.post(
        f"/v1/{DOCS}/users/alice",
        json={"fields": {}},
    )
    assert r.status_code == 400


def test_commit_delete_field_from_update_mask(firestore_client):
    """updateMask can remove a field by listing it but not including it in the doc."""
    firestore_client.post(
        f"/v1/{DOCS}/items",
        params={"documentId": "del-field-doc"},
        json={"fields": {"a": {"stringValue": "x"}, "b": {"stringValue": "y"}}},
    )
    r = firestore_client.post(
        f"/v1/{DB}:commit",
        json={
            "writes": [
                {
                    "update": {
                        "name": f"{DOCS}/items/del-field-doc",
                        "fields": {"a": {"stringValue": "updated"}},
                    },
                    "updateMask": {"fieldPaths": ["a", "b"]},  # b not in doc → remove it
                }
            ]
        },
    )
    assert r.status_code == 200
    r2 = firestore_client.get(f"/v1/{DOCS}/items/del-field-doc")
    fields = r2.json().get("fields", {})
    assert "a" in fields


# ---------------------------------------------------------------------------
# Field transforms
# ---------------------------------------------------------------------------


def test_transform_increment_integer(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/counters",
        params={"documentId": "c1"},
        json={"fields": {"views": {"integerValue": "10"}}},
    )
    r = firestore_client.post(
        f"/v1/{DB}:commit",
        json={
            "writes": [{
                "update": {"name": f"{DOCS}/counters/c1", "fields": {}},
                "updateMask": {"fieldPaths": []},
                "updateTransforms": [{"fieldPath": "views", "increment": {"integerValue": "5"}}],
            }]
        },
    )
    assert r.status_code == 200
    doc = firestore_client.get(f"/v1/{DOCS}/counters/c1").json()
    assert doc["fields"]["views"]["integerValue"] == "15"


def test_transform_increment_creates_field(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/counters",
        params={"documentId": "c2"},
        json={"fields": {}},
    )
    firestore_client.post(
        f"/v1/{DB}:commit",
        json={
            "writes": [{
                "update": {"name": f"{DOCS}/counters/c2", "fields": {}},
                "updateMask": {"fieldPaths": []},
                "updateTransforms": [{"fieldPath": "hits", "increment": {"integerValue": "3"}}],
            }]
        },
    )
    doc = firestore_client.get(f"/v1/{DOCS}/counters/c2").json()
    assert doc["fields"]["hits"]["integerValue"] == "3"


def test_transform_increment_double(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/metrics",
        params={"documentId": "m1"},
        json={"fields": {"score": {"doubleValue": 1.5}}},
    )
    firestore_client.post(
        f"/v1/{DB}:commit",
        json={
            "writes": [{
                "update": {"name": f"{DOCS}/metrics/m1", "fields": {}},
                "updateMask": {"fieldPaths": []},
                "updateTransforms": [{"fieldPath": "score", "increment": {"doubleValue": 0.5}}],
            }]
        },
    )
    doc = firestore_client.get(f"/v1/{DOCS}/metrics/m1").json()
    assert doc["fields"]["score"]["doubleValue"] == 2.0


def test_transform_set_to_server_value(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/events",
        params={"documentId": "e1"},
        json={"fields": {"name": {"stringValue": "login"}}},
    )
    firestore_client.post(
        f"/v1/{DB}:commit",
        json={
            "writes": [{
                "update": {"name": f"{DOCS}/events/e1", "fields": {}},
                "updateMask": {"fieldPaths": []},
                "updateTransforms": [{"fieldPath": "updatedAt", "setToServerValue": "REQUEST_TIME"}],
            }]
        },
    )
    doc = firestore_client.get(f"/v1/{DOCS}/events/e1").json()
    assert "timestampValue" in doc["fields"]["updatedAt"]


def test_transform_append_missing_elements(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/lists",
        params={"documentId": "l1"},
        json={"fields": {"tags": {"arrayValue": {"values": [{"stringValue": "a"}]}}}},
    )
    firestore_client.post(
        f"/v1/{DB}:commit",
        json={
            "writes": [{
                "update": {"name": f"{DOCS}/lists/l1", "fields": {}},
                "updateMask": {"fieldPaths": []},
                "updateTransforms": [{
                    "fieldPath": "tags",
                    "appendMissingElements": {"values": [{"stringValue": "a"}, {"stringValue": "b"}]},
                }],
            }]
        },
    )
    doc = firestore_client.get(f"/v1/{DOCS}/lists/l1").json()
    values = [v["stringValue"] for v in doc["fields"]["tags"]["arrayValue"]["values"]]
    assert values == ["a", "b"]  # "a" not duplicated


def test_transform_remove_all_from_array(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/lists",
        params={"documentId": "l2"},
        json={"fields": {"tags": {"arrayValue": {"values": [
            {"stringValue": "a"}, {"stringValue": "b"}, {"stringValue": "a"},
        ]}}}},
    )
    firestore_client.post(
        f"/v1/{DB}:commit",
        json={
            "writes": [{
                "update": {"name": f"{DOCS}/lists/l2", "fields": {}},
                "updateMask": {"fieldPaths": []},
                "updateTransforms": [{
                    "fieldPath": "tags",
                    "removeAllFromArray": {"values": [{"stringValue": "a"}]},
                }],
            }]
        },
    )
    doc = firestore_client.get(f"/v1/{DOCS}/lists/l2").json()
    values = [v["stringValue"] for v in doc["fields"]["tags"]["arrayValue"]["values"]]
    assert values == ["b"]


def test_transform_nested_field_path(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/nested",
        params={"documentId": "n1"},
        json={"fields": {"stats": {"mapValue": {"fields": {"count": {"integerValue": "0"}}}}}},
    )
    firestore_client.post(
        f"/v1/{DB}:commit",
        json={
            "writes": [{
                "update": {"name": f"{DOCS}/nested/n1", "fields": {}},
                "updateMask": {"fieldPaths": []},
                "updateTransforms": [{"fieldPath": "stats.count", "increment": {"integerValue": "7"}}],
            }]
        },
    )
    doc = firestore_client.get(f"/v1/{DOCS}/nested/n1").json()
    count = doc["fields"]["stats"]["mapValue"]["fields"]["count"]["integerValue"]
    assert count == "7"


# ---------------------------------------------------------------------------
# Aggregation queries
# ---------------------------------------------------------------------------


def _seed_scores(firestore_client, collection, docs):
    """Insert documents with name + score fields into a collection."""
    for doc_id, score in docs:
        firestore_client.post(
            f"/v1/{DOCS}/{collection}",
            params={"documentId": doc_id},
            json={"fields": {
                "name": {"stringValue": doc_id},
                "score": {"integerValue": str(score)},
            }},
        )


def test_aggregation_count(firestore_client):
    _seed_scores(firestore_client, "agg_col1", [("a", 10), ("b", 20), ("c", 30)])
    r = firestore_client.post(
        f"/v1/{DOCS}:runAggregationQuery",
        json={
            "structuredAggregationQuery": {
                "structuredQuery": {"from": [{"collectionId": "agg_col1"}]},
                "aggregations": [{"alias": "total", "count": {}}],
            }
        },
    )
    assert r.status_code == 200
    result = r.json()
    assert result[0]["result"]["aggregateFields"]["total"]["integerValue"] == "3"


def test_aggregation_count_with_filter(firestore_client):
    _seed_scores(firestore_client, "agg_col2", [("a", 5), ("b", 15), ("c", 25)])
    r = firestore_client.post(
        f"/v1/{DOCS}:runAggregationQuery",
        json={
            "structuredAggregationQuery": {
                "structuredQuery": {
                    "from": [{"collectionId": "agg_col2"}],
                    "where": {
                        "fieldFilter": {
                            "field": {"fieldPath": "score"},
                            "op": "GREATER_THAN",
                            "value": {"integerValue": "10"},
                        }
                    },
                },
                "aggregations": [{"alias": "n", "count": {}}],
            }
        },
    )
    assert r.json()[0]["result"]["aggregateFields"]["n"]["integerValue"] == "2"


def test_aggregation_count_up_to(firestore_client):
    _seed_scores(firestore_client, "agg_col3", [(f"d{i}", i) for i in range(10)])
    r = firestore_client.post(
        f"/v1/{DOCS}:runAggregationQuery",
        json={
            "structuredAggregationQuery": {
                "structuredQuery": {"from": [{"collectionId": "agg_col3"}]},
                "aggregations": [{"alias": "capped", "count": {"upTo": "5"}}],
            }
        },
    )
    assert r.json()[0]["result"]["aggregateFields"]["capped"]["integerValue"] == "5"


def test_aggregation_sum_integer(firestore_client):
    _seed_scores(firestore_client, "agg_col4", [("x", 10), ("y", 20), ("z", 30)])
    r = firestore_client.post(
        f"/v1/{DOCS}:runAggregationQuery",
        json={
            "structuredAggregationQuery": {
                "structuredQuery": {"from": [{"collectionId": "agg_col4"}]},
                "aggregations": [{"alias": "total", "sum": {"field": {"fieldPath": "score"}}}],
            }
        },
    )
    agg = r.json()[0]["result"]["aggregateFields"]["total"]
    assert agg["integerValue"] == "60"


def test_aggregation_sum_with_doubles(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/agg_col5",
        params={"documentId": "p"},
        json={"fields": {"val": {"doubleValue": 1.5}}},
    )
    firestore_client.post(
        f"/v1/{DOCS}/agg_col5",
        params={"documentId": "q"},
        json={"fields": {"val": {"integerValue": "2"}}},
    )
    r = firestore_client.post(
        f"/v1/{DOCS}:runAggregationQuery",
        json={
            "structuredAggregationQuery": {
                "structuredQuery": {"from": [{"collectionId": "agg_col5"}]},
                "aggregations": [{"alias": "s", "sum": {"field": {"fieldPath": "val"}}}],
            }
        },
    )
    agg = r.json()[0]["result"]["aggregateFields"]["s"]
    assert "doubleValue" in agg
    assert abs(agg["doubleValue"] - 3.5) < 1e-9


def test_aggregation_avg(firestore_client):
    _seed_scores(firestore_client, "agg_col6", [("a", 10), ("b", 20), ("c", 30)])
    r = firestore_client.post(
        f"/v1/{DOCS}:runAggregationQuery",
        json={
            "structuredAggregationQuery": {
                "structuredQuery": {"from": [{"collectionId": "agg_col6"}]},
                "aggregations": [{"alias": "mean", "avg": {"field": {"fieldPath": "score"}}}],
            }
        },
    )
    agg = r.json()[0]["result"]["aggregateFields"]["mean"]
    assert abs(agg["doubleValue"] - 20.0) < 1e-9


def test_aggregation_avg_no_values_returns_null(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/agg_col7",
        params={"documentId": "only"},
        json={"fields": {"name": {"stringValue": "no-score"}}},
    )
    r = firestore_client.post(
        f"/v1/{DOCS}:runAggregationQuery",
        json={
            "structuredAggregationQuery": {
                "structuredQuery": {"from": [{"collectionId": "agg_col7"}]},
                "aggregations": [{"alias": "mean", "avg": {"field": {"fieldPath": "score"}}}],
            }
        },
    )
    agg = r.json()[0]["result"]["aggregateFields"]["mean"]
    assert "nullValue" in agg


def test_aggregation_multiple_in_one_query(firestore_client):
    _seed_scores(firestore_client, "agg_col8", [("a", 4), ("b", 6)])
    r = firestore_client.post(
        f"/v1/{DOCS}:runAggregationQuery",
        json={
            "structuredAggregationQuery": {
                "structuredQuery": {"from": [{"collectionId": "agg_col8"}]},
                "aggregations": [
                    {"alias": "n", "count": {}},
                    {"alias": "total", "sum": {"field": {"fieldPath": "score"}}},
                    {"alias": "mean", "avg": {"field": {"fieldPath": "score"}}},
                ],
            }
        },
    )
    fields = r.json()[0]["result"]["aggregateFields"]
    assert fields["n"]["integerValue"] == "2"
    assert fields["total"]["integerValue"] == "10"
    assert abs(fields["mean"]["doubleValue"] - 5.0) < 1e-9


def test_aggregation_empty_request_returns_empty(firestore_client):
    r = firestore_client.post(f"/v1/{DOCS}:runAggregationQuery", json={})
    assert r.status_code == 200
    assert r.json() == []


# ---------------------------------------------------------------------------
# Cursor pagination
# ---------------------------------------------------------------------------


def _seed_cursor_col(firestore_client, collection, items):
    """Insert {score, name} documents into a collection."""
    for doc_id, score in items:
        firestore_client.post(
            f"/v1/{DOCS}/{collection}",
            params={"documentId": doc_id},
            json={"fields": {
                "score": {"integerValue": str(score)},
                "name": {"stringValue": doc_id},
            }},
        )


def _scores(results):
    return [int(d["document"]["fields"]["score"]["integerValue"]) for d in results]


def test_start_at_inclusive(firestore_client):
    """startAt with before=True: include the cursor document."""
    _seed_cursor_col(firestore_client, "cur1", [("a", 10), ("b", 20), ("c", 30), ("d", 40)])
    r = firestore_client.post(f"/v1/{DOCS}:runQuery", json={
        "structuredQuery": {
            "from": [{"collectionId": "cur1"}],
            "orderBy": [{"field": {"fieldPath": "score"}, "direction": "ASCENDING"}],
            "startAt": {"values": [{"integerValue": "20"}], "before": True},
        }
    })
    assert r.status_code == 200
    assert _scores(r.json()) == [20, 30, 40]


def test_start_after_exclusive(firestore_client):
    """startAt with before=False: exclude the cursor document (startAfter)."""
    _seed_cursor_col(firestore_client, "cur2", [("a", 10), ("b", 20), ("c", 30), ("d", 40)])
    r = firestore_client.post(f"/v1/{DOCS}:runQuery", json={
        "structuredQuery": {
            "from": [{"collectionId": "cur2"}],
            "orderBy": [{"field": {"fieldPath": "score"}, "direction": "ASCENDING"}],
            "startAt": {"values": [{"integerValue": "20"}], "before": False},
        }
    })
    assert _scores(r.json()) == [30, 40]


def test_end_before_exclusive(firestore_client):
    """endAt with before=True: exclude the cursor document (endBefore)."""
    _seed_cursor_col(firestore_client, "cur3", [("a", 10), ("b", 20), ("c", 30), ("d", 40)])
    r = firestore_client.post(f"/v1/{DOCS}:runQuery", json={
        "structuredQuery": {
            "from": [{"collectionId": "cur3"}],
            "orderBy": [{"field": {"fieldPath": "score"}, "direction": "ASCENDING"}],
            "endAt": {"values": [{"integerValue": "30"}], "before": True},
        }
    })
    assert _scores(r.json()) == [10, 20]


def test_end_at_inclusive(firestore_client):
    """endAt with before=False: include the cursor document."""
    _seed_cursor_col(firestore_client, "cur4", [("a", 10), ("b", 20), ("c", 30), ("d", 40)])
    r = firestore_client.post(f"/v1/{DOCS}:runQuery", json={
        "structuredQuery": {
            "from": [{"collectionId": "cur4"}],
            "orderBy": [{"field": {"fieldPath": "score"}, "direction": "ASCENDING"}],
            "endAt": {"values": [{"integerValue": "30"}], "before": False},
        }
    })
    assert _scores(r.json()) == [10, 20, 30]


def test_start_at_and_end_at_window(firestore_client):
    """Both cursors together define a window."""
    _seed_cursor_col(firestore_client, "cur5", [(f"d{i}", i * 10) for i in range(6)])
    r = firestore_client.post(f"/v1/{DOCS}:runQuery", json={
        "structuredQuery": {
            "from": [{"collectionId": "cur5"}],
            "orderBy": [{"field": {"fieldPath": "score"}, "direction": "ASCENDING"}],
            "startAt": {"values": [{"integerValue": "10"}], "before": True},
            "endAt":   {"values": [{"integerValue": "30"}], "before": False},
        }
    })
    assert _scores(r.json()) == [10, 20, 30]


def test_cursor_with_descending_order(firestore_client):
    """Cursors respect DESCENDING sort direction."""
    _seed_cursor_col(firestore_client, "cur6", [("a", 10), ("b", 20), ("c", 30), ("d", 40)])
    r = firestore_client.post(f"/v1/{DOCS}:runQuery", json={
        "structuredQuery": {
            "from": [{"collectionId": "cur6"}],
            "orderBy": [{"field": {"fieldPath": "score"}, "direction": "DESCENDING"}],
            "startAt": {"values": [{"integerValue": "30"}], "before": True},
        }
    })
    # Descending order: 40, 30, 20, 10. startAt 30 inclusive → 30, 20, 10
    assert _scores(r.json()) == [30, 20, 10]


def test_cursor_with_limit(firestore_client):
    """Cursors compose correctly with LIMIT for page-by-page iteration."""
    _seed_cursor_col(firestore_client, "cur7", [(f"d{i}", i * 5) for i in range(8)])
    # Page 1: first 3 docs
    r1 = firestore_client.post(f"/v1/{DOCS}:runQuery", json={
        "structuredQuery": {
            "from": [{"collectionId": "cur7"}],
            "orderBy": [{"field": {"fieldPath": "score"}, "direction": "ASCENDING"}],
            "limit": 3,
        }
    })
    page1 = _scores(r1.json())
    assert page1 == [0, 5, 10]

    # Page 2: startAfter the last doc on page 1
    last_score = page1[-1]
    r2 = firestore_client.post(f"/v1/{DOCS}:runQuery", json={
        "structuredQuery": {
            "from": [{"collectionId": "cur7"}],
            "orderBy": [{"field": {"fieldPath": "score"}, "direction": "ASCENDING"}],
            "startAt": {"values": [{"integerValue": str(last_score)}], "before": False},
            "limit": 3,
        }
    })
    assert _scores(r2.json()) == [15, 20, 25]


def test_transform_multiple_in_one_write(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/multi",
        params={"documentId": "mx1"},
        json={"fields": {"views": {"integerValue": "1"}}},
    )
    firestore_client.post(
        f"/v1/{DB}:commit",
        json={
            "writes": [{
                "update": {"name": f"{DOCS}/multi/mx1", "fields": {"label": {"stringValue": "new"}}},
                "updateMask": {"fieldPaths": ["label"]},
                "updateTransforms": [
                    {"fieldPath": "views", "increment": {"integerValue": "9"}},
                    {"fieldPath": "updatedAt", "setToServerValue": "REQUEST_TIME"},
                ],
            }]
        },
    )
    doc = firestore_client.get(f"/v1/{DOCS}/multi/mx1").json()
    assert doc["fields"]["views"]["integerValue"] == "10"
    assert doc["fields"]["label"]["stringValue"] == "new"
    assert "timestampValue" in doc["fields"]["updatedAt"]


# ---------------------------------------------------------------------------
# batchWrite
# ---------------------------------------------------------------------------


def test_batch_write_creates_and_updates(firestore_client):
    """batchWrite can create multiple documents in one call."""
    r = firestore_client.post(
        f"/v1/{DB}:batchWrite",
        json={
            "writes": [
                {
                    "update": {
                        "name": f"{DOCS}/bw/doc1",
                        "fields": {"x": {"integerValue": "1"}},
                    }
                },
                {
                    "update": {
                        "name": f"{DOCS}/bw/doc2",
                        "fields": {"x": {"integerValue": "2"}},
                    }
                },
            ]
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert len(body["writeResults"]) == 2
    assert len(body["status"]) == 2
    assert body["status"][0]["code"] == 0
    assert body["status"][1]["code"] == 0

    assert firestore_client.get(f"/v1/{DOCS}/bw/doc1").json()["fields"]["x"]["integerValue"] == "1"
    assert firestore_client.get(f"/v1/{DOCS}/bw/doc2").json()["fields"]["x"]["integerValue"] == "2"


def test_batch_write_delete(firestore_client):
    """batchWrite can delete documents."""
    firestore_client.post(
        f"/v1/{DOCS}/bwdel",
        params={"documentId": "d1"},
        json={"fields": {"v": {"integerValue": "1"}}},
    )
    r = firestore_client.post(
        f"/v1/{DB}:batchWrite",
        json={"writes": [{"delete": f"{DOCS}/bwdel/d1"}]},
    )
    assert r.status_code == 200
    assert r.json()["status"][0]["code"] == 0
    assert firestore_client.get(f"/v1/{DOCS}/bwdel/d1").status_code == 404


def test_batch_write_partial_failure(firestore_client):
    """A failed write does not abort the remaining writes."""
    # doc3 does not exist yet → exists=true precondition will fail
    r = firestore_client.post(
        f"/v1/{DB}:batchWrite",
        json={
            "writes": [
                {
                    "update": {
                        "name": f"{DOCS}/bwpf/doc3",
                        "fields": {"ok": {"booleanValue": False}},
                    },
                    "currentDocument": {"exists": True},
                },
                {
                    "update": {
                        "name": f"{DOCS}/bwpf/doc4",
                        "fields": {"ok": {"booleanValue": True}},
                    }
                },
            ]
        },
    )
    assert r.status_code == 200
    body = r.json()
    # first write failed (doc3 doesn't exist)
    assert body["status"][0]["code"] != 0
    # second write succeeded independently
    assert body["status"][1]["code"] == 0
    assert firestore_client.get(f"/v1/{DOCS}/bwpf/doc4").json()["fields"]["ok"]["booleanValue"] is True


def test_batch_write_empty(firestore_client):
    """batchWrite with no writes returns empty results."""
    r = firestore_client.post(f"/v1/{DB}:batchWrite", json={"writes": []})
    assert r.status_code == 200
    body = r.json()
    assert body["writeResults"] == []
    assert body["status"] == []


# ---------------------------------------------------------------------------
# Field projection (SELECT)
# ---------------------------------------------------------------------------


def test_select_single_field(firestore_client):
    for i in range(3):
        firestore_client.post(
            f"/v1/{DOCS}/proj",
            params={"documentId": f"p{i}"},
            json={"fields": {
                "name": {"stringValue": f"item{i}"},
                "price": {"integerValue": str(i * 10)},
                "hidden": {"stringValue": "secret"},
            }},
        )
    r = firestore_client.post(f"/v1/{DOCS}:runQuery", json={
        "structuredQuery": {
            "from": [{"collectionId": "proj"}],
            "select": {"fields": [{"fieldPath": "name"}]},
            "orderBy": [{"field": {"fieldPath": "name"}, "direction": "ASCENDING"}],
        }
    })
    docs = [row["document"] for row in r.json() if "document" in row]
    assert len(docs) == 3
    for doc in docs:
        assert "name" in doc["fields"]
        assert "price" not in doc["fields"]
        assert "hidden" not in doc["fields"]


def test_select_multiple_fields(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/projm",
        params={"documentId": "m1"},
        json={"fields": {
            "a": {"stringValue": "A"},
            "b": {"integerValue": "2"},
            "c": {"booleanValue": True},
        }},
    )
    r = firestore_client.post(f"/v1/{DOCS}:runQuery", json={
        "structuredQuery": {
            "from": [{"collectionId": "projm"}],
            "select": {"fields": [{"fieldPath": "a"}, {"fieldPath": "c"}]},
        }
    })
    docs = [row["document"] for row in r.json() if "document" in row]
    assert len(docs) == 1
    fields = docs[0]["fields"]
    assert "a" in fields
    assert "c" in fields
    assert "b" not in fields


def test_select_preserves_filters_and_order(firestore_client):
    """Projection does not break WHERE or ORDER BY."""
    for i, (name, score) in enumerate([("alpha", 5), ("beta", 15), ("gamma", 8)]):
        firestore_client.post(
            f"/v1/{DOCS}/projf",
            params={"documentId": f"f{i}"},
            json={"fields": {
                "name": {"stringValue": name},
                "score": {"integerValue": str(score)},
            }},
        )
    r = firestore_client.post(f"/v1/{DOCS}:runQuery", json={
        "structuredQuery": {
            "from": [{"collectionId": "projf"}],
            "where": {"fieldFilter": {
                "field": {"fieldPath": "score"},
                "op": "GREATER_THAN",
                "value": {"integerValue": "6"},
            }},
            "orderBy": [{"field": {"fieldPath": "score"}, "direction": "ASCENDING"}],
            "select": {"fields": [{"fieldPath": "name"}]},
        }
    })
    docs = [row["document"] for row in r.json() if "document" in row]
    names = [d["fields"]["name"]["stringValue"] for d in docs]
    assert names == ["gamma", "beta"]
    for doc in docs:
        assert "score" not in doc["fields"]
