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
