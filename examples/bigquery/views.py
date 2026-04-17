"""BigQuery — create views, query through them, and update view definitions.

    uv run python examples/bigquery/views.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import BIGQUERY_BASE, PROJECT, client, ok

DATASET = "views_dataset"


def main():
    http = client()

    # Setup: dataset and base table
    ok(http.post(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": DATASET}},
    ))
    ok(http.post(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables",
        json={
            "tableReference": {"projectId": PROJECT, "datasetId": DATASET, "tableId": "orders"},
            "schema": {"fields": [
                {"name": "order_id",  "type": "INTEGER"},
                {"name": "customer",  "type": "STRING"},
                {"name": "amount",    "type": "FLOAT"},
                {"name": "status",    "type": "STRING"},
            ]},
        },
    ))
    ok(http.post(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables/orders/insertAll",
        json={"rows": [
            {"insertId": "1", "json": {"order_id": 1, "customer": "alice", "amount": 99.00,  "status": "completed"}},
            {"insertId": "2", "json": {"order_id": 2, "customer": "bob",   "amount": 49.50,  "status": "pending"}},
            {"insertId": "3", "json": {"order_id": 3, "customer": "alice", "amount": 250.00, "status": "completed"}},
            {"insertId": "4", "json": {"order_id": 4, "customer": "carol", "amount": 15.00,  "status": "cancelled"}},
        ]},
    ))
    print("Seeded orders table with 4 rows")

    # Create a view over completed orders only
    r = ok(http.post(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables",
        json={
            "tableReference": {"projectId": PROJECT, "datasetId": DATASET, "tableId": "completed_orders"},
            "view": {
                "query": f"SELECT order_id, customer, amount FROM `{PROJECT}.{DATASET}.orders` WHERE status = 'completed'",
                "useLegacySql": False,
            },
        },
    ))
    assert r.json()["type"] == "VIEW"
    print(f"Created view: completed_orders (type={r.json()['type']})")

    # List tables — view appears alongside the base table
    r = ok(http.get(f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables"))
    for t in r.json()["tables"]:
        print(f"  {t['tableReference']['tableId']:25s} type={t['type']}")

    # Query the view
    r = ok(http.post(f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/queries", json={
        "query": f"SELECT customer, SUM(amount) AS total FROM `{PROJECT}.{DATASET}.completed_orders` GROUP BY customer ORDER BY total DESC",
        "useLegacySql": False,
    }))
    print("\nRevenue from completed orders (via view):")
    for row in r.json().get("rows", []):
        vals = [f["v"] for f in row["f"]]
        print(f"  {vals[0]:10s} ${float(vals[1]):.2f}")

    # Update view to include all non-cancelled orders
    ok(http.patch(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables/completed_orders",
        json={
            "view": {
                "query": f"SELECT order_id, customer, amount FROM `{PROJECT}.{DATASET}.orders` WHERE status != 'cancelled'",
                "useLegacySql": False,
            }
        },
    ))
    print("\nUpdated view to exclude only cancelled orders")

    r = ok(http.post(f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/queries", json={
        "query": f"SELECT COUNT(*) AS n FROM `{PROJECT}.{DATASET}.completed_orders`",
        "useLegacySql": False,
    }))
    print(f"Rows in updated view: {r.json()['rows'][0]['f'][0]['v']}")  # 3 (completed + pending)

    # Cleanup
    http.delete(f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables/completed_orders")
    http.delete(f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables/orders")
    http.delete(f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}?deleteContents=true")
    print("Done")


if __name__ == "__main__":
    main()
