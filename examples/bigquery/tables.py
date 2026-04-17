"""BigQuery — create dataset and table, insert rows, run a query.

    uv run python examples/bigquery/tables.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import BIGQUERY_BASE, PROJECT, client, ok

DATASET = "example_dataset"
TABLE = "sales"


def main():
    http = client()

    # Create dataset
    ok(http.post(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": DATASET}},
    ))
    print(f"Created dataset: {DATASET}")

    # Create table with schema
    ok(http.post(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables",
        json={
            "tableReference": {"projectId": PROJECT, "datasetId": DATASET, "tableId": TABLE},
            "schema": {"fields": [
                {"name": "product",  "type": "STRING"},
                {"name": "quantity", "type": "INTEGER"},
                {"name": "revenue",  "type": "FLOAT"},
            ]},
        },
    ))
    print(f"Created table: {TABLE}")

    # Insert rows
    ok(http.post(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables/{TABLE}/insertAll",
        json={"rows": [
            {"insertId": "r1", "json": {"product": "Widget", "quantity": 10, "revenue": 49.90}},
            {"insertId": "r2", "json": {"product": "Gadget", "quantity":  5, "revenue": 124.75}},
            {"insertId": "r3", "json": {"product": "Widget", "quantity":  3, "revenue": 14.97}},
        ]},
    ))
    print("Inserted 3 rows")

    # Query: total revenue per product
    r = ok(http.post(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/queries",
        json={
            "query": f"SELECT product, SUM(quantity) AS units, SUM(revenue) AS total FROM `{PROJECT}.{DATASET}.{TABLE}` GROUP BY product ORDER BY total DESC",
            "useLegacySql": False,
        },
    ))
    rows = r.json().get("rows", [])
    print("\nRevenue by product:")
    for row in rows:
        vals = [f["v"] for f in row["f"]]
        print(f"  {vals[0]:10s}  units={vals[1]}  total=${float(vals[2]):.2f}")

    # Schema evolution: add a new column via PATCH
    r = ok(http.patch(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables/{TABLE}",
        json={"schema": {"fields": [
            {"name": "product",  "type": "STRING"},
            {"name": "quantity", "type": "INTEGER"},
            {"name": "revenue",  "type": "FLOAT"},
            {"name": "region",   "type": "STRING"},   # new column
        ]}},
    ))
    cols = [f["name"] for f in r.json()["schema"]["fields"]]
    print(f"\nSchema after evolution: {cols}")

    # Insert a row using the new column
    ok(http.post(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables/{TABLE}/insertAll",
        json={"rows": [{"insertId": "r4", "json": {"product": "Donut", "quantity": 20, "revenue": 9.80, "region": "west"}}]},
    ))
    r = ok(http.post(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/queries",
        json={
            "query": f"SELECT product, region FROM `{PROJECT}.{DATASET}.{TABLE}` WHERE region IS NOT NULL",
            "useLegacySql": False,
        },
    ))
    row = r.json()["rows"][0]
    print(f"New row with region: product={row['f'][0]['v']}, region={row['f'][1]['v']}")

    # Cleanup
    http.delete(f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables/{TABLE}")
    http.delete(f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}?deleteContents=true")
    print("\nCleaned up")


if __name__ == "__main__":
    main()
