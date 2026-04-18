"""BigQuery — query INFORMATION_SCHEMA for dataset/table/column metadata.

    uv run python examples/bigquery/information_schema.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import BIGQUERY_BASE, PROJECT, client, ok

DATASET = "catalog_ds"


def query(http, sql: str) -> list[dict]:
    r = ok(http.post(f"{BIGQUERY_BASE}/queries", json={"query": sql, "useLegacySql": False}))
    body = r.json()
    fields = body.get("schema", {}).get("fields", [])
    rows = []
    for row in body.get("rows", []):
        rows.append({fields[i]["name"]: cell["v"] for i, cell in enumerate(row["f"])})
    return rows


def main():
    http = client()

    # Create dataset and tables
    http.post(f"{BIGQUERY_BASE}/datasets", json={
        "datasetReference": {"projectId": PROJECT, "datasetId": DATASET},
    })
    for table_id, schema_fields in [
        ("orders", [
            {"name": "order_id", "type": "INTEGER"},
            {"name": "customer", "type": "STRING"},
            {"name": "amount", "type": "FLOAT"},
        ]),
        ("products", [
            {"name": "product_id", "type": "INTEGER"},
            {"name": "name", "type": "STRING"},
            {"name": "price", "type": "FLOAT"},
            {"name": "in_stock", "type": "BOOLEAN"},
        ]),
    ]:
        http.post(f"{BIGQUERY_BASE}/datasets/{DATASET}/tables", json={
            "tableReference": {"projectId": PROJECT, "datasetId": DATASET, "tableId": table_id},
            "schema": {"fields": schema_fields},
        })

    # INFORMATION_SCHEMA.TABLES
    print("=== INFORMATION_SCHEMA.TABLES ===")
    rows = query(http, f"SELECT table_name, table_type FROM `{PROJECT}.{DATASET}.INFORMATION_SCHEMA.TABLES`")
    for row in rows:
        print(f"  {row['table_name']:20s}  type={row['table_type']}")

    # INFORMATION_SCHEMA.COLUMNS for a specific table
    print("\n=== INFORMATION_SCHEMA.COLUMNS (products) ===")
    rows = query(http, (
        f"SELECT column_name, data_type, is_nullable "
        f"FROM {DATASET}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE table_name = 'products' ORDER BY ordinal_position"
    ))
    for row in rows:
        print(f"  {row['column_name']:15s}  type={row['data_type']:10s}  nullable={row['is_nullable']}")

    # INFORMATION_SCHEMA.SCHEMATA
    print("\n=== INFORMATION_SCHEMA.SCHEMATA ===")
    rows = query(http, "SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA")
    for row in rows:
        print(f"  {row['schema_name']}")

    # Cleanup
    http.delete(f"{BIGQUERY_BASE}/datasets/{DATASET}/tables/orders")
    http.delete(f"{BIGQUERY_BASE}/datasets/{DATASET}/tables/products")
    http.delete(f"{BIGQUERY_BASE}/datasets/{DATASET}?deleteContents=true")
    print("\nDone.")


if __name__ == "__main__":
    main()
