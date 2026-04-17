"""Firestore — runQuery with filters, ordering, pagination, and aggregation.

    uv run python examples/firestore/queries.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import FIRESTORE_BASE, PROJECT, client, ok

DB = f"projects/{PROJECT}/databases/(default)"
DOCS = f"{DB}/documents"
COLLECTION = "products"


def seed(http):
    items = [
        ("p1", "Widget",  10, "electronics"),
        ("p2", "Gadget",  25, "electronics"),
        ("p3", "Donut",    2, "food"),
        ("p4", "Laptop", 999, "electronics"),
        ("p5", "Coffee",   5, "food"),
    ]
    for doc_id, name, price, category in items:
        http.post(
            f"{FIRESTORE_BASE}/v1/{DOCS}/{COLLECTION}",
            params={"documentId": doc_id},
            json={"fields": {
                "name":     {"stringValue": name},
                "price":    {"integerValue": str(price)},
                "category": {"stringValue": category},
            }},
        )


def query(http, structured_query):
    r = ok(http.post(f"{FIRESTORE_BASE}/v1/{DOCS}:runQuery", json={"structuredQuery": structured_query}))
    return [row["document"] for row in r.json() if "document" in row]


def main():
    http = client()
    seed(http)

    # Filter: electronics under $100
    docs = query(http, {
        "from": [{"collectionId": COLLECTION}],
        "where": {"compositeFilter": {"op": "AND", "filters": [
            {"fieldFilter": {"field": {"fieldPath": "category"}, "op": "EQUAL",       "value": {"stringValue": "electronics"}}},
            {"fieldFilter": {"field": {"fieldPath": "price"},    "op": "LESS_THAN",   "value": {"integerValue": "100"}}},
        ]}},
        "orderBy": [{"field": {"fieldPath": "price"}, "direction": "ASCENDING"}],
    })
    print("Electronics under $100 (by price):")
    for d in docs:
        f = d["fields"]
        print(f"  {f['name']['stringValue']:10s} ${f['price']['integerValue']}")

    # Cursor pagination: page 1 (limit 2), then page 2 (startAfter last price)
    page1 = query(http, {
        "from": [{"collectionId": COLLECTION}],
        "orderBy": [{"field": {"fieldPath": "price"}, "direction": "ASCENDING"}],
        "limit": 2,
    })
    print("\nPage 1 (cheapest 2):")
    for d in page1:
        f = d["fields"]
        print(f"  {f['name']['stringValue']:10s} ${f['price']['integerValue']}")

    last_price = page1[-1]["fields"]["price"]["integerValue"]
    page2 = query(http, {
        "from": [{"collectionId": COLLECTION}],
        "orderBy": [{"field": {"fieldPath": "price"}, "direction": "ASCENDING"}],
        "startAt": {"values": [{"integerValue": last_price}], "before": False},
        "limit": 2,
    })
    print("Page 2:")
    for d in page2:
        f = d["fields"]
        print(f"  {f['name']['stringValue']:10s} ${f['price']['integerValue']}")

    # Aggregation: count all, sum price of food items
    r = ok(http.post(f"{FIRESTORE_BASE}/v1/{DOCS}:runAggregationQuery", json={
        "structuredAggregationQuery": {
            "structuredQuery": {"from": [{"collectionId": COLLECTION}]},
            "aggregations": [{"alias": "total", "count": {}}],
        }
    }))
    total = r.json()[0]["result"]["aggregateFields"]["total"]["integerValue"]
    print(f"\nTotal products: {total}")

    r = ok(http.post(f"{FIRESTORE_BASE}/v1/{DOCS}:runAggregationQuery", json={
        "structuredAggregationQuery": {
            "structuredQuery": {
                "from": [{"collectionId": COLLECTION}],
                "where": {"fieldFilter": {"field": {"fieldPath": "category"}, "op": "EQUAL", "value": {"stringValue": "food"}}},
            },
            "aggregations": [
                {"alias": "food_count", "count": {}},
                {"alias": "food_total", "sum": {"field": {"fieldPath": "price"}}},
            ],
        }
    }))
    agg = r.json()[0]["result"]["aggregateFields"]
    print(f"Food items: count={agg['food_count']['integerValue']}, price sum={agg['food_total']['integerValue']}")

    # Field projection: return only 'name', not 'price' or 'category'
    docs = query(http, {
        "from": [{"collectionId": COLLECTION}],
        "select": {"fields": [{"fieldPath": "name"}]},
        "orderBy": [{"field": {"fieldPath": "name"}, "direction": "ASCENDING"}],
    })
    names = [d["fields"]["name"]["stringValue"] for d in docs]
    print(f"\nAll names (projected): {names}")
    for doc in docs:
        assert "price" not in doc["fields"], "price should be projected out"

    # Cleanup
    for doc_id in ("p1", "p2", "p3", "p4", "p5"):
        http.delete(f"{FIRESTORE_BASE}/v1/{DOCS}/{COLLECTION}/{doc_id}")


if __name__ == "__main__":
    main()
