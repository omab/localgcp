"""GCS — configure and inspect bucket CORS rules.

    uv run python examples/gcs/cors.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import GCS_BASE, PROJECT, client, ok

BUCKET = "cors-bucket"

CORS_RULES = [
    {
        "origin": ["https://app.example.com", "https://staging.example.com"],
        "method": ["GET", "POST", "PUT", "DELETE"],
        "responseHeader": ["Content-Type", "Authorization", "X-Requested-With"],
        "maxAgeSeconds": 3600,
    }
]


def main():
    http = client()

    ok(http.post(f"{GCS_BASE}/storage/v1/b", params={"project": PROJECT}, json={"name": BUCKET}))
    print(f"Created bucket: {BUCKET}")

    # Default CORS is empty
    r = ok(http.get(f"{GCS_BASE}/storage/v1/b/{BUCKET}/cors"))
    print(f"Default CORS: {r.json()['cors']}")  # []

    # Set CORS rules via dedicated endpoint
    r = ok(http.put(f"{GCS_BASE}/storage/v1/b/{BUCKET}/cors", json={"cors": CORS_RULES}))
    print(f"Set CORS: {len(r.json()['cors'])} rule(s)")

    # CORS is also visible in the full bucket metadata
    r = ok(http.get(f"{GCS_BASE}/storage/v1/b/{BUCKET}"))
    rules = r.json().get("cors", [])
    print(f"Bucket metadata includes CORS: {len(rules)} rule(s)")
    for rule in rules:
        print(f"  origins: {rule['origin']}")
        print(f"  methods: {rule['method']}")
        print(f"  maxAgeSeconds: {rule['maxAgeSeconds']}")

    # Update via PATCH on bucket
    ok(http.patch(f"{GCS_BASE}/storage/v1/b/{BUCKET}", json={"cors": [
        {"origin": ["*"], "method": ["GET"], "maxAgeSeconds": 60}
    ]}))
    r = ok(http.get(f"{GCS_BASE}/storage/v1/b/{BUCKET}/cors"))
    print(f"\nAfter PATCH: origins={r.json()['cors'][0]['origin']}")

    # Remove all CORS rules
    ok(http.delete(f"{GCS_BASE}/storage/v1/b/{BUCKET}/cors"))
    r = ok(http.get(f"{GCS_BASE}/storage/v1/b/{BUCKET}/cors"))
    print(f"After DELETE: {r.json()['cors']}")  # []

    # Cleanup
    http.delete(f"{GCS_BASE}/storage/v1/b/{BUCKET}")
    print("Done")


if __name__ == "__main__":
    main()
