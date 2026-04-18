"""GCS — bucket retention policies: set, inspect, enforce, remove.

    uv run python examples/gcs/retention.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import GCS_BASE, client, ok

BUCKET = "retention-example"


def main():
    http = client()

    # Create bucket
    ok(http.post(f"{GCS_BASE}/b", json={"name": BUCKET}))
    print(f"Created bucket: {BUCKET}")

    # Set a retention policy (1 hour)
    r = ok(http.patch(f"{GCS_BASE}/b/{BUCKET}/retentionPolicy", json={
        "retentionPolicy": {"retentionPeriod": "3600"},
    }))
    policy = r.json()["retentionPolicy"]
    print(f"Retention policy set: period={policy['retentionPeriod']}s, effectiveTime={policy['effectiveTime']}")

    # Upload an object — it will inherit the retention expiry
    r = ok(http.post(
        f"{GCS_BASE}/b/{BUCKET}/o?name=report.txt&uploadType=media",
        content=b"quarterly report data",
        headers={"content-type": "text/plain"},
    ))
    obj = r.json()
    print(f"Uploaded object: {obj['name']}, retentionExpirationTime={obj.get('retentionExpirationTime')}")

    # Try to delete — should be blocked by the retention policy
    r = http.delete(f"{GCS_BASE}/b/{BUCKET}/o/report.txt")
    if r.status_code == 403:
        print(f"Delete blocked (expected): {r.json()['error']['message']}")
    else:
        print(f"Delete returned: {r.status_code} (unexpected)")

    # Get the retention policy
    r = ok(http.get(f"{GCS_BASE}/b/{BUCKET}/retentionPolicy"))
    print(f"Policy via dedicated endpoint: {r.json()['retentionPolicy']}")

    # Remove the retention policy
    ok(http.delete(f"{GCS_BASE}/b/{BUCKET}/retentionPolicy"))
    print("Retention policy removed")

    # Now delete should succeed
    r = http.delete(f"{GCS_BASE}/b/{BUCKET}/o/report.txt")
    print(f"Delete after policy removal: {r.status_code} (expected 204)")

    # Cleanup
    http.delete(f"{GCS_BASE}/b/{BUCKET}")
    print("Bucket deleted. Done.")


if __name__ == "__main__":
    main()
