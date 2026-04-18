"""Cloud Logging — log exclusions: create, filter at write time, disable, delete.

    uv run python examples/logging/exclusions.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import LOGGING_BASE, PROJECT, client, ok

LOG_NAME = f"projects/{PROJECT}/logs/app"


def write_entry(http, severity: str, insert_id: str, text: str):
    ok(http.post(f"{LOGGING_BASE}/v2/entries:write", json={
        "entries": [{
            "logName": LOG_NAME,
            "severity": severity,
            "insertId": insert_id,
            "textPayload": text,
            "resource": {"type": "global", "labels": {}},
        }]
    }))


def list_entries(http) -> list[dict]:
    r = ok(http.post(f"{LOGGING_BASE}/v2/entries:list", json={
        "resourceNames": [f"projects/{PROJECT}"],
        "filter": f'logName = "{LOG_NAME}"',
    }))
    return r.json().get("entries", [])


def main():
    http = client()

    # Write some entries before any exclusion
    write_entry(http, "DEBUG", "pre-excl-debug", "debug before exclusion")
    write_entry(http, "INFO", "pre-excl-info", "info before exclusion")
    print(f"Entries before exclusion: {len(list_entries(http))}")

    # Create an exclusion that drops DEBUG entries
    r = ok(http.post(f"{LOGGING_BASE}/v2/projects/{PROJECT}/exclusions", json={
        "name": "drop-debug",
        "description": "Suppress DEBUG-level noise",
        "filter": 'severity = "DEBUG"',
    }))
    excl = r.json()
    print(f"Created exclusion: {excl['name']} — filter: {excl['filter']}")

    # Write entries — DEBUG should be silently dropped
    write_entry(http, "DEBUG", "post-excl-debug", "this should be dropped")
    write_entry(http, "WARNING", "post-excl-warning", "this should be kept")
    entries = list_entries(http)
    ids = {e["insertId"] for e in entries}
    print(f"After exclusion — stored entries: {len(entries)}")
    print(f"  DEBUG dropped: {'post-excl-debug' not in ids}")
    print(f"  WARNING kept:  {'post-excl-warning' in ids}")

    # List all exclusions
    r = ok(http.get(f"{LOGGING_BASE}/v2/projects/{PROJECT}/exclusions"))
    print(f"\nAll exclusions: {[e['name'] for e in r.json()['exclusions']]}")

    # Disable the exclusion — DEBUG entries should now be stored again
    ok(http.patch(f"{LOGGING_BASE}/v2/projects/{PROJECT}/exclusions/drop-debug", json={
        "disabled": True,
    }))
    write_entry(http, "DEBUG", "after-disable-debug", "debug after disabling exclusion")
    ids2 = {e["insertId"] for e in list_entries(http)}
    print(f"\nAfter disabling exclusion — DEBUG stored: {'after-disable-debug' in ids2}")

    # Delete the exclusion
    ok(http.delete(f"{LOGGING_BASE}/v2/projects/{PROJECT}/exclusions/drop-debug"))
    r = http.get(f"{LOGGING_BASE}/v2/projects/{PROJECT}/exclusions/drop-debug")
    print(f"After delete — GET exclusion returns: {r.status_code} (expected 404)")

    print("\nDone.")


if __name__ == "__main__":
    main()
