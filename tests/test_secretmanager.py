"""Tests for Secret Manager emulator."""

import base64

PROJECT = "local-project"


def test_create_and_get_secret(sm_client):
    r = sm_client.post(
        f"/v1/projects/{PROJECT}/secrets",
        params={"secretId": "my-secret"},
        json={},
    )
    assert r.status_code == 200
    assert r.json()["name"] == f"projects/{PROJECT}/secrets/my-secret"

    r = sm_client.get(f"/v1/projects/{PROJECT}/secrets/my-secret")
    assert r.status_code == 200


def test_duplicate_secret_returns_409(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "dup"}, json={})
    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "dup"}, json={})
    assert r.status_code == 409


def test_add_and_access_version(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "api-key"}, json={})
    payload = base64.b64encode(b"super-secret-value").decode()

    r = sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/api-key:addVersion",
        json={"payload": {"data": payload}},
    )
    assert r.status_code == 200
    version_name = r.json()["name"]
    assert "/versions/1" in version_name

    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets/api-key/versions/latest:access")
    assert r.status_code == 200
    assert r.json()["payload"]["data"] == payload


def test_multiple_versions_latest_resolves(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "versioned"}, json={})
    for i in range(3):
        sm_client.post(
            f"/v1/projects/{PROJECT}/secrets/versioned:addVersion",
            json={"payload": {"data": base64.b64encode(f"v{i}".encode()).decode()}},
        )

    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets/versioned/versions/latest:access")
    assert r.status_code == 200
    # latest should be version 3
    val = base64.b64decode(r.json()["payload"]["data"]).decode()
    assert val == "v2"


def test_list_secrets(sm_client):
    for name in ("s1", "s2", "s3"):
        sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": name}, json={})
    r = sm_client.get(f"/v1/projects/{PROJECT}/secrets")
    assert r.status_code == 200
    names = [s["name"].split("/")[-1] for s in r.json()["secrets"]]
    assert {"s1", "s2", "s3"}.issubset(set(names))


def test_delete_secret(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "to-del"}, json={})
    r = sm_client.delete(f"/v1/projects/{PROJECT}/secrets/to-del")
    assert r.status_code == 200
    r = sm_client.get(f"/v1/projects/{PROJECT}/secrets/to-del")
    assert r.status_code == 404


def test_destroy_version_clears_payload(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "destroyable"}, json={})
    payload = base64.b64encode(b"sensitive").decode()
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/destroyable:addVersion",
        json={"payload": {"data": payload}},
    )
    sm_client.post(f"/v1/projects/{PROJECT}/secrets/destroyable/versions/1:destroy")

    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets/destroyable/versions/1:access")
    assert r.status_code == 403  # not enabled


def test_update_secret_labels(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "labeled"}, json={})
    r = sm_client.patch(
        f"/v1/projects/{PROJECT}/secrets/labeled",
        json={"labels": {"env": "prod", "team": "backend"}},
    )
    assert r.status_code == 200
    assert r.json()["labels"]["env"] == "prod"


def test_get_version_by_number(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "numbered"}, json={})
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/numbered:addVersion",
        json={"payload": {"data": base64.b64encode(b"v1").decode()}},
    )
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/numbered:addVersion",
        json={"payload": {"data": base64.b64encode(b"v2").decode()}},
    )
    r = sm_client.get(f"/v1/projects/{PROJECT}/secrets/numbered/versions/1")
    assert r.status_code == 200
    assert "/versions/1" in r.json()["name"]

    r2 = sm_client.get(f"/v1/projects/{PROJECT}/secrets/numbered/versions/2")
    assert "/versions/2" in r2.json()["name"]


def test_list_versions(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "multi-ver"}, json={})
    for i in range(3):
        sm_client.post(
            f"/v1/projects/{PROJECT}/secrets/multi-ver:addVersion",
            json={"payload": {"data": base64.b64encode(f"val{i}".encode()).decode()}},
        )
    r = sm_client.get(f"/v1/projects/{PROJECT}/secrets/multi-ver/versions")
    assert r.status_code == 200
    assert r.json()["totalSize"] == 3
    assert len(r.json()["versions"]) == 3


def test_disable_version_blocks_access(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "toggled"}, json={})
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/toggled:addVersion",
        json={"payload": {"data": base64.b64encode(b"secret").decode()}},
    )
    sm_client.post(f"/v1/projects/{PROJECT}/secrets/toggled/versions/1:disable")

    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets/toggled/versions/1:access")
    assert r.status_code == 403

    # Re-enable and access should succeed
    sm_client.post(f"/v1/projects/{PROJECT}/secrets/toggled/versions/1:enable")
    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets/toggled/versions/1:access")
    assert r.status_code == 200


def test_filter_versions_by_state(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "filterable"}, json={})
    for _ in range(2):
        sm_client.post(
            f"/v1/projects/{PROJECT}/secrets/filterable:addVersion",
            json={"payload": {"data": base64.b64encode(b"x").decode()}},
        )
    sm_client.post(f"/v1/projects/{PROJECT}/secrets/filterable/versions/1:disable")

    r = sm_client.get(f"/v1/projects/{PROJECT}/secrets/filterable/versions?filter=state=DISABLED")
    assert r.status_code == 200
    versions = r.json()["versions"]
    assert all(v["state"] == "DISABLED" for v in versions)
    assert len(versions) == 1


def test_delete_secret_cascades_versions(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "cascade"}, json={})
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/cascade:addVersion",
        json={"payload": {"data": base64.b64encode(b"data").decode()}},
    )
    sm_client.delete(f"/v1/projects/{PROJECT}/secrets/cascade")

    r = sm_client.get(f"/v1/projects/{PROJECT}/secrets/cascade/versions")
    assert r.status_code == 404


def test_get_missing_secret_returns_404(sm_client):
    r = sm_client.get(f"/v1/projects/{PROJECT}/secrets/nonexistent")
    assert r.status_code == 404


def test_create_secret_no_id_returns_400(sm_client):
    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets", json={})
    assert r.status_code == 400


def test_update_missing_secret_returns_404(sm_client):
    r = sm_client.patch(
        f"/v1/projects/{PROJECT}/secrets/no-such-secret",
        json={"labels": {"env": "test"}},
    )
    assert r.status_code == 404


def test_delete_missing_secret_returns_404(sm_client):
    r = sm_client.delete(f"/v1/projects/{PROJECT}/secrets/no-such-secret")
    assert r.status_code == 404


def test_add_version_missing_secret_returns_404(sm_client):
    r = sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/no-secret:addVersion",
        json={"payload": {"data": "dGVzdA=="}},
    )
    assert r.status_code == 404


def test_get_missing_version_returns_404(sm_client):
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets?secretId=ver-secret",
        json={"replication": {"automatic": {}}},
    )
    r = sm_client.get(f"/v1/projects/{PROJECT}/secrets/ver-secret/versions/99")
    assert r.status_code == 404


def test_access_missing_version_returns_404(sm_client):
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets?secretId=acc-secret",
        json={"replication": {"automatic": {}}},
    )
    r = sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/acc-secret/versions/99:access",
    )
    assert r.status_code == 404


def test_disable_missing_version_returns_404(sm_client):
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets?secretId=dis-secret",
        json={"replication": {"automatic": {}}},
    )
    r = sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/dis-secret/versions/99:disable",
    )
    assert r.status_code == 404


def test_enable_version_response(sm_client):
    """Enable returns 200 with state ENABLED and preserves the version name."""
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "en1"}, json={})
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/en1:addVersion",
        json={"payload": {"data": base64.b64encode(b"val").decode()}},
    )
    sm_client.post(f"/v1/projects/{PROJECT}/secrets/en1/versions/1:disable")
    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets/en1/versions/1:enable")
    assert r.status_code == 200
    assert r.json()["state"] == "ENABLED"
    assert "/versions/1" in r.json()["name"]


def test_enable_missing_version_returns_404(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "en2"}, json={})
    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets/en2/versions/99:enable")
    assert r.status_code == 404


def test_destroy_missing_version_returns_404(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "dest1"}, json={})
    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets/dest1/versions/99:destroy")
    assert r.status_code == 404


def test_destroy_disabled_version(sm_client):
    """A disabled version can be destroyed; state becomes DESTROYED."""
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "dest2"}, json={})
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/dest2:addVersion",
        json={"payload": {"data": base64.b64encode(b"data").decode()}},
    )
    sm_client.post(f"/v1/projects/{PROJECT}/secrets/dest2/versions/1:disable")
    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets/dest2/versions/1:destroy")
    assert r.status_code == 200
    assert r.json()["state"] == "DESTROYED"


def test_access_destroyed_version_returns_403(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "dest3"}, json={})
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/dest3:addVersion",
        json={"payload": {"data": base64.b64encode(b"sensitive").decode()}},
    )
    sm_client.post(f"/v1/projects/{PROJECT}/secrets/dest3/versions/1:destroy")
    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets/dest3/versions/1:access")
    assert r.status_code == 403


def test_filter_versions_by_enabled_state(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "filt2"}, json={})
    for _ in range(3):
        sm_client.post(
            f"/v1/projects/{PROJECT}/secrets/filt2:addVersion",
            json={"payload": {"data": base64.b64encode(b"x").decode()}},
        )
    sm_client.post(f"/v1/projects/{PROJECT}/secrets/filt2/versions/2:disable")

    r = sm_client.get(f"/v1/projects/{PROJECT}/secrets/filt2/versions?filter=state=ENABLED")
    assert r.status_code == 200
    versions = r.json()["versions"]
    assert all(v["state"] == "ENABLED" for v in versions)
    assert len(versions) == 2


def test_latest_skips_disabled_versions(sm_client):
    """Latest resolves to the highest ENABLED version, skipping disabled ones."""
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "lat1"}, json={})
    for i in range(3):
        sm_client.post(
            f"/v1/projects/{PROJECT}/secrets/lat1:addVersion",
            json={"payload": {"data": base64.b64encode(f"v{i + 1}".encode()).decode()}},
        )
    # Disable the newest (v3)
    sm_client.post(f"/v1/projects/{PROJECT}/secrets/lat1/versions/3:disable")

    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets/lat1/versions/latest:access")
    assert r.status_code == 200
    assert base64.b64decode(r.json()["payload"]["data"]).decode() == "v2"


def test_list_secrets_pagination(sm_client):
    for name in ("pa1", "pa2", "pa3", "pa4", "pa5"):
        sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": name}, json={})

    r1 = sm_client.get(f"/v1/projects/{PROJECT}/secrets?pageSize=2")
    assert r1.status_code == 200
    body1 = r1.json()
    assert len(body1["secrets"]) == 2
    assert "nextPageToken" in body1

    r2 = sm_client.get(
        f"/v1/projects/{PROJECT}/secrets?pageSize=2&pageToken={body1['nextPageToken']}"
    )
    body2 = r2.json()
    assert len(body2["secrets"]) == 2

    r3 = sm_client.get(
        f"/v1/projects/{PROJECT}/secrets?pageSize=2&pageToken={body2['nextPageToken']}"
    )
    body3 = r3.json()
    assert len(body3["secrets"]) == 1
    assert "nextPageToken" not in body3
