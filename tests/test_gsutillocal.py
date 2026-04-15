"""Tests for the gsutillocal CLI."""
import argparse
import io
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from localgcp.services.gcs.app import app as gcs_app

# ---------------------------------------------------------------------------
# Adapter: makes TestClient look like an httpx.Client context manager so the
# gsutillocal command functions can be tested without a running server.
# ---------------------------------------------------------------------------


class _GCSAdapter:
    """Wraps a Starlette TestClient to match the httpx.Client interface used
    by gsutillocal (context manager + .get/.post/.delete with httpx kwarg names).
    """

    def __init__(self):
        self._tc = TestClient(gcs_app)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def get(self, url, **kw):
        return self._tc.get(url, **kw)

    def delete(self, url, **kw):
        return self._tc.delete(url, **kw)

    def post(self, url, **kw):
        # httpx uses 'content' for raw bytes; requests/TestClient uses 'content' too
        # (requests accepts it as an alias for 'data' via httpx's compatibility layer)
        return self._tc.post(url, **kw)


@pytest.fixture
def gcs_adapter():
    return _GCSAdapter()


@pytest.fixture(autouse=True)
def patch_client(gcs_adapter):
    """Replace gsutillocal._client() with our TestClient adapter."""
    with patch("localgcp.gsutillocal._client", return_value=gcs_adapter):
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _args(**kw) -> argparse.Namespace:
    return argparse.Namespace(**kw)


def _make_bucket(adapter, name: str) -> None:
    adapter._tc.post(
        "/storage/v1/b",
        params={"project": "local-project"},
        json={"name": name},
    )


def _upload(adapter, bucket: str, name: str, body: bytes = b"hello") -> None:
    adapter._tc.post(
        f"/upload/storage/v1/b/{bucket}/o",
        params={"uploadType": "media", "name": name},
        content=body,
        headers={"Content-Type": "text/plain"},
    )


# ---------------------------------------------------------------------------
# ls
# ---------------------------------------------------------------------------


def test_ls_no_args_lists_buckets(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_ls

    _make_bucket(gcs_adapter, "alpha")
    _make_bucket(gcs_adapter, "beta")

    cmd_ls(_args(uri=None, long=False, recursive=False))

    out = capsys.readouterr().out
    assert "gs://alpha/" in out
    assert "gs://beta/" in out


def test_ls_bucket_lists_objects(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_ls

    _make_bucket(gcs_adapter, "my-bucket")
    _upload(gcs_adapter, "my-bucket", "file.txt")

    cmd_ls(_args(uri="gs://my-bucket", long=False, recursive=False))

    out = capsys.readouterr().out
    assert "gs://my-bucket/file.txt" in out


def test_ls_long_shows_size(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_ls

    _make_bucket(gcs_adapter, "my-bucket")
    _upload(gcs_adapter, "my-bucket", "data.bin", body=b"x" * 512)

    cmd_ls(_args(uri="gs://my-bucket", long=True, recursive=False))

    out = capsys.readouterr().out
    assert "512" in out
    assert "gs://my-bucket/data.bin" in out


# ---------------------------------------------------------------------------
# mb / rb
# ---------------------------------------------------------------------------


def test_mb_creates_bucket(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_mb

    cmd_mb(_args(bucket="gs://new-bucket", location=""))

    out = capsys.readouterr().out
    assert "Creating gs://new-bucket/" in out

    # Bucket exists
    r = gcs_adapter._tc.get("/storage/v1/b/new-bucket")
    assert r.status_code == 200


def test_mb_with_location(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_mb

    cmd_mb(_args(bucket="gs://regional-bucket", location="us-east1"))
    capsys.readouterr()

    r = gcs_adapter._tc.get("/storage/v1/b/regional-bucket")
    assert r.json()["location"] == "US-EAST1"


def test_rb_removes_bucket(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_rb

    _make_bucket(gcs_adapter, "to-delete")
    cmd_rb(_args(bucket="gs://to-delete"))

    out = capsys.readouterr().out
    assert "Removing gs://to-delete/" in out

    r = gcs_adapter._tc.get("/storage/v1/b/to-delete")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# cp
# ---------------------------------------------------------------------------


def test_cp_upload(gcs_adapter, tmp_path, capsys):
    from localgcp.gsutillocal import cmd_cp

    _make_bucket(gcs_adapter, "uploads")
    src = tmp_path / "hello.txt"
    src.write_bytes(b"hello world")

    cmd_cp(_args(src=str(src), dst="gs://uploads/hello.txt", recursive=False))
    capsys.readouterr()

    r = gcs_adapter._tc.get(
        "/storage/v1/b/uploads/o/hello.txt", params={"alt": "media"}
    )
    assert r.content == b"hello world"


def test_cp_download(gcs_adapter, tmp_path, capsys):
    from localgcp.gsutillocal import cmd_cp

    _make_bucket(gcs_adapter, "dl-bucket")
    _upload(gcs_adapter, "dl-bucket", "notes.txt", body=b"downloaded")

    dst = tmp_path / "notes.txt"
    cmd_cp(_args(src="gs://dl-bucket/notes.txt", dst=str(dst), recursive=False))
    capsys.readouterr()

    assert dst.read_bytes() == b"downloaded"


def test_cp_gcs_to_gcs(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_cp

    _make_bucket(gcs_adapter, "src-bucket")
    _make_bucket(gcs_adapter, "dst-bucket")
    _upload(gcs_adapter, "src-bucket", "obj.txt", body=b"gcs2gcs")

    cmd_cp(_args(src="gs://src-bucket/obj.txt", dst="gs://dst-bucket/obj.txt", recursive=False))
    capsys.readouterr()

    r = gcs_adapter._tc.get(
        "/storage/v1/b/dst-bucket/o/obj.txt", params={"alt": "media"}
    )
    assert r.content == b"gcs2gcs"


def test_cp_upload_recursive(gcs_adapter, tmp_path, capsys):
    from localgcp.gsutillocal import cmd_cp

    _make_bucket(gcs_adapter, "rcp-bucket")
    d = tmp_path / "mydir"
    d.mkdir()
    (d / "a.txt").write_bytes(b"aaa")
    (d / "b.txt").write_bytes(b"bbb")

    cmd_cp(_args(src=str(d), dst="gs://rcp-bucket/", recursive=True))
    capsys.readouterr()

    r = gcs_adapter._tc.get("/storage/v1/b/rcp-bucket/o")
    names = {o["name"] for o in r.json().get("items", [])}
    assert any(n.endswith("a.txt") for n in names)
    assert any(n.endswith("b.txt") for n in names)


# ---------------------------------------------------------------------------
# mv
# ---------------------------------------------------------------------------


def test_mv_renames_object(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_mv

    _make_bucket(gcs_adapter, "mv-bucket")
    _upload(gcs_adapter, "mv-bucket", "old.txt", body=b"move me")

    cmd_mv(_args(src="gs://mv-bucket/old.txt", dst="gs://mv-bucket/new.txt"))
    capsys.readouterr()

    r_new = gcs_adapter._tc.get(
        "/storage/v1/b/mv-bucket/o/new.txt", params={"alt": "media"}
    )
    assert r_new.content == b"move me"

    r_old = gcs_adapter._tc.get("/storage/v1/b/mv-bucket/o/old.txt")
    assert r_old.status_code == 404


# ---------------------------------------------------------------------------
# rm
# ---------------------------------------------------------------------------


def test_rm_single_object(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_rm

    _make_bucket(gcs_adapter, "rm-bucket")
    _upload(gcs_adapter, "rm-bucket", "bye.txt")

    cmd_rm(_args(uris=["gs://rm-bucket/bye.txt"], recursive=False))
    out = capsys.readouterr().out
    assert "Removing gs://rm-bucket/bye.txt" in out

    r = gcs_adapter._tc.get("/storage/v1/b/rm-bucket/o/bye.txt")
    assert r.status_code == 404


def test_rm_wildcard(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_rm

    _make_bucket(gcs_adapter, "wc-bucket")
    _upload(gcs_adapter, "wc-bucket", "logs/a.log")
    _upload(gcs_adapter, "wc-bucket", "logs/b.log")
    _upload(gcs_adapter, "wc-bucket", "other.txt")

    cmd_rm(_args(uris=["gs://wc-bucket/logs/*"], recursive=False))
    capsys.readouterr()

    r = gcs_adapter._tc.get("/storage/v1/b/wc-bucket/o")
    items = r.json().get("items", [])
    assert all(o["name"] == "other.txt" for o in items)


def test_rm_recursive_bucket(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_rm

    _make_bucket(gcs_adapter, "nuke-me")
    _upload(gcs_adapter, "nuke-me", "x.txt")
    _upload(gcs_adapter, "nuke-me", "y.txt")

    cmd_rm(_args(uris=["gs://nuke-me"], recursive=True))
    capsys.readouterr()

    r = gcs_adapter._tc.get("/storage/v1/b/nuke-me")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# cat
# ---------------------------------------------------------------------------


def test_cat_writes_to_stdout(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_cat

    _make_bucket(gcs_adapter, "cat-bucket")
    _upload(gcs_adapter, "cat-bucket", "greet.txt", body=b"hello cat")

    # Redirect stdout.buffer
    buf = io.BytesIO()
    with patch("sys.stdout", new=type("FakeStdout", (), {"buffer": buf})()):
        cmd_cat(_args(uri="gs://cat-bucket/greet.txt"))

    assert buf.getvalue() == b"hello cat"


# ---------------------------------------------------------------------------
# stat
# ---------------------------------------------------------------------------


def test_stat_shows_metadata(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_stat

    _make_bucket(gcs_adapter, "stat-bucket")
    _upload(gcs_adapter, "stat-bucket", "meta.txt", body=b"data")

    cmd_stat(_args(uri="gs://stat-bucket/meta.txt"))

    out = capsys.readouterr().out
    assert "gs://stat-bucket/meta.txt" in out
    assert "Content-Length" in out
    assert "Content-Type" in out


# ---------------------------------------------------------------------------
# du
# ---------------------------------------------------------------------------


def test_du_shows_size(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_du

    _make_bucket(gcs_adapter, "du-bucket")
    _upload(gcs_adapter, "du-bucket", "big.bin", body=b"x" * 1000)

    cmd_du(_args(uri="gs://du-bucket"))

    out = capsys.readouterr().out
    assert "1000" in out
    assert "gs://du-bucket" in out


def test_du_no_args_all_buckets(gcs_adapter, capsys):
    from localgcp.gsutillocal import cmd_du

    _make_bucket(gcs_adapter, "alpha")
    _make_bucket(gcs_adapter, "beta")
    _upload(gcs_adapter, "alpha", "f.txt", body=b"123")

    cmd_du(_args(uri=None))

    out = capsys.readouterr().out
    assert "gs://alpha" in out
    assert "gs://beta" in out
