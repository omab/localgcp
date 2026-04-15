"""gsutil-compatible CLI for LocalGCP Cloud Storage emulator.

Mirrors the real gsutil command syntax so existing scripts can target
LocalGCP with minimal changes.  Point at a running LocalGCP instance via
environment variables:

    LOCALGCP_HOST      (default: localhost)
    LOCALGCP_GCS_PORT  (default: 4443)
    LOCALGCP_PROJECT   (default: local-project)

Usage:
    gsutillocal ls [gs://bucket[/prefix]]  [-l] [-r]
    gsutillocal cp [-r] <src> <dst>
    gsutillocal mv <src> <dst>
    gsutillocal mb [-l LOCATION] gs://bucket
    gsutillocal rb gs://bucket
    gsutillocal rm [-r] gs://bucket/object [...]
    gsutillocal cat gs://bucket/object
    gsutillocal stat gs://bucket/object
    gsutillocal du [gs://bucket[/prefix]]
"""
from __future__ import annotations

import argparse
import mimetypes
import os
import sys
from pathlib import Path

import httpx

_HOST = os.environ.get("LOCALGCP_HOST", "localhost")
_GCS_PORT = int(os.environ.get("LOCALGCP_GCS_PORT", "4443"))
_BASE = f"http://{_HOST}:{_GCS_PORT}"
_PROJECT = os.environ.get("LOCALGCP_PROJECT", "local-project")


def _client() -> httpx.Client:
    return httpx.Client(base_url=_BASE, timeout=60.0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _check(resp) -> dict:
    if resp.status_code >= 400:
        try:
            msg = resp.json().get("error", {}).get("message") or resp.text
        except Exception:
            msg = resp.text
        print(f"CommandException: {msg}", file=sys.stderr)
        sys.exit(1)
    if resp.status_code == 204 or not resp.content:
        return {}
    return resp.json()


def _parse_gs_uri(uri: str) -> tuple[str, str]:
    """Return (bucket, object_path). object_path may be empty."""
    if not uri.startswith("gs://"):
        raise ValueError(f"Not a GCS URI: {uri}")
    rest = uri[5:]
    if "/" in rest:
        bucket, obj = rest.split("/", 1)
    else:
        bucket, obj = rest, ""
    return bucket, obj


def _is_gs(uri: str) -> bool:
    return uri.startswith("gs://")


def _human_size(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 ** 2:
        return f"{n / 1024:.1f} KiB"
    if n < 1024 ** 3:
        return f"{n / 1024 ** 2:.1f} MiB"
    return f"{n / 1024 ** 3:.1f} GiB"


def _upload_file(c, bucket: str, obj_name: str, path: Path) -> None:
    content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    data = path.read_bytes()
    _check(c.post(
        f"/upload/storage/v1/b/{bucket}/o",
        params={"uploadType": "media", "name": obj_name},
        content=data,
        headers={"Content-Type": content_type},
    ))


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_ls(args) -> None:
    with _client() as c:
        if not args.uri:
            data = _check(c.get("/storage/v1/b", params={"project": _PROJECT}))
            for b in data.get("items", []):
                print(f"gs://{b['name']}/")
            return

        bucket, prefix = _parse_gs_uri(args.uri)
        params: dict = {}
        if prefix:
            params["prefix"] = prefix
        if not args.recursive:
            params["delimiter"] = "/"

        data = _check(c.get(f"/storage/v1/b/{bucket}/o", params=params))

        for cp in data.get("prefixes", []):
            print(f"gs://{bucket}/{cp}")

        for obj in data.get("items", []):
            name = obj["name"]
            if args.long:
                size = int(obj.get("size", 0))
                updated = (obj.get("updated") or "")[:19].replace("T", " ")
                print(f"{size:>13}  {updated}  gs://{bucket}/{name}")
            else:
                print(f"gs://{bucket}/{name}")


def cmd_cp(args) -> None:
    src, dst = args.src, args.dst

    with _client() as c:
        if _is_gs(src) and _is_gs(dst):
            sb, so = _parse_gs_uri(src)
            db, do_ = _parse_gs_uri(dst)
            if not so:
                print("CommandException: source must include an object path", file=sys.stderr)
                sys.exit(1)
            if not do_:
                do_ = so.split("/")[-1]
            elif do_.endswith("/"):
                do_ = do_ + so.split("/")[-1]
            _check(c.post(f"/storage/v1/b/{sb}/o/{so}/copyTo/b/{db}/o/{do_}"))
            print(f"Copying gs://{sb}/{so} [Content-Type=application/octet-stream]...")
            print("  / [1 files]")

        elif _is_gs(src):
            sb, so = _parse_gs_uri(src)
            if not so:
                print("CommandException: source must include an object path", file=sys.stderr)
                sys.exit(1)
            resp = c.get(f"/storage/v1/b/{sb}/o/{so}", params={"alt": "media"})
            if resp.status_code >= 400:
                _check(resp)
            if dst == "-":
                sys.stdout.buffer.write(resp.content)
            else:
                dst_path = Path(dst)
                if dst_path.is_dir():
                    dst_path = dst_path / so.split("/")[-1]
                dst_path.write_bytes(resp.content)
                size = len(resp.content)
                ct = resp.headers.get("content-type", "application/octet-stream")
                print(f"Copying gs://{sb}/{so} [Content-Type={ct}]...")
                print(f"  / [1 files][{_human_size(size)}/{_human_size(size)}]")

        elif _is_gs(dst):
            db, do_ = _parse_gs_uri(dst)
            src_path = Path(src)
            if args.recursive and src_path.is_dir():
                files = sorted(f for f in src_path.rglob("*") if f.is_file())
                for f in files:
                    rel = f.relative_to(src_path.parent)
                    obj_name = (do_.rstrip("/") + "/" + str(rel)).lstrip("/") if do_ else str(rel)
                    _upload_file(c, db, obj_name, f)
                    print(f"Copying file://{f.resolve()} [Content-Type={mimetypes.guess_type(f.name)[0] or 'application/octet-stream'}]...")
                print(f"  / [{len(files)} files]")
            else:
                if not src_path.exists():
                    print(f"CommandException: {src}: No such file or directory", file=sys.stderr)
                    sys.exit(1)
                obj_name = do_ if do_ and not do_.endswith("/") else (do_ or "") + src_path.name
                _upload_file(c, db, obj_name, src_path)
                size = src_path.stat().st_size
                ct = mimetypes.guess_type(src_path.name)[0] or "application/octet-stream"
                print(f"Copying file://{src_path.resolve()} [Content-Type={ct}]...")
                print(f"  / [1 files][{_human_size(size)}/{_human_size(size)}]")
        else:
            print("CommandException: at least one of src/dst must be a gs:// URI", file=sys.stderr)
            sys.exit(1)


def cmd_mv(args) -> None:
    with _client() as c:
        # Copy
        sb, so = _parse_gs_uri(args.src)
        db, do_ = _parse_gs_uri(args.dst)
        if not do_:
            do_ = so.split("/")[-1]
        elif do_.endswith("/"):
            do_ = do_ + so.split("/")[-1]
        _check(c.post(f"/storage/v1/b/{sb}/o/{so}/copyTo/b/{db}/o/{do_}"))
        # Delete source
        _check(c.delete(f"/storage/v1/b/{sb}/o/{so}"))
        print(f"Moving gs://{sb}/{so} to gs://{db}/{do_}...")


def cmd_mb(args) -> None:
    bucket = args.bucket.removeprefix("gs://").rstrip("/")
    with _client() as c:
        body: dict = {"name": bucket}
        if args.location:
            body["location"] = args.location.upper()
        _check(c.post("/storage/v1/b", params={"project": _PROJECT}, json=body))
    print(f"Creating gs://{bucket}/...")


def cmd_rb(args) -> None:
    bucket = args.bucket.removeprefix("gs://").rstrip("/")
    with _client() as c:
        _check(c.delete(f"/storage/v1/b/{bucket}"))
    print(f"Removing gs://{bucket}/...")


def cmd_rm(args) -> None:
    with _client() as c:
        for uri in args.uris:
            bucket, obj = _parse_gs_uri(uri)
            if args.recursive and not obj:
                data = _check(c.get(f"/storage/v1/b/{bucket}/o"))
                for item in data.get("items", []):
                    _check(c.delete(f"/storage/v1/b/{bucket}/o/{item['name']}"))
                    print(f"Removing gs://{bucket}/{item['name']}...")
                _check(c.delete(f"/storage/v1/b/{bucket}"))
                print(f"Removing gs://{bucket}/...")
            elif "*" in obj:
                prefix = obj[: obj.index("*")]
                data = _check(c.get(f"/storage/v1/b/{bucket}/o", params={"prefix": prefix}))
                for item in data.get("items", []):
                    _check(c.delete(f"/storage/v1/b/{bucket}/o/{item['name']}"))
                    print(f"Removing gs://{bucket}/{item['name']}...")
            else:
                _check(c.delete(f"/storage/v1/b/{bucket}/o/{obj}"))
                print(f"Removing gs://{bucket}/{obj}...")


def cmd_cat(args) -> None:
    bucket, obj = _parse_gs_uri(args.uri)
    if not obj:
        print("CommandException: must specify an object path", file=sys.stderr)
        sys.exit(1)
    with _client() as c:
        resp = c.get(f"/storage/v1/b/{bucket}/o/{obj}", params={"alt": "media"})
        if resp.status_code >= 400:
            _check(resp)
        sys.stdout.buffer.write(resp.content)


def cmd_stat(args) -> None:
    bucket, obj = _parse_gs_uri(args.uri)
    with _client() as c:
        data = _check(c.get(f"/storage/v1/b/{bucket}/o/{obj}"))
    name = data.get("name", obj)
    size = int(data.get("size", 0))
    print(f"gs://{bucket}/{name}:")
    print(f"        Creation time:          {data.get('timeCreated', 'N/A')}")
    print(f"        Update time:            {data.get('updated', 'N/A')}")
    print(f"        Storage class:          {data.get('storageClass', 'STANDARD')}")
    print(f"        Content-Length:         {size}")
    print(f"        Content-Type:           {data.get('contentType', 'N/A')}")
    print(f"        Hash (crc32c):          {data.get('crc32c', 'N/A')}")
    print(f"        ETag:                   {data.get('etag', 'N/A')}")


def cmd_du(args) -> None:
    with _client() as c:
        if not args.uri:
            bdata = _check(c.get("/storage/v1/b", params={"project": _PROJECT}))
            for b in bdata.get("items", []):
                _du_bucket(c, b["name"], "")
        else:
            bucket, prefix = _parse_gs_uri(args.uri)
            _du_bucket(c, bucket, prefix)


def _du_bucket(c, bucket: str, prefix: str) -> None:
    params: dict = {}
    if prefix:
        params["prefix"] = prefix
    data = _check(c.get(f"/storage/v1/b/{bucket}/o", params=params))
    total = sum(int(o.get("size", 0)) for o in data.get("items", []))
    label = f"gs://{bucket}/{prefix}" if prefix else f"gs://{bucket}"
    print(f"{total:>13}  {label}")


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="gsutillocal",
        description="gsutil-compatible CLI for LocalGCP Cloud Storage emulator",
    )
    # Accept (and ignore) global flags gsutil users commonly pass
    p.add_argument("-o", metavar="OPTION", action="append", default=[],
                   help="Set a gsutil/boto option (accepted but ignored)")
    p.add_argument("-m", dest="parallel", action="store_true",
                   help="Parallel operations (accepted but ignored)")

    sub = p.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # ls
    p_ls = sub.add_parser("ls", help="List buckets or objects")
    p_ls.add_argument("uri", nargs="?", metavar="gs://bucket[/prefix]")
    p_ls.add_argument("-l", dest="long", action="store_true", help="Long listing")
    p_ls.add_argument("-r", dest="recursive", action="store_true", help="Recursive")

    # cp
    p_cp = sub.add_parser("cp", help="Copy files (upload / download / GCS-to-GCS)")
    p_cp.add_argument("src", metavar="SRC")
    p_cp.add_argument("dst", metavar="DST")
    p_cp.add_argument("-r", "-R", dest="recursive", action="store_true", help="Recursive")

    # mv
    p_mv = sub.add_parser("mv", help="Move / rename objects")
    p_mv.add_argument("src", metavar="gs://bucket/src")
    p_mv.add_argument("dst", metavar="gs://bucket/dst")

    # mb
    p_mb = sub.add_parser("mb", help="Make bucket")
    p_mb.add_argument("bucket", metavar="gs://bucket")
    p_mb.add_argument("-l", dest="location", metavar="LOCATION", default="",
                      help="Bucket location (e.g. US-CENTRAL1)")

    # rb
    p_rb = sub.add_parser("rb", help="Remove bucket")
    p_rb.add_argument("bucket", metavar="gs://bucket")

    # rm
    p_rm = sub.add_parser("rm", help="Remove objects")
    p_rm.add_argument("uris", metavar="gs://bucket/object", nargs="+")
    p_rm.add_argument("-r", "-R", dest="recursive", action="store_true", help="Recursive")

    # cat
    p_cat = sub.add_parser("cat", help="Output object content to stdout")
    p_cat.add_argument("uri", metavar="gs://bucket/object")

    # stat
    p_stat = sub.add_parser("stat", help="Display object status / metadata")
    p_stat.add_argument("uri", metavar="gs://bucket/object")

    # du
    p_du = sub.add_parser("du", help="Display object size usage")
    p_du.add_argument("uri", nargs="?", metavar="gs://bucket[/prefix]")

    return p


_COMMANDS = {
    "ls":   cmd_ls,
    "cp":   cmd_cp,
    "mv":   cmd_mv,
    "mb":   cmd_mb,
    "rb":   cmd_rb,
    "rm":   cmd_rm,
    "cat":  cmd_cat,
    "stat": cmd_stat,
    "du":   cmd_du,
}


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    _COMMANDS[args.command](args)


if __name__ == "__main__":
    main()
