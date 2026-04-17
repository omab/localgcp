"""Firestore structured query evaluation."""
from __future__ import annotations

from typing import Any


def _extract_value(v: dict) -> Any:
    """Extract a Python value from a Firestore typed value dict."""
    if "nullValue" in v:
        return None
    if "booleanValue" in v:
        return v["booleanValue"]
    if "integerValue" in v:
        return int(v["integerValue"])
    if "doubleValue" in v:
        return float(v["doubleValue"])
    if "stringValue" in v:
        return v["stringValue"]
    if "timestampValue" in v:
        return v["timestampValue"]
    if "arrayValue" in v:
        return [_extract_value(item) for item in v["arrayValue"].get("values", [])]
    if "mapValue" in v:
        return {k: _extract_value(fv) for k, fv in v["mapValue"].get("fields", {}).items()}
    return None


def _get_field(doc_fields: dict, field_path: str) -> Any:
    """Navigate a dotted field path into a Firestore fields dict."""
    parts = field_path.split(".")
    current = doc_fields
    for part in parts:
        if not isinstance(current, dict):
            return None
        fv = current.get(part)
        if fv is None:
            return None
        if isinstance(fv, dict) and any(k in fv for k in (
            "stringValue", "integerValue", "booleanValue", "nullValue",
            "doubleValue", "mapValue", "arrayValue", "timestampValue",
        )):
            current = _extract_value(fv)
        else:
            current = fv
    return current


def _eval_filter(doc: dict, filter_node: dict) -> bool:
    """Evaluate a Firestore filter node against a document."""
    if "compositeFilter" in filter_node:
        cf = filter_node["compositeFilter"]
        op = cf.get("op", "AND")
        filters = cf.get("filters", [])
        if op == "AND":
            return all(_eval_filter(doc, f) for f in filters)
        if op == "OR":
            return any(_eval_filter(doc, f) for f in filters)
        return True

    if "fieldFilter" in filter_node:
        ff = filter_node["fieldFilter"]
        field_path = ff["field"]["fieldPath"]
        op = ff["op"]
        filter_value = _extract_value(ff["value"])
        doc_value = _get_field(doc.get("fields", {}), field_path)

        try:
            if op == "EQUAL":
                return doc_value == filter_value
            if op == "NOT_EQUAL":
                return doc_value != filter_value
            if op == "LESS_THAN":
                return doc_value < filter_value
            if op == "LESS_THAN_OR_EQUAL":
                return doc_value <= filter_value
            if op == "GREATER_THAN":
                return doc_value > filter_value
            if op == "GREATER_THAN_OR_EQUAL":
                return doc_value >= filter_value
            if op == "ARRAY_CONTAINS":
                return isinstance(doc_value, list) and filter_value in doc_value
            if op == "IN":
                return doc_value in (filter_value if isinstance(filter_value, list) else [])
            if op == "NOT_IN":
                return doc_value not in (filter_value if isinstance(filter_value, list) else [])
            if op == "ARRAY_CONTAINS_ANY":
                fv_list = filter_value if isinstance(filter_value, list) else []
                return isinstance(doc_value, list) and any(x in doc_value for x in fv_list)
        except TypeError:
            return False

    if "unaryFilter" in filter_node:
        uf = filter_node["unaryFilter"]
        field_path = uf["field"]["fieldPath"]
        op = uf["op"]
        doc_value = _get_field(doc.get("fields", {}), field_path)
        if op == "IS_NULL":
            return doc_value is None
        if op == "IS_NOT_NULL":
            return doc_value is not None
        if op == "IS_NAN":
            import math
            return isinstance(doc_value, float) and math.isnan(doc_value)
        if op == "IS_NOT_NAN":
            import math
            return not (isinstance(doc_value, float) and math.isnan(doc_value))

    return True


def _cursor_doc_value(doc: dict, order: dict) -> Any:
    """Return the Python value used for cursor comparison for one orderBy clause."""
    field_path = order["field"]["fieldPath"]
    if field_path == "__name__":
        return doc.get("name", "")
    return _get_field(doc.get("fields", {}), field_path)


def _compare_doc_to_cursor(doc: dict, order_by: list[dict], cursor_values: list) -> int:
    """Return -1, 0, or 1 if doc sorts before, equal to, or after the cursor position.

    Comparison respects each orderBy direction (ASCENDING / DESCENDING).
    Stops at the shortest of order_by / cursor_values.
    """
    for i, order in enumerate(order_by):
        if i >= len(cursor_values):
            break
        doc_val = _cursor_doc_value(doc, order)
        raw_cv = cursor_values[i]
        # cursor values arrive as Firestore Value dicts or plain Python values
        if isinstance(raw_cv, dict):
            if "referenceValue" in raw_cv:
                cursor_val = raw_cv["referenceValue"]
                # compare against doc name when sorting by __name__
                if order["field"]["fieldPath"] == "__name__":
                    doc_val = doc.get("name", "")
            else:
                cursor_val = _extract_value(raw_cv)
        else:
            cursor_val = raw_cv

        if doc_val == cursor_val:
            continue

        try:
            cmp = -1 if doc_val < cursor_val else 1
        except TypeError:
            cmp = -1 if doc_val is None else 1

        if order.get("direction", "ASCENDING") == "DESCENDING":
            cmp = -cmp

        return cmp

    return 0  # all compared fields equal → doc is AT the cursor position


def run_query(docs: list[dict], query: dict) -> list[dict]:
    """Apply a structuredQuery dict to a list of document dicts."""
    results = list(docs)

    # WHERE
    where = query.get("where")
    if where:
        results = [d for d in results if _eval_filter(d, where)]

    # ORDER BY
    order_by = query.get("orderBy", [])
    if order_by:
        # Multi-key sort: Python's sort is stable so we sort by each key in reverse priority
        for i in reversed(range(len(order_by))):
            field_path = order_by[i]["field"]["fieldPath"]
            desc = order_by[i].get("direction", "ASCENDING") == "DESCENDING"
            if field_path == "__name__":
                key_fn = lambda d, _d=desc: (d.get("name", "") is None, d.get("name", ""))
            else:
                key_fn = lambda d, fp=field_path: (
                    _get_field(d.get("fields", {}), fp) is None,
                    _get_field(d.get("fields", {}), fp),
                )
            try:
                results.sort(key=key_fn, reverse=desc)
            except TypeError:
                pass

    # CURSORS — applied after sorting, before offset/limit
    start_cursor = query.get("startAt")
    if start_cursor and order_by:
        cv = start_cursor.get("values", [])
        before = start_cursor.get("before", True)
        # before=True  → cursor is AT this position (inclusive start)
        # before=False → cursor is AFTER this position (exclusive start / startAfter)
        if cv:
            results = [
                d for d in results
                if _compare_doc_to_cursor(d, order_by, cv) > 0
                or (before and _compare_doc_to_cursor(d, order_by, cv) == 0)
            ]

    end_cursor = query.get("endAt")
    if end_cursor and order_by:
        cv = end_cursor.get("values", [])
        before = end_cursor.get("before", False)
        # before=True  → cursor is BEFORE this position (exclusive end / endBefore)
        # before=False → cursor is AT this position (inclusive end)
        if cv:
            results = [
                d for d in results
                if _compare_doc_to_cursor(d, order_by, cv) < 0
                or (not before and _compare_doc_to_cursor(d, order_by, cv) == 0)
            ]

    # OFFSET
    offset = query.get("offset", 0)
    if offset:
        results = results[offset:]

    # LIMIT
    limit = query.get("limit")
    if limit is not None:
        results = results[:limit]

    # SELECT (field projection) — applied last so filters/cursors see all fields
    select = query.get("select")
    if select:
        field_paths = [f["fieldPath"] for f in select.get("fields", [])]
        if field_paths and field_paths != ["__name__"]:
            projected = []
            for doc in results:
                fields = doc.get("fields", {})
                kept = {fp: _get_field_raw(fields, fp) for fp in field_paths if _get_field_raw(fields, fp) is not None}
                projected.append({**doc, "fields": kept})
            results = projected

    return results


def _get_field_raw(doc_fields: dict, field_path: str) -> dict | None:
    """Return the raw Firestore Value dict for a dotted field path, or None."""
    parts = field_path.split(".")
    current = doc_fields
    for i, part in enumerate(parts):
        if not isinstance(current, dict):
            return None
        fv = current.get(part)
        if fv is None:
            return None
        if i == len(parts) - 1:
            return fv
        # traverse into mapValue
        if isinstance(fv, dict) and "mapValue" in fv:
            current = fv["mapValue"].get("fields", {})
        else:
            return None
    return None
