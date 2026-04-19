"""Pub/Sub in-memory state.

Beyond the NamespacedStore (for topics/subscriptions metadata), we need
a per-subscription message queue with ack tracking.

Structure:
  topics        → topic_name → TopicModel dict
  subscriptions → sub_name   → SubscriptionModel dict
  snapshots     → snap_name  → SnapshotModel dict (includes snapshotTime)

Message queues are kept in _queues: sub_name → deque of _Envelope.
Unacked messages are in _unacked: sub_name → {ack_id: _Envelope}.
Topic message log in _topic_log: topic_name → list of message dicts.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime

from cloudbox.config import settings
from cloudbox.core.store import NamespacedStore

_store = NamespacedStore("pubsub", settings.data_dir)


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _parse_duration(s: str) -> float:
    """Parse a GCP duration string into seconds.

    Args:
        s (str): Duration string such as '10s' or '600s'.

    Returns:
        float: Number of seconds represented by the duration string.
    """
    s = s.strip()
    if s.endswith("s"):
        return float(s[:-1])
    return float(s)


@dataclass
class _Envelope:
    message: dict  # PubsubMessage dict
    ack_deadline: float = 0.0  # epoch seconds when ack expires
    delivery_attempt: int = 1
    not_before: float = 0.0  # monotonic time before which the message should not be delivered


_lock = threading.RLock()
_queues: dict[str, deque[_Envelope]] = {}  # sub → pending messages
_unacked: dict[str, dict[str, _Envelope]] = {}  # sub → ack_id → envelope
_inflight_keys: dict[str, set[str]] = {}  # sub → set of ordering keys currently unacked
_topic_log: dict[str, list[dict]] = {}  # topic → retained messages (for seek)


def get_store() -> NamespacedStore:
    """Return the shared Pub/Sub store instance.

    Returns:
        The module-level ``NamespacedStore`` used by all Pub/Sub route handlers.
    """
    return _store


def ensure_queue(sub_name: str) -> None:
    """Initialize the in-memory queue structures for a subscription if not present.

    Args:
        sub_name: Full subscription resource name.
    """
    with _lock:
        _queues.setdefault(sub_name, deque())
        _unacked.setdefault(sub_name, {})
        _inflight_keys.setdefault(sub_name, set())


def remove_queue(sub_name: str) -> None:
    """Remove all in-memory queue state for a subscription.

    Args:
        sub_name: Full subscription resource name.
    """
    with _lock:
        _queues.pop(sub_name, None)
        _unacked.pop(sub_name, None)
        _inflight_keys.pop(sub_name, None)


def enqueue(sub_name: str, message: dict) -> None:
    """Add a message to a subscription's queue.

    Args:
        sub_name (str): Full subscription resource name.
        message (dict): PubsubMessage dict to enqueue.
    """
    with _lock:
        q = _queues.get(sub_name)
        if q is not None:
            q.append(_Envelope(message=message))


def pull(sub_name: str, max_messages: int) -> list[tuple[str, dict, int]]:
    """Pull up to max_messages from the queue.

    Moves messages to the unacked map with a deadline. Expired unacked messages
    are re-enqueued before pulling.

    Args:
        sub_name (str): Full subscription resource name.
        max_messages (int): Maximum number of messages to pull.

    Returns:
        list[tuple[str, dict, int]]: List of (ack_id, message_dict, delivery_attempt) tuples.
    """
    with _lock:
        q = _queues.get(sub_name, deque())
        unacked = _unacked.setdefault(sub_name, {})

        # Re-enqueue expired unacked messages first
        now = time.monotonic()
        expired = [aid for aid, env in unacked.items() if env.ack_deadline < now]
        sub_data = _store.get("subscriptions", sub_name)
        dlp = (sub_data or {}).get("deadLetterPolicy")
        rp = (sub_data or {}).get("retryPolicy")
        ordering = bool((sub_data or {}).get("enableMessageOrdering"))
        inflight = _inflight_keys.setdefault(sub_name, set())
        for aid in expired:
            env = unacked.pop(aid)
            env.delivery_attempt += 1
            key = env.message.get("orderingKey", "") if ordering else ""
            if key:
                inflight.discard(key)
            if (
                dlp
                and dlp.get("deadLetterTopic")
                and env.delivery_attempt > dlp.get("maxDeliveryAttempts", 5)
            ):
                _route_to_dlq(dlp["deadLetterTopic"], env.message)
            else:
                if rp:
                    min_backoff = _parse_duration(rp.get("minimumBackoff", "10s"))
                    max_backoff = _parse_duration(rp.get("maximumBackoff", "600s"))
                    backoff = min(min_backoff * (2 ** (env.delivery_attempt - 2)), max_backoff)
                    env.not_before = now + backoff
                q.appendleft(env)

        deadline_secs = sub_data["ackDeadlineSeconds"] if sub_data else 10

        results = []
        pending = list(q)
        q.clear()
        for env in pending:
            key = env.message.get("orderingKey", "") if ordering else ""
            if env.not_before > now or (key and key in inflight):
                q.append(env)  # not ready yet or blocked by in-flight key — keep in queue
            elif len(results) < max_messages:
                ack_id = str(uuid.uuid4())
                env.ack_deadline = now + deadline_secs
                unacked[ack_id] = env
                if key:
                    inflight.add(key)
                results.append((ack_id, env.message, env.delivery_attempt))
            else:
                q.append(env)

        return results


def acknowledge(sub_name: str, ack_ids: list[str]) -> None:
    """Acknowledge messages, removing them from the unacked map.

    Args:
        sub_name: Full subscription resource name.
        ack_ids: List of ack IDs to acknowledge.
    """
    with _lock:
        unacked = _unacked.get(sub_name, {})
        inflight = _inflight_keys.get(sub_name, set())
        sub_data = _store.get("subscriptions", sub_name)
        ordering = bool((sub_data or {}).get("enableMessageOrdering"))
        for aid in ack_ids:
            env = unacked.pop(aid, None)
            if env and ordering:
                key = env.message.get("orderingKey", "")
                if key:
                    inflight.discard(key)


def modify_ack_deadline(sub_name: str, ack_ids: list[str], deadline_secs: int) -> None:
    """Extend the ack deadline for a set of messages.

    Args:
        sub_name: Full subscription resource name.
        ack_ids: List of ack IDs whose deadlines should be extended.
        deadline_secs: New deadline in seconds from now.
    """
    with _lock:
        unacked = _unacked.get(sub_name, {})
        new_deadline = time.monotonic() + deadline_secs
        for aid in ack_ids:
            if aid in unacked:
                unacked[aid].ack_deadline = new_deadline


def queue_depth(sub_name: str) -> int:
    """Return the number of pending (not yet pulled) messages.

    Args:
        sub_name: Full subscription resource name.

    Returns:
        Count of messages waiting in the queue.
    """
    with _lock:
        return len(_queues.get(sub_name, []))


def unacked_count(sub_name: str) -> int:
    """Return the number of in-flight (pulled but not yet acked) messages.

    Args:
        sub_name: Full subscription resource name.

    Returns:
        Count of unacknowledged messages.
    """
    with _lock:
        return len(_unacked.get(sub_name, {}))


def retained_count(topic_name: str) -> int:
    """Return the number of messages retained in the topic log.

    Args:
        topic_name: Full topic resource name.

    Returns:
        Count of retained messages.
    """
    with _lock:
        return len(_topic_log.get(topic_name, []))


def _route_to_dlq(dlq_topic: str, message: dict) -> None:
    """Enqueue a message to all subscriptions of a dead-letter topic.

    Must be called with _lock held.

    Args:
        dlq_topic (str): Full topic resource name of the dead-letter topic.
        message (dict): PubsubMessage dict to route to the dead-letter topic.
    """
    for sub in _store.list("subscriptions"):
        if sub.get("topic") == dlq_topic:
            q = _queues.get(sub["name"])
            if q is not None:
                q.append(_Envelope(message=message))


# ---------------------------------------------------------------------------
# Topic message log — retention & seek
# ---------------------------------------------------------------------------

_DEFAULT_RETENTION_SECS = 604_800.0  # 7 days


def log_to_topic(topic: str, message: dict) -> None:
    """Append a message to the topic log and prune expired entries.

    The retention window is taken from the topic's messageRetentionDuration
    field (default 7 days).

    Args:
        topic (str): Full topic resource name.
        message (dict): PubsubMessage dict to append to the log.
    """
    topic_data = _store.get("topics", topic) or {}
    retention_str = topic_data.get("messageRetentionDuration", "604800s")
    try:
        retention_secs = _parse_duration(retention_str)
    except (ValueError, AttributeError):
        retention_secs = _DEFAULT_RETENTION_SECS

    with _lock:
        if topic not in _topic_log:
            _topic_log[topic] = []
        now = time.time()
        _topic_log[topic].append({**message, "_expires_at": now + retention_secs})
        _topic_log[topic] = [m for m in _topic_log[topic] if m.get("_expires_at", 0) > now]


def _log_since_locked(topic: str, since_iso: str) -> list[dict]:
    """Return messages from the topic log with publishTime at or after since_iso.

    Must be called with _lock held.

    Args:
        topic (str): Full topic resource name.
        since_iso (str): ISO 8601 timestamp; messages published at or after this time are returned.

    Returns:
        list[dict]: Matching message dicts with internal fields stripped.
    """
    try:
        since_dt = datetime.fromisoformat(since_iso.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return []
    result = []
    for m in _topic_log.get(topic, []):
        try:
            msg_dt = datetime.fromisoformat(m["publishTime"].replace("Z", "+00:00"))
            if msg_dt >= since_dt:
                result.append({k: v for k, v in m.items() if not k.startswith("_")})
        except (ValueError, KeyError):
            pass
    return result


def _oldest_publish_time(sub_name: str) -> str:
    """Return the publishTime of the oldest queued or unacked message, or now if empty.

    Must be called with _lock held.

    Args:
        sub_name (str): Full subscription resource name.

    Returns:
        str: ISO 8601 timestamp of the oldest message, or the current time if no messages exist.
    """
    all_msgs: list[dict] = []
    for env in _queues.get(sub_name, deque()):
        all_msgs.append(env.message)
    for env in _unacked.get(sub_name, {}).values():
        all_msgs.append(env.message)

    oldest: str | None = None
    for m in all_msgs:
        pt = m.get("publishTime", "")
        if pt and (oldest is None or pt < oldest):
            oldest = pt
    return oldest if oldest else _now_iso()


def seek_subscription(sub_name: str, topic: str, since_iso: str) -> None:
    """Reset a subscription's queue to replay messages published at or after a timestamp.

    Clears all in-flight (unacked) messages first, then re-enqueues messages
    from the topic log that pass the subscription's filter expression.

    Args:
        sub_name (str): Full subscription resource name.
        topic (str): Full topic resource name the subscription is attached to.
        since_iso (str): ISO 8601 timestamp; messages at or after this time are replayed.
    """
    from cloudbox.services.pubsub.filter import matches as filter_matches

    sub_data = _store.get("subscriptions", sub_name) or {}
    filter_expr = sub_data.get("filter", "")

    with _lock:
        messages = _log_since_locked(topic, since_iso)
        _queues[sub_name] = deque(
            _Envelope(message=m) for m in messages if filter_matches(filter_expr, m)
        )
        _unacked[sub_name] = {}
        _inflight_keys[sub_name] = set()


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------


def create_snapshot(snap_name: str, sub_name: str) -> dict | None:
    """Create a snapshot capturing the subscription's current backlog cursor.

    Args:
        snap_name (str): Full snapshot resource name to create.
        sub_name (str): Full subscription resource name to snapshot.

    Returns:
        dict | None: The snapshot dict, or None if the subscription does not exist.
    """
    sub_data = _store.get("subscriptions", sub_name)
    if sub_data is None:
        return None

    topic = sub_data["topic"]

    with _lock:
        snapshot_time = _oldest_publish_time(sub_name)

    # Snapshot expires in 7 days (GCS default)
    from datetime import timedelta

    expire_dt = datetime.now(UTC) + timedelta(days=7)
    expire_str = expire_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    snap = {
        "name": snap_name,
        "topic": topic,
        "expireTime": expire_str,
        "labels": {},
        "snapshotTime": snapshot_time,
    }
    _store.set("snapshots", snap_name, snap)
    return snap
