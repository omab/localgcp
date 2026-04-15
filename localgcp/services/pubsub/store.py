"""Pub/Sub in-memory state.

Beyond the NamespacedStore (for topics/subscriptions metadata), we need
a per-subscription message queue with ack tracking.

Structure:
  topics        → topic_name → TopicModel dict
  subscriptions → sub_name   → SubscriptionModel dict

Message queues are kept in _queues: sub_name → deque of _Envelope.
Unacked messages are in _unacked: sub_name → {ack_id: _Envelope}.
"""
from __future__ import annotations

import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field

from localgcp.config import settings
from localgcp.core.store import NamespacedStore

_store = NamespacedStore("pubsub", settings.data_dir)


def _parse_duration(s: str) -> float:
    """Parse a GCP duration string like '10s' or '600s' into seconds."""
    s = s.strip()
    if s.endswith("s"):
        return float(s[:-1])
    return float(s)


@dataclass
class _Envelope:
    message: dict          # PubsubMessage dict
    ack_deadline: float = 0.0   # epoch seconds when ack expires
    delivery_attempt: int = 1
    not_before: float = 0.0     # monotonic time before which the message should not be delivered


_lock = threading.Lock()
_queues: dict[str, deque[_Envelope]] = {}    # sub → pending messages
_unacked: dict[str, dict[str, _Envelope]] = {}  # sub → ack_id → envelope
_inflight_keys: dict[str, set[str]] = {}     # sub → set of ordering keys currently unacked


def get_store() -> NamespacedStore:
    return _store


def ensure_queue(sub_name: str) -> None:
    with _lock:
        _queues.setdefault(sub_name, deque())
        _unacked.setdefault(sub_name, {})
        _inflight_keys.setdefault(sub_name, set())


def remove_queue(sub_name: str) -> None:
    with _lock:
        _queues.pop(sub_name, None)
        _unacked.pop(sub_name, None)
        _inflight_keys.pop(sub_name, None)


def enqueue(sub_name: str, message: dict) -> None:
    """Add a message to a subscription's queue."""
    with _lock:
        q = _queues.get(sub_name)
        if q is not None:
            q.append(_Envelope(message=message))


def pull(sub_name: str, max_messages: int) -> list[tuple[str, dict, int]]:
    """Pull up to max_messages from the queue.

    Returns list of (ack_id, message_dict, delivery_attempt).
    Moves messages to unacked map with a deadline.
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
            if dlp and dlp.get("deadLetterTopic") and env.delivery_attempt > dlp.get("maxDeliveryAttempts", 5):
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
    with _lock:
        unacked = _unacked.get(sub_name, {})
        new_deadline = time.monotonic() + deadline_secs
        for aid in ack_ids:
            if aid in unacked:
                unacked[aid].ack_deadline = new_deadline


def queue_depth(sub_name: str) -> int:
    with _lock:
        return len(_queues.get(sub_name, []))


def _route_to_dlq(dlq_topic: str, message: dict) -> None:
    """Enqueue a message to all subscriptions of a dead-letter topic (called under _lock)."""
    for sub in _store.list("subscriptions"):
        if sub.get("topic") == dlq_topic:
            q = _queues.get(sub["name"])
            if q is not None:
                q.append(_Envelope(message=message))
