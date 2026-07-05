from __future__ import annotations

import asyncio

import pytest
from unittest.mock import MagicMock

from proxen.core.config import Settings
from proxen.core.models import RequestRecord
from proxen.services.telemetry import TelemetryWriter
from proxen.services.upstream import UpstreamManager


def _mgr(limits: dict) -> UpstreamManager:
    """A minimal UpstreamManager with only provider-limit state populated."""
    mgr = UpstreamManager(Settings(), MagicMock(), MagicMock())
    mgr._provider_limits = limits
    return mgr


# ─── Provider concurrency slots (release correctness) ────────────


def test_provider_slot_acquire_release_cycle():
    mgr = _mgr({"p": 2})
    assert mgr.acquire_provider("p") is True
    assert mgr.acquire_provider("p") is True
    assert mgr.acquire_provider("p") is False  # limit reached
    mgr.release_provider("p")
    assert mgr.acquire_provider("p") is True
    # release below zero is guarded
    mgr.release_provider("p")
    mgr.release_provider("p")
    mgr.release_provider("p")
    assert mgr.provider_inflight()["p"] == 0


def test_provider_slot_no_limit_means_unbounded():
    mgr = _mgr({"p": None})
    for _ in range(50):
        assert mgr.acquire_provider("p") is True
    assert mgr.provider_inflight()["p"] == 50


# ─── release_provider bare decrement ───────────────────────────────


def test_release_provider_bare_decrement():
    """release_provider is a bare decrement guarded against underflow."""
    mgr = _mgr({"p": 2})

    mgr.acquire_provider("p")
    mgr.acquire_provider("p")
    assert mgr.provider_inflight()["p"] == 2

    mgr.release_provider("p")
    assert mgr.provider_inflight()["p"] == 1
    mgr.release_provider("p")
    assert mgr.provider_inflight()["p"] == 0


# ─── Telemetry queue drop policy ───────────────────────────────


def _rec() -> RequestRecord:
    return RequestRecord(
        timestamp=0.0, model="m", upstream="u", key_id="k"
    )


@pytest.mark.asyncio
async def test_telemetry_queue_drops_when_full():
    writer = TelemetryWriter(db=None, max_queue=4)
    for _ in range(4):
        writer.enqueue(_rec())
    assert writer.dropped == 0
    # Beyond the cap: dropped counter increments, no exception raised.
    writer.enqueue(_rec())
    writer.enqueue(_rec())
    assert writer.dropped == 2
    # Drain one and confirm we can enqueue again.
    await writer._queue.get()
    writer.enqueue(_rec())
    assert writer.dropped == 2  # this one fit
