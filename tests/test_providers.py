"""Tests for per-provider concurrency queuing (ConcurrencyGate provider methods)."""
from __future__ import annotations

import asyncio

import pytest

from proxen.core.gate import ConcurrencyGate, QueueOverflow, QueueTimeout


def _make_gate(max_inflight=2, max_waiting=10, timeout=2.0):
    gate = ConcurrencyGate(max_inflight=100, max_waiting=max_waiting, timeout=timeout)
    gate.set_provider_limit("p", max_inflight)
    return gate


# ─── Provider primitives ─────────────────────────────────────────────


def test_try_provider_respects_limit():
    gate = _make_gate(max_inflight=2)
    assert gate.try_provider("p") is True
    assert gate.try_provider("p") is True
    assert gate.try_provider("p") is False
    gate.release_provider("p")
    assert gate.try_provider("p") is True


def test_unlimited_provider_always_acquires():
    gate = ConcurrencyGate(max_inflight=100, max_waiting=0, timeout=1.0)
    gate.set_provider_limit("p", None)
    for _ in range(50):
        assert gate.try_provider("p") is True
    assert gate.provider_inflight()["p"] == 50


@pytest.mark.asyncio
async def test_release_promotes_waiter():
    gate = _make_gate(max_inflight=1, max_waiting=2)
    gate.try_provider("p")

    promoted = asyncio.Event()

    async def waiter():
        disc = asyncio.Event()
        name = await gate.wait_provider(["p"], disc)
        assert name == "p"
        promoted.set()
        gate.release_provider(name)

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)
    assert not promoted.is_set()
    gate.release_provider("p")
    await asyncio.wait_for(task, 2.0)
    assert promoted.is_set()


@pytest.mark.asyncio
async def test_cancel_waiter_pending_removes_from_queue():
    gate = _make_gate(max_inflight=1, max_waiting=2)
    gate.try_provider("p")
    disc = asyncio.Event()

    async def _wait():
        return await gate.wait_provider(["p"], disc)

    task = asyncio.create_task(_wait())
    await asyncio.sleep(0.05)
    assert gate.provider_status()["p"]["waiting"] == 1
    disc.set()
    result = await asyncio.wait_for(task, 2.0)
    assert result is None
    assert gate.provider_status()["p"]["waiting"] == 0
    gate.release_provider("p")


@pytest.mark.asyncio
async def test_cancel_waiter_promoted_releases_slot():
    """Cancelling a promoted waiter releases the slot back."""
    gate = _make_gate(max_inflight=1, max_waiting=2)
    gate.try_provider("p")
    disc = asyncio.Event()
    task = asyncio.create_task(gate.wait_provider(["p"], disc))
    await asyncio.sleep(0.05)
    gate.release_provider("p")  # promotes waiter
    name = await asyncio.wait_for(task, 2.0)
    assert name == "p"
    # now cancel: release the slot back
    gate.release_provider(name)
    assert gate.provider_inflight()["p"] == 0
    assert gate.try_provider("p") is True


@pytest.mark.asyncio
async def test_set_provider_limit_promotes_waiters():
    gate = _make_gate(max_inflight=1, max_waiting=5)
    gate.try_provider("p")

    disc = asyncio.Event()
    task = asyncio.create_task(gate.wait_provider(["p"], disc))
    await asyncio.sleep(0.05)
    # raise the limit → waiter should be promoted
    gate.set_provider_limit("p", 2)
    name = await asyncio.wait_for(task, 2.0)
    assert name == "p"
    assert gate.provider_inflight()["p"] == 2


def test_provider_status():
    gate = _make_gate(max_inflight=3, max_waiting=5)
    gate.try_provider("p")
    gate.try_provider("p")
    snap = gate.provider_status()["p"]
    assert snap["inflight"] == 2
    assert snap["waiting"] == 0
    assert snap["max_inflight"] == 3
    assert snap["max_waiting"] == 5


# ─── wait_provider (race) ────────────────────────────────────────────


def _make_mgr_gate(max_inflight=1, max_waiting=5, timeout=2.0):
    gate = ConcurrencyGate(max_inflight=100, max_waiting=max_waiting, timeout=timeout)
    gate.set_provider_limit("p1", max_inflight)
    gate.set_provider_limit("p2", max_inflight)
    return gate


@pytest.mark.asyncio
async def test_race_phase1_non_blocking():
    gate = _make_mgr_gate(max_inflight=2)
    disc = asyncio.Event()
    name = await gate.wait_provider(["p1", "p2"], disc)
    assert name in ("p1", "p2")
    gate.release_provider(name)


@pytest.mark.asyncio
async def test_race_phase2_waits_then_proceeds():
    gate = _make_mgr_gate(max_inflight=1, timeout=5.0)
    disc = asyncio.Event()
    gate.try_provider("p1")
    gate.try_provider("p2")

    acquired = asyncio.Event()

    async def waiter():
        name = await gate.wait_provider(["p1", "p2"], disc)
        assert name is not None
        acquired.set()
        gate.release_provider(name)

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)
    assert not acquired.is_set()

    gate.release_provider("p1")
    await asyncio.wait_for(task, 2.0)
    assert acquired.is_set()


@pytest.mark.asyncio
async def test_race_disconnect_returns_none():
    gate = _make_mgr_gate(max_inflight=1, timeout=10.0)
    disc = asyncio.Event()
    gate.try_provider("p1")

    async def _wait():
        return await gate.wait_provider(["p1"], disc)

    task = asyncio.create_task(_wait())
    await asyncio.sleep(0.05)
    disc.set()
    result = await asyncio.wait_for(task, 2.0)
    assert result is None
    assert gate.provider_inflight()["p1"] == 1
    gate.release_provider("p1")


@pytest.mark.asyncio
async def test_race_timeout_raises_queue_timeout():
    gate = _make_mgr_gate(max_inflight=1, timeout=0.05)
    disc = asyncio.Event()
    gate.try_provider("p1")

    with pytest.raises(QueueTimeout):
        await gate.wait_provider(["p1"], disc)
    gate.release_provider("p1")


@pytest.mark.asyncio
async def test_race_all_queues_full_raises_overflow():
    gate = _make_mgr_gate(max_inflight=1, max_waiting=1, timeout=10.0)
    disc = asyncio.Event()
    gate.try_provider("p1")
    # fill p1's wait queue
    fut = asyncio.get_running_loop().create_future()
    gate._provider_waiters["p1"].append(fut)

    with pytest.raises(QueueOverflow):
        await gate.wait_provider(["p1"], disc)
    gate.release_provider("p1")
