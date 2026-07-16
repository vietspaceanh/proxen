from __future__ import annotations

import asyncio
import time

import pytest

from proxen.core.concurrency import (
    ConcurrencyGate,
    KeyLimits,
    QueueOverflow,
    QueueTimeout,
    RateLimitExceeded,
)


@pytest.mark.asyncio
async def test_acquire_release_basic():
    gate = ConcurrencyGate(max_inflight=2, max_waiting=2, timeout=1.0)
    s1 = await gate.acquire()
    s2 = await gate.acquire()
    assert gate.snapshot().active == 2
    gate.release(s1)
    gate.release(s2)
    assert gate.snapshot().active == 0


@pytest.mark.asyncio
async def test_queue_overflow():
    gate = ConcurrencyGate(max_inflight=1, max_waiting=1, timeout=2.0)
    held = await gate.acquire()
    # First waiter goes into the queue.
    wait_task = asyncio.create_task(gate.acquire())
    await asyncio.sleep(0.05)
    assert gate.snapshot().waiting == 1
    # Second waiter exceeds max_waiting.
    with pytest.raises(QueueOverflow):
        await gate.acquire()
    wait_task.cancel()
    gate.release(held)


@pytest.mark.asyncio
async def test_queue_timeout():
    gate = ConcurrencyGate(max_inflight=1, max_waiting=2, timeout=0.1)
    held = await gate.acquire()
    with pytest.raises(QueueTimeout):
        await gate.acquire()
    gate.release(held)


@pytest.mark.asyncio
async def test_queue_timeout_disabled():
    """timeout=0 disables the queue timeout: a waiter stays queued and is
    promoted only when a slot frees, rather than timing out."""
    gate = ConcurrencyGate(max_inflight=1, max_waiting=2, timeout=0)
    held = await gate.acquire()
    acquired = asyncio.Event()

    async def waiter():
        slot = await gate.acquire("k1")
        acquired.set()
        gate.release(slot)

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)
    # Still queued - has not timed out, has not been promoted.
    assert not acquired.is_set()
    assert gate.snapshot().waiting == 1
    # Releasing the held slot promotes the waiter.
    gate.release(held)
    await asyncio.wait_for(task, 2.0)
    assert acquired.is_set()


@pytest.mark.asyncio
async def test_release_wakes_waiter():
    gate = ConcurrencyGate(max_inflight=1, max_waiting=2, timeout=5.0)
    held = await gate.acquire()
    started = asyncio.Event()

    async def waiter():
        slot = await gate.acquire()
        started.set()
        gate.release(slot)

    t = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)
    assert not started.is_set()
    gate.release(held)
    await asyncio.wait_for(t, timeout=2.0)
    assert started.is_set()


@pytest.mark.asyncio
async def test_idempotent_release_does_not_double_count():
    """Releasing the same slot twice must be a no-op."""
    gate = ConcurrencyGate(max_inflight=1, max_waiting=0, timeout=1.0)
    slot = await gate.acquire()
    gate.release(slot)
    gate.release(slot)  # second release must not underflow _active
    assert gate.snapshot().active == 0
    # And we can acquire again (counter wasn't driven negative).
    slot2 = await gate.acquire()
    assert gate.snapshot().active == 1
    gate.release(slot2)


@pytest.mark.asyncio
async def test_idempotent_release_does_not_double_count_key_limits():
    """Double-release must not double-decrement key_inflight or double-record tokens."""
    gate = ConcurrencyGate(max_inflight=10, max_waiting=0, timeout=1.0)
    gate.set_key_limits("k1", KeyLimits(
        max_inflight=1,
        max_tokens=1000,
        max_tokens_window_s=60.0,
    ))
    slot = await gate.acquire("k1")
    slot.input_tokens = 50
    slot.output_tokens = 0
    gate.release(slot)
    # Second release must be a no-op for key limiter too.
    gate.release(slot)
    # key_inflight entry is pruned at zero — double-release must not drive it negative.
    assert gate._keys.key_inflight.get("k1", 0) == 0
    # Token window should only record 50, not 100.
    tok_window = gate._keys.tok_windows["k1"]
    assert tok_window.total == 50


@pytest.mark.asyncio
async def test_per_key_concurrency_limit():
    gate = ConcurrencyGate(max_inflight=10, max_waiting=10, timeout=1.0)
    gate.set_key_limits("k1", KeyLimits(max_inflight=1))
    s = await gate.acquire("k1")
    with pytest.raises(RateLimitExceeded) as exc:
        await gate.acquire("k1")
    assert exc.value.limit_type == "concurrency"
    gate.release(s)


@pytest.mark.asyncio
async def test_per_key_requests_limit():
    gate = ConcurrencyGate(max_inflight=10, max_waiting=10, timeout=1.0)
    gate.set_key_limits("k1", KeyLimits(max_requests=2, max_requests_window_s=60.0))
    s1 = await gate.acquire("k1")
    gate.release(s1)
    s2 = await gate.acquire("k1")
    gate.release(s2)
    with pytest.raises(RateLimitExceeded) as exc:
        await gate.acquire("k1")
    assert exc.value.limit_type == "requests"


@pytest.mark.asyncio
async def test_requests_not_counted_when_queue_overflows():
    """A request that overflows the queue must not consume request budget."""
    gate = ConcurrencyGate(max_inflight=1, max_waiting=0, timeout=1.0)
    gate.set_key_limits("k1", KeyLimits(max_requests=2, max_requests_window_s=60.0))
    held = await gate.acquire("k1")          # consumes 1 of 2 request budget
    with pytest.raises(QueueOverflow):
        await gate.acquire("k1")             # passes check, overflows -> undone
    gate.release(held)
    # Budget should still have 1 slot -> this 2nd acquire must succeed.
    # (If the overflow had leaked into the budget, this would raise RateLimitExceeded.)
    s1 = await gate.acquire("k1")
    gate.release(s1)
    # 3rd would-be request now exceeds the budget of 2.
    with pytest.raises(RateLimitExceeded) as exc:
        await gate.acquire("k1")
    assert exc.value.limit_type == "requests"


@pytest.mark.asyncio
async def test_requests_not_counted_on_queue_timeout():
    """A timed-out waiter must not consume request budget."""
    gate = ConcurrencyGate(max_inflight=1, max_waiting=2, timeout=0.05)
    gate.set_key_limits("k1", KeyLimits(max_requests=2, max_requests_window_s=60.0))
    held = await gate.acquire("k1")          # consumes 1 of 2 request budget
    with pytest.raises(QueueTimeout):
        await gate.acquire("k1")             # queues, times out -> undone
    gate.release(held)
    # Budget still has room -> 2nd acquire succeeds.
    s1 = await gate.acquire("k1")
    gate.release(s1)


@pytest.mark.asyncio
async def test_per_key_tokens_limit():
    gate = ConcurrencyGate(max_inflight=10, max_waiting=10, timeout=1.0)
    gate.set_key_limits("k1", KeyLimits(max_tokens=10, max_tokens_window_s=60.0))
    s1 = await gate.acquire("k1")
    s1.input_tokens = 6
    s1.output_tokens = 6  # total 12 > 10
    gate.release(s1)
    with pytest.raises(RateLimitExceeded) as exc:
        await gate.acquire("k1")
    assert exc.value.limit_type == "tokens"


@pytest.mark.asyncio
async def test_per_key_requests_hour_window():
    """A multi-hour request window enforces its cap just like the minute one."""
    gate = ConcurrencyGate(max_inflight=10, max_waiting=10, timeout=1.0)
    gate.set_key_limits("k1", KeyLimits(max_requests=3, max_requests_window_s=5 * 3600))
    for _ in range(3):
        s = await gate.acquire("k1")
        gate.release(s)
    with pytest.raises(RateLimitExceeded) as exc:
        await gate.acquire("k1")
    assert exc.value.limit_type == "requests"
    assert exc.value.limit == 3


@pytest.mark.asyncio
async def test_per_key_tokens_hour_window():
    """A multi-hour token window accumulates tokens across releases."""
    gate = ConcurrencyGate(max_inflight=10, max_waiting=10, timeout=1.0)
    gate.set_key_limits("k1", KeyLimits(max_tokens=100, max_tokens_window_s=5 * 3600))
    s1 = await gate.acquire("k1")
    s1.input_tokens = 60
    s1.output_tokens = 0
    gate.release(s1)
    s2 = await gate.acquire("k1")
    s2.input_tokens = 50  # running total 110 > 100
    s2.output_tokens = 0
    gate.release(s2)
    with pytest.raises(RateLimitExceeded) as exc:
        await gate.acquire("k1")
    assert exc.value.limit_type == "tokens"
    assert exc.value.limit == 100


@pytest.mark.asyncio
async def test_request_window_evicts_expired_entries():
    """Entries older than the window are evicted, freeing budget."""
    gate = ConcurrencyGate(max_inflight=10, max_waiting=10, timeout=1.0)
    gate.set_key_limits("k1", KeyLimits(max_requests=1, max_requests_window_s=0.1))
    s = await gate.acquire("k1")
    gate.release(s)
    with pytest.raises(RateLimitExceeded):
        await gate.acquire("k1")  # window still holds the entry
    await asyncio.sleep(0.15)  # entry now expired
    s2 = await gate.acquire("k1")  # succeeds again
    gate.release(s2)


@pytest.mark.asyncio
async def test_token_window_evicts_expired_entries():
    """Token entries older than the window are evicted, freeing budget."""
    gate = ConcurrencyGate(max_inflight=10, max_waiting=10, timeout=1.0)
    gate.set_key_limits("k1", KeyLimits(max_tokens=5, max_tokens_window_s=0.1))
    s = await gate.acquire("k1")
    s.input_tokens = 5
    gate.release(s)
    with pytest.raises(RateLimitExceeded):
        await gate.acquire("k1")  # token window holds 5
    await asyncio.sleep(0.15)  # entry now expired
    s2 = await gate.acquire("k1")  # succeeds again
    s2.input_tokens = 0
    gate.release(s2)


def test_key_limits_validate_rejects_misconfiguration():
    """validate() rejects half-specified pairs and non-positive windows."""
    with pytest.raises(ValueError):
        KeyLimits(max_requests=5).validate()
    with pytest.raises(ValueError):
        KeyLimits(max_requests_window_s=60.0).validate()
    with pytest.raises(ValueError):
        KeyLimits(max_tokens=5).validate()
    with pytest.raises(ValueError):
        KeyLimits(max_tokens_window_s=60.0).validate()
    with pytest.raises(ValueError):
        KeyLimits(max_requests=5, max_requests_window_s=0.0).validate()
    with pytest.raises(ValueError):
        KeyLimits(max_tokens=5, max_tokens_window_s=0.0).validate()
    # A complete pair (or fully absent) is valid.
    KeyLimits(max_requests=5, max_requests_window_s=60.0).validate()
    KeyLimits().validate()


def test_set_limits_floors_at_one():
    gate = ConcurrencyGate(max_inflight=5, max_waiting=5, timeout=1.0)
    gate.set_limits(0, -1)
    snap = gate.snapshot()
    assert snap.max_inflight == 1
    assert snap.max_waiting == 0


# ─── Inflight snapshot + idle tracking + event broadcast ──────────────


@pytest.mark.asyncio
async def test_snapshot_inflight_shows_active_requests():
    """Inflight snapshot lists active slots and drops them on release."""
    gate = ConcurrencyGate(max_inflight=5, max_waiting=5, timeout=1.0)
    s1 = await gate.acquire("k1")
    s1.model = "m1"
    s2 = await gate.acquire("k1")
    s2.model = "m2"
    snap = gate.snapshot()
    assert len(snap.inflight) == 2
    assert {r["model"] for r in snap.inflight} == {"m1", "m2"}
    gate.release(s1)
    assert len(gate.snapshot().inflight) == 1
    gate.release(s2)
    assert gate.snapshot().inflight == []


@pytest.mark.asyncio
async def test_snapshot_includes_started_at_and_no_signal():
    """Snapshot includes started_at (epoch ms) and no_signal (bool)."""
    gate = ConcurrencyGate(max_inflight=5, max_waiting=5, timeout=1.0)
    slot = await gate.acquire("k1")
    snap = gate.snapshot()
    entry = snap.inflight[0]
    assert "started_at" in entry
    assert "no_signal" in entry
    assert entry["no_signal"] is False  # newly acquired, not idle
    assert "elapsed_ms" not in entry
    assert "idle_ms" not in entry
    gate.release(slot)


@pytest.mark.asyncio
async def test_queued_slot_excluded_from_inflight_counted_as_waiting():
    """A slot waiting on a provider slot (phase=queued) is excluded from the
    inflight list and counted in waiting instead of active."""
    gate = ConcurrencyGate(max_inflight=5, max_waiting=5, timeout=1.0)
    active_slot = await gate.acquire("k1")
    queued_slot = await gate.acquire("k2")
    queued_slot.mark_queued()

    snap = gate.snapshot()
    assert len(snap.inflight) == 1            # only the active slot
    assert snap.inflight[0]["id"] == active_slot.seq_id
    assert snap.active == 1                    # processing count, not admitted
    assert snap.waiting == 1                    # provider-queued slot
    assert gate._global.active == 2            # internal admitted counter unchanged

    queued_slot.mark_requesting()
    snap = gate.snapshot()
    assert len(snap.inflight) == 2
    assert snap.active == 2
    assert snap.waiting == 0

    gate.release(active_slot)
    gate.release(queued_slot)
    assert gate.snapshot().active == 0


@pytest.mark.asyncio
async def test_on_change_fires_on_acquire_and_release():
    """The dashboard broadcast hook is invoked on both acquire and release."""
    gate = ConcurrencyGate(max_inflight=5, max_waiting=5, timeout=1.0)
    calls: list[int] = []
    gate.set_on_change(lambda: calls.append(1))
    slot = await gate.acquire("k1")
    assert len(calls) >= 1  # acquire notified
    gate.release(slot)
    assert len(calls) >= 2  # release notified


@pytest.mark.asyncio
async def test_active_not_leaked_on_waiter_cancellation():
    """When a waiter is cancelled while queued (before release grants it),
    _active must not be leaked."""
    gate = ConcurrencyGate(max_inflight=1, max_waiting=4, timeout=10.0)
    s1 = await gate.acquire("k1")
    assert gate._global.active == 1

    # Start a waiter that will be cancelled while still queued.
    waiter_task = asyncio.create_task(gate.acquire("k2"))
    await asyncio.sleep(0.05)
    assert gate.snapshot().waiting == 1

    # Cancel the waiter BEFORE it's granted a slot.
    waiter_task.cancel()
    try:
        await waiter_task
    except (asyncio.CancelledError, Exception):
        pass

    # _active should still be 1 (only s1 holds a slot).
    assert gate._global.active == 1

    # Release s1: no waiter to wake (it was cancelled), _active drops to 0.
    gate.release(s1)
    assert gate._global.active == 0


@pytest.mark.asyncio
async def test_on_change_callback_error_does_not_crash():
    """A failing on_change callback must not crash _notify or prevent
    waiter wakeup."""
    gate = ConcurrencyGate(max_inflight=1, max_waiting=2, timeout=2.0)

    def bad_callback():
        raise RuntimeError("boom")

    gate.set_on_change(bad_callback)
    s1 = await gate.acquire("k1")

    # Start a waiter.
    waiter = asyncio.create_task(gate.acquire("k2"))
    await asyncio.sleep(0.05)

    # Release should still wake the waiter despite callback errors.
    gate.release(s1)
    s2 = await asyncio.wait_for(waiter, 1.0)
    assert s2 is not None
    gate.release(s2)


@pytest.mark.asyncio
async def test_waiter_completes_after_release_grants_slot():
    """When release() grants a slot to a waiter, the waiter must register
    its slot and return, no _active leak."""
    gate = ConcurrencyGate(max_inflight=1, max_waiting=4, timeout=2.0)
    s1 = await gate.acquire("k1")

    acq_task = asyncio.create_task(gate.acquire("k2"))
    await asyncio.sleep(0.05)

    gate.release(s1)
    slot = await asyncio.wait_for(acq_task, 1.0)
    assert gate._global.active == 1
    gate.release(slot)
    assert gate._global.active == 0


@pytest.mark.asyncio
async def test_cancel_waiter_with_already_resolved_future():
    """_cancel_waiter must return the granted slot when the future was
    already resolved by release(), tests the timeout/cancel race fix."""
    gate = ConcurrencyGate(max_inflight=1, max_waiting=4, timeout=2.0)
    s1 = await gate.acquire("k1")

    # Manually queue a waiter to get a reference to its future.
    acq_task = asyncio.create_task(gate.acquire("k2"))
    await asyncio.sleep(0.05)

    # Release grants the slot, future is now resolved.
    gate.release(s1)

    # The waiter should complete successfully (not timeout/cancel).
    slot = await asyncio.wait_for(acq_task, 1.0)
    gate.release(slot)
    assert gate._global.active == 0


@pytest.mark.asyncio
async def test_no_signal_not_set_during_ttft_wait():
    """`_idle_notified` ("no signal") must not fire before the first byte."""
    gate = ConcurrencyGate(max_inflight=5, max_waiting=5, timeout=10.0)
    slot = await gate.acquire("k1")
    gate._global._on_idle_timeout(slot)

    assert slot._idle_notified is False


@pytest.mark.asyncio
async def test_no_signal_set_after_byte_then_idle():
    """`_idle_notified` ("no signal") fires when bytes were flowing but
    then stopped for longer than the idle threshold."""
    gate = ConcurrencyGate(max_inflight=5, max_waiting=5, timeout=10.0)
    slot = await gate.acquire("k1")
    slot.last_byte_time = time.monotonic() - 40.0
    gate._global._on_idle_timeout(slot)

    assert slot._idle_notified is True
