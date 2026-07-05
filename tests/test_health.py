from __future__ import annotations

import time

from proxen.core.health import HealthCheck


def test_should_try_healthy():
    h = HealthCheck(failure_threshold=5, backoff_base=5.0)
    assert h.should_try(("p1", "model-a")) is True


def test_should_try_false_after_trip():
    h = HealthCheck(failure_threshold=5, backoff_base=5.0)
    h.record_failure(("p1", "model-a"), weight=5)
    assert h.should_try(("p1", "model-a")) is False


def test_should_retry_false_within_backoff():
    h = HealthCheck(failure_threshold=3, backoff_base=0.2)
    h.record_failure(("p1", "m"), weight=3)
    assert h.should_retry(("p1", "m")) is False


def test_should_retry_true_after_backoff():
    h = HealthCheck(failure_threshold=3, backoff_base=0.05)
    h.record_failure(("p1", "m"), weight=3)
    time.sleep(0.06)
    assert h.should_retry(("p1", "m")) is True


def test_should_retry_false_for_healthy():
    h = HealthCheck(failure_threshold=3, backoff_base=0.05)
    assert h.should_retry(("p1", "m")) is False


def test_weighted_failure_trips_faster():
    h = HealthCheck(failure_threshold=5, backoff_base=5.0)
    h.record_failure(("p1", "m"), weight=2)
    h.record_failure(("p1", "m"), weight=2)
    assert h.should_try(("p1", "m")) is True
    h.record_failure(("p1", "m"), weight=2)
    assert h.should_try(("p1", "m")) is False


def test_normal_failure_trips_at_threshold():
    h = HealthCheck(failure_threshold=5, backoff_base=5.0)
    for _ in range(4):
        h.record_failure(("p1", "m"))
    assert h.should_try(("p1", "m")) is True
    h.record_failure(("p1", "m"))
    assert h.should_try(("p1", "m")) is False


def test_record_success_resets():
    h = HealthCheck(failure_threshold=3, backoff_base=5.0)
    h.record_failure(("p1", "m"), weight=3)
    assert h.should_try(("p1", "m")) is False
    h.record_success(("p1", "m"))
    assert h.should_try(("p1", "m")) is True
    assert h.should_retry(("p1", "m")) is False


def test_exponential_backoff_sequence():
    h = HealthCheck(failure_threshold=1, backoff_base=0.01)

    t0 = time.monotonic()
    h.record_failure(("p1", "m"))
    assert h._next[("p1", "m")] >= t0 + 0.01

    t1 = time.monotonic()
    h.record_failure(("p1", "m"))
    assert h._next[("p1", "m")] >= t1 + 0.02

    t2 = time.monotonic()
    h.record_failure(("p1", "m"))
    assert h._next[("p1", "m")] >= t2 + 0.04

    t3 = time.monotonic()
    h.record_failure(("p1", "m"))
    assert h._next[("p1", "m")] >= t3 + 0.08


def test_backoff_capped_at_max():
    h = HealthCheck(failure_threshold=1, backoff_base=200.0)
    h.record_failure(("p1", "m"))
    for _ in range(10):
        h.record_failure(("p1", "m"))
    assert h._next[("p1", "m")] <= time.monotonic() + HealthCheck._MAX + 1


def test_disabled_via_zero_threshold():
    h = HealthCheck(failure_threshold=0, backoff_base=5.0)
    h.record_failure(("p1", "m"), weight=100)
    assert h.should_try(("p1", "m")) is True
    assert h.should_retry(("p1", "m")) is False


def test_per_key_isolation():
    h = HealthCheck(failure_threshold=3, backoff_base=5.0)
    h.record_failure(("p1", "model-a"), weight=3)
    assert h.should_try(("p1", "model-a")) is False
    assert h.should_try(("p1", "model-b")) is True
    assert h.should_try(("p2", "model-a")) is True


def test_state_healthy():
    h = HealthCheck(failure_threshold=5, backoff_base=5.0)
    assert h.state(("p1", "m")) == "healthy"


def test_state_failing():
    h = HealthCheck(failure_threshold=3, backoff_base=5.0)
    h.record_failure(("p1", "m"), weight=3)
    assert h.state(("p1", "m")) == "failing"


def test_state_probing():
    h = HealthCheck(failure_threshold=3, backoff_base=0.05)
    h.record_failure(("p1", "m"), weight=3)
    time.sleep(0.06)
    assert h.state(("p1", "m")) == "probing"


def test_failing_states_excludes_healthy():
    h = HealthCheck(failure_threshold=3, backoff_base=5.0)
    h.record_failure(("p1", "a"), weight=3)
    h.record_failure(("p1", "b"))
    states = h.failing_states()
    assert ("p1", "a") in states
    assert ("p1", "b") not in states


def test_full_cycle():
    h = HealthCheck(failure_threshold=2, backoff_base=0.05)
    assert h.state(("p1", "m")) == "healthy"

    h.record_failure(("p1", "m"))
    h.record_failure(("p1", "m"))
    assert h.state(("p1", "m")) == "failing"
    assert h.should_try(("p1", "m")) is False

    time.sleep(0.06)
    assert h.state(("p1", "m")) == "probing"
    assert h.should_retry(("p1", "m")) is True

    h.record_failure(("p1", "m"))
    assert h.state(("p1", "m")) == "failing"

    time.sleep(0.11)
    assert h.state(("p1", "m")) == "probing"

    h.record_success(("p1", "m"))
    assert h.state(("p1", "m")) == "healthy"
