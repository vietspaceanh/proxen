from __future__ import annotations

import time

from proxen.core.gate import HealthGuard


# ─── State transitions ──────────────────────────────────────────────


def test_starts_healthy():
    g = HealthGuard(failure_threshold=3, cooldown=10.0)
    assert g.state == "healthy"
    assert g.is_healthy() is True


def test_healthy_after_few_failures():
    g = HealthGuard(failure_threshold=3, cooldown=10.0)
    g.record_failure()
    g.record_failure()
    assert g.state == "healthy"
    assert g.is_healthy() is True


def test_transitions_to_failing_at_threshold():
    g = HealthGuard(failure_threshold=3, cooldown=10.0)
    for _ in range(3):
        g.record_failure()
    assert g.state == "failing"
    assert g.is_healthy() is False


def test_success_resets_failures():
    g = HealthGuard(failure_threshold=3, cooldown=10.0)
    g.record_failure()
    g.record_failure()
    g.record_success()
    g.record_failure()
    assert g.state == "healthy"


def test_failing_to_probing_after_recovery():
    g = HealthGuard(failure_threshold=2, cooldown=0.05)
    g.record_failure()
    g.record_failure()
    assert g.state == "failing"
    time.sleep(0.06)
    assert g.state == "probing"
    assert g.is_healthy() is True


def test_probing_success_returns_to_healthy():
    g = HealthGuard(failure_threshold=2, cooldown=0.05)
    g.record_failure()
    g.record_failure()
    assert g.state == "failing"
    time.sleep(0.06)
    assert g.state == "probing"
    g.record_success()
    assert g.state == "healthy"
    assert g.is_healthy() is True


def test_probing_failure_returns_to_failing():
    g = HealthGuard(failure_threshold=2, cooldown=0.05)
    g.record_failure()
    g.record_failure()
    assert g.state == "failing"
    time.sleep(0.06)
    assert g.state == "probing"
    g.record_failure()
    assert g.state == "failing"
    assert g.is_healthy() is False


def test_full_cycle():
    g = HealthGuard(failure_threshold=2, cooldown=0.05)
    assert g.state == "healthy"

    g.record_failure()
    g.record_failure()
    assert g.state == "failing"

    time.sleep(0.06)
    assert g.state == "probing"

    g.record_failure()
    assert g.state == "failing"

    time.sleep(0.06)
    assert g.state == "probing"

    g.record_success()
    assert g.state == "healthy"
