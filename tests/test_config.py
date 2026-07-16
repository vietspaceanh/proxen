"""Config validation tests for upstream timeout settings."""
from __future__ import annotations

import pytest

from proxen.core.config import _build_settings


def test_default_settings_are_valid():
    settings = _build_settings({})
    assert settings.upstream_sock_read > 0
    assert settings.upstream_non_streaming_timeout > 0


def test_upstream_sock_read_must_be_positive():
    with pytest.raises(ValueError, match="upstream_sock_read"):
        _build_settings({"upstream_sock_read": 0})
    with pytest.raises(ValueError, match="upstream_sock_read"):
        _build_settings({"upstream_sock_read": -1})


def test_upstream_non_streaming_timeout_must_be_positive():
    with pytest.raises(ValueError, match="upstream_non_streaming_timeout"):
        _build_settings({"upstream_non_streaming_timeout": 0})
    with pytest.raises(ValueError, match="upstream_non_streaming_timeout"):
        _build_settings({"upstream_non_streaming_timeout": -5})


def test_upstream_ttft_timeout_zero_is_allowed():
    # 0 disables the TTFT gate, so it is a valid value.
    settings = _build_settings({"upstream_ttft_timeout": 0})
    assert settings.upstream_ttft_timeout == 0


def test_upstream_ttft_timeout_negative_rejected():
    with pytest.raises(ValueError, match="upstream_ttft_timeout"):
        _build_settings({"upstream_ttft_timeout": -1})
