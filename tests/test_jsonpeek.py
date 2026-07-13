"""Tests for the fast model/stream JSON scanner (core.jsonpeek)."""
from __future__ import annotations

import msgspec

from proxen.core.body import peek_model_stream


def test_basic_extraction():
    body = b'{"model":"gpt-4","messages":[],"stream":true}'
    assert peek_model_stream(body) == ("gpt-4", True)


def test_stream_defaults_false():
    body = b'{"model":"claude-3","messages":[]}'
    assert peek_model_stream(body) == ("claude-3", False)


def test_stops_before_messages_array():
    """The scanner never enters the messages array (the expensive part)."""
    big = b'{"model":"gpt-4","stream":false,"messages":[{"role":"user","content":"' + b"x" * 100_000 + b'"}]}'
    model, stream = peek_model_stream(big)
    assert model == "gpt-4"
    assert stream is False


def test_keys_in_any_order():
    body = b'{"stream":true,"messages":[],"model":"gpt-4"}'
    assert peek_model_stream(body) == ("gpt-4", True)


def test_escaped_model_string():
    body = b'{"model":"model\\"with\\"quotes","stream":false}'
    model, _ = peek_model_stream(body)
    assert model == 'model"with"quotes'


def test_unicode_model():
    body = b'{"model":"gpt-4\xc3\xa9","stream":false}'
    model, _ = peek_model_stream(body)
    assert model == "gpt-4é"


def test_fallback_non_object_json():
    """A non-object top level falls back to full decode."""
    body = b'["not","an","object"]'
    assert peek_model_stream(body) == ("", False)


def test_fallback_empty_body():
    assert peek_model_stream(b"") == ("", False)


def test_fallback_malformed_json():
    assert peek_model_stream(b"{not json") == ("", False)


def test_fallback_model_not_string():
    """If model is not a string, falls back to full decode."""
    body = b'{"model":42,"stream":true}'
    model, stream = peek_model_stream(body)
    assert model == "42"  # full decode: str(42)
    assert stream is True


def test_fallback_stream_not_bool():
    """If stream is not a bool literal, falls back to full decode."""
    body = b'{"model":"gpt-4","stream":"yes"}'
    model, stream = peek_model_stream(body)
    assert model == "gpt-4"
    assert stream is True  # full decode: bool("yes") = True


def test_no_model_field():
    body = b'{"messages":[],"stream":true}'
    assert peek_model_stream(body) == ("", True)


def test_matches_full_decode_on_various_inputs():
    """The scanner must produce the same result as full decode for valid inputs."""
    cases = [
        b'{"model":"a","stream":true}',
        b'{"stream":false,"model":"b","messages":[]}',
        b'{"model":"","stream":false}',
        b'{"nested":{"model":"decoy"},"model":"real","stream":true}',
        b'{"model":"x","extra":[1,2,3],"stream":false,"messages":[]}',
    ]
    for body in cases:
        scanner_result = peek_model_stream(body)
        payload = msgspec.json.decode(body)
        expected = (str(payload.get("model", "") or ""), bool(payload.get("stream", False)))
        assert scanner_result == expected, f"mismatch for {body!r}"


def test_nested_model_not_extracted():
    """A 'model' key nested inside a value must not be extracted."""
    body = b'{"model":"real","messages":[{"model":"decoy"}],"stream":false}'
    model, _ = peek_model_stream(body)
    assert model == "real"
