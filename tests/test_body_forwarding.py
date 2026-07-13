"""Tests for byte-level model field patching (cache-transparency guarantee)."""

from __future__ import annotations

import json

from proxen.core.body import patch_field


def test_patch_model_changes_only_model_value():
    body = b'{"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]}'
    patched = patch_field(body, "model", "claude-3-opus")
    expected = b'{"model": "claude-3-opus", "messages": [{"role": "user", "content": "hi"}]}'
    assert patched == expected


def test_patch_preserves_key_order_and_whitespace():
    body = b'{\n  "z": 1,\n  "model": "gpt-4",\n  "a": 2\n}'
    patched = patch_field(body, "model", "claude-3")
    expected = b'{\n  "z": 1,\n  "model": "claude-3",\n  "a": 2\n}'
    assert patched == expected


def test_patch_model_same_length():
    body = b'{"model": "aaa", "x": 1}'
    patched = patch_field(body, "model", "bbb")
    assert patched == b'{"model": "bbb", "x": 1}'


def test_patch_model_not_found_returns_unchanged():
    body = b'{"messages": []}'
    patched = patch_field(body, "model", "gpt-4")
    assert patched == body


def test_patch_model_with_special_chars():
    body = b'{"model": "old", "x": 1}'
    patched = patch_field(body, "model", "model/with-special.chars")
    assert json.loads(patched)["model"] == "model/with-special.chars"
    assert json.loads(patched)["x"] == 1


def test_patch_model_unicode():
    body = b'{"model": "old", "x": 1}'
    patched = patch_field(body, "model", "model-\u2713")
    assert json.loads(patched)["model"] == "model-\u2713"
    assert json.loads(patched)["x"] == 1


def test_patch_ignores_nested_model():
    body = b'{"model": "old", "messages": [{"model": "nested"}]}'
    patched = patch_field(body, "model", "new")
    result = json.loads(patched)
    assert result["model"] == "new"
    assert result["messages"][0]["model"] == "nested"


def test_patch_does_not_change_body_size_when_same_length():
    body = b'{"model": "abcd", "x": 1}'
    patched = patch_field(body, "model", "wxyz")
    assert len(patched) == len(body)


def test_patch_changes_size_when_different_length():
    body = b'{"model": "ab", "x": 1}'
    patched = patch_field(body, "model", "longer-model-name")
    assert len(patched) > len(body)
    assert json.loads(patched)["model"] == "longer-model-name"
    assert json.loads(patched)["x"] == 1


def test_patch_empty_body():
    assert patch_field(b"", "model", "x") == b""


def test_patch_non_json_body():
    body = b"not json"
    assert patch_field(body, "model", "x") == body


def test_patch_non_object_json():
    body = b'[1, 2, 3]'
    assert patch_field(body, "model", "x") == body
