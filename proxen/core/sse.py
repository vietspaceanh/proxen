"""SSE usage extraction for OpenAI- and Anthropic-compatible streams.

Pure protocol helpers with no dependency on the proxy or upstream code, so
they can be unit-tested in isolation and reused by any compatible stream
consumer.
"""
from __future__ import annotations

from dataclasses import dataclass

import msgspec


@dataclass
class UsageStats:
    input_tokens: int = 0
    cached_input_tokens: int = 0
    output_tokens: int = 0


def _extract_usage(obj: dict, protocol: str = "openai") -> UsageStats:
    usage = obj.get("usage") or {}
    if not isinstance(usage, dict):
        usage = {}
    if protocol == "anthropic":
        return UsageStats(
            input_tokens=usage.get("input_tokens", 0) or 0,
            cached_input_tokens=usage.get("cache_read_input_tokens", 0) or 0,
            output_tokens=usage.get("output_tokens", 0) or 0,
        )
    cached = 0
    details = usage.get("prompt_tokens_details")
    if isinstance(details, dict):
        cached = details.get("cached_tokens", 0) or 0
    return UsageStats(
        input_tokens=usage.get("prompt_tokens", 0) or 0,
        cached_input_tokens=cached,
        output_tokens=usage.get("completion_tokens", 0) or 0,
    )


def parse_json_usage(body: bytes, protocol: str = "openai") -> UsageStats:
    """Extract usage from a complete (non-streaming) JSON response body."""
    try:
        obj = msgspec.json.decode(body)
    except (msgspec.DecodeError, UnicodeDecodeError):
        return UsageStats()
    if not isinstance(obj, dict):
        return UsageStats()
    return _extract_usage(obj, protocol)


def _iter_data_objs(buf: bytes):
    """Yield parsed JSON dicts from `data:` lines in an SSE buffer."""
    for line in buf.splitlines():
        idx = line.find(b"data:")
        if idx == -1:
            continue
        data = line[idx + 5:].lstrip()
        if not data or data == b"[DONE]":
            continue
        try:
            obj = msgspec.json.decode(data)
        except Exception:
            continue
        if isinstance(obj, dict):
            yield obj


class SSEUsageParser:
    """Zero-overhead SSE usage scanner.

    OpenAI: usage is in the final data line, so a small tail buffer scanned
    backward at `finalize` suffices.

    Anthropic: input tokens arrive in the first event (`message_start`)
    and output tokens in the last (`message_delta`), so a small head
    buffer keeps the former and the tail buffer keeps the latter. Only at
    `finalize` are the relevant data lines parsed — no per-chunk JSON.
    """

    _TAIL = 8192  # usage always in the last chunk; 8 KB is generous
    _HEAD = 4096  # anthropic only; message_start is small and arrives first

    def __init__(self, protocol: str = "openai") -> None:
        self._protocol = protocol
        self._tail = bytearray()
        self._head: bytearray | None = (
            bytearray() if protocol == "anthropic" else None
        )
        self._head_full = False

    def feed(self, chunk: bytes) -> None:
        if self._head is not None and not self._head_full:
            room = self._HEAD - len(self._head)
            if len(chunk) <= room:
                self._head.extend(chunk)
            else:
                self._head.extend(chunk[:room])
                self._head_full = True
        self._tail.extend(chunk)
        if len(self._tail) > self._TAIL:
            del self._tail[: len(self._tail) - self._TAIL]

    def finalize(self) -> tuple[UsageStats, bool]:
        if self._protocol == "anthropic":
            return self._finalize_anthropic()
        return self._finalize_openai()

    def _finalize_openai(self) -> tuple[UsageStats, bool]:
        # Scan SSE data lines backwards; usage is in the final event.
        for line in reversed(self._tail.splitlines()):
            idx = line.find(b"data:")
            if idx == -1:
                continue
            data = line[idx + 5:].lstrip()
            try:
                obj = msgspec.json.decode(data)
            except Exception:
                continue
            # "usage" key present -> stream reached its final event.
            if isinstance(obj, dict) and "usage" in obj:
                return _extract_usage(obj, "openai"), True
        return UsageStats(), False

    def _finalize_anthropic(self) -> tuple[UsageStats, bool]:
        # message_start (near stream start) carries message.usage with the
        # input + cached token counts.
        input_tokens = 0
        cached = 0
        found_start = False
        if self._head is not None:
            for obj in _iter_data_objs(self._head):
                if obj.get("type") == "message_start":
                    msg = obj.get("message")
                    if isinstance(msg, dict):
                        u = _extract_usage(msg, "anthropic")
                        input_tokens = u.input_tokens
                        cached = u.cached_input_tokens
                    found_start = True
                    break
        # message_delta (near stream end) carries usage.output_tokens.
        output_tokens = 0
        found_delta = False
        for obj in _iter_data_objs(self._tail):
            if obj.get("type") == "message_delta":
                u = _extract_usage(obj, "anthropic")
                output_tokens = u.output_tokens
                found_delta = True
        return (
            UsageStats(input_tokens, cached, output_tokens),
            found_start or found_delta,
        )
