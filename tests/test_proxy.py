from __future__ import annotations

import time

import pytest

from proxen.core.sse import (
    SSEUsageParser,
    _extract_usage,
    parse_json_usage,
)
from proxen.core.httputil import (
    filter_headers,
    protocol_from_path,
    speed_metrics,
)
from proxen.services.proxy import (
    Proxy,
)


# ─── Usage extraction ───────────────────────────────────────────────


def test_extract_usage_basic():
    obj = {"usage": {"prompt_tokens": 10, "completion_tokens": 4}}
    u = _extract_usage(obj)
    assert u.input_tokens == 10
    assert u.output_tokens == 4
    assert u.cached_input_tokens == 0


def test_extract_usage_cached_tokens():
    obj = {
        "usage": {
            "prompt_tokens": 12,
            "completion_tokens": 2,
            "prompt_tokens_details": {"cached_tokens": 5},
        }
    }
    u = _extract_usage(obj)
    assert u.input_tokens == 12
    assert u.cached_input_tokens == 5
    assert u.output_tokens == 2


def test_extract_usage_missing():
    assert _extract_usage({}).input_tokens == 0
    assert _extract_usage({"usage": "junk"}).output_tokens == 0


def test_parse_json_usage_invalid():
    assert parse_json_usage(b"not json").input_tokens == 0
    assert parse_json_usage(b"[]").input_tokens == 0


# ─── SSE parser (incl. usage straddling a chunk boundary) ───────────


FULL_STREAM = (
    b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n'
    b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":7,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":3}}}\n\n'
    b'data: [DONE]\n\n'
)


def test_sse_parser_single_chunk():
    p = SSEUsageParser()
    p.feed(FULL_STREAM)
    u, _ = p.finalize()
    assert u.input_tokens == 7
    assert u.output_tokens == 2
    assert u.cached_input_tokens == 3


def test_sse_parser_usage_split_across_boundary():
    """The usage event is split precisely so that its tail buffer rebuilds
    the data line across feed() calls."""
    usage_line = (
        b'data: {"choices":[],"usage":{"prompt_tokens":9,"completion_tokens":5}}\n\n'
    )
    pre = b'data: {"choices":[{"delta":{"content":"x"}}]}\n\n'
    blob = pre + usage_line + b'data: [DONE]\n\n'
    # Feed in odd-sized slices so usage straddles boundaries.
    p = SSEUsageParser()
    for i in range(0, len(blob), 7):
        p.feed(blob[i : i + 7])
    u, _ = p.finalize()
    assert u.input_tokens == 9
    assert u.output_tokens == 5


def test_sse_parser_no_usage():
    p = SSEUsageParser()
    p.feed(b'data: {"choices":[{"delta":{"content":"x"}}]}\n\ndata: [DONE]\n\n')
    u, _ = p.finalize()
    assert u.input_tokens == 0 and u.output_tokens == 0


def test_sse_parser_captures_usage_with_choices():
    """Usage + choices travel in the final chunk; the SSE parser reads it."""
    p = SSEUsageParser()
    p.feed(
        b'data: {"choices":[{"delta":{"content":"x"}}]}\n\n'
        b'data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":4,"completion_tokens":1}}\n\n'
        b'data: [DONE]\n\n'
    )
    u, _ = p.finalize()
    assert u.input_tokens == 4 and u.output_tokens == 1


def test_sse_parser_tail_buffer_capped():
    """A large early chunk must not blow up memory; usage is in the tail."""
    p = SSEUsageParser()
    p.feed(b"x" * 20000)  # larger than the internal tail cap
    p.feed(
        b'data: {"usage":{"prompt_tokens":1,"completion_tokens":1}}\n\n'
    )
    u, _ = p.finalize()
    assert u.input_tokens == 1 and u.output_tokens == 1


def test_sse_parser_found_usage():
    """finalize() returns found_usage=True when 'usage' key is present."""
    p = SSEUsageParser()
    p.feed(b'data: {"choices":[]}\n\n')
    _, found = p.finalize()
    assert found is False
    p2 = SSEUsageParser()
    p2.feed(b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}\n\n')
    _, found2 = p2.finalize()
    assert found2 is True


def test_sse_parser_content_containing_done_string():
    """Model output containing [DONE] in content must not break usage parsing.
    The parser no longer uses string matching for stream completion."""
    p = SSEUsageParser()
    p.feed(
        b'data: {"choices":[{"delta":{"content":"the marker is [DONE]"}}]}\n\n'
        b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":5,"completion_tokens":3}}\n\n'
        b'data: [DONE]\n\n'
    )
    u, _ = p.finalize()
    assert u.input_tokens == 5 and u.output_tokens == 3


# ─── Header forwarding / compression ───────────────────────────


class _DummyHeaders(dict):
    def items(self):
        return super().items()


def test_forward_headers_preserves_accept_encoding():
    """proxen preserves the client's Accept-Encoding for cache transparency."""
    h = filter_headers(
        {"Authorization": "Bearer clientkey", "Accept-Encoding": "br, gzip, deflate"},
        provider_key="provider-secret",
    )
    assert h["Authorization"] == "Bearer provider-secret"
    assert h["Accept-Encoding"] == "br, gzip, deflate"


def test_forward_headers_strips_hop_by_hop():
    h = filter_headers(
        {
            "Host": "evil",
            "Content-Length": "5",
            "Connection": "keep-alive",
            "X-Custom": "keep-me",
        },
        provider_key="pk",
    )
    assert "Host" not in h and "Content-Length" not in h
    assert "Connection" not in h
    assert h["X-Custom"] == "keep-me"


def test_resp_headers_strips_encoding_and_length():
    """With auto_decompress=True the body is decoded, so the forwarded
    response must not advertise content-encoding/content-length."""
    from proxen.core.httputil import filter_headers

    out = filter_headers(
        {
            "Content-Encoding": "gzip",
            "Content-Length": "42",
            "Content-Type": "application/json",
            "X-Trace": "keep",
        }
    )
    assert "Content-Encoding" not in out
    assert "Content-Length" not in out
    assert out["Content-Type"] == "application/json"
    assert out["X-Trace"] == "keep"


def test_record_completion_flags():
    """client_disconnect / upstream_dropped / needs_review from completion state.

    - upstream_dropped: stream ended without completion and no client disconnect.
    - client_disconnect: client cancelled, including the race where the upstream
      closes the stream (completed=True) before usage is emitted.
    - needs_review: a cleanly completed 200 with zero usage (parser suspect);
      cancels and drops are exempt.
    """
    from unittest.mock import MagicMock

    from proxen.core.sse import UsageStats

    def _call(completed, disconnected, usage=None):
        sink = MagicMock()
        proxy = Proxy(MagicMock(), MagicMock(), MagicMock(), sink, MagicMock())
        proxy._record(
            wall_start=0.0, model="m", upstream="u", key_id="k",
            ttft=0.0, tps=0.0, usage=usage or UsageStats(),
            status=200, duration=0.0, stream=True,
            disconnected=disconnected, completed=completed,
        )
        return sink.enqueue.call_args.args[0]

    used = UsageStats(input_tokens=5, output_tokens=2)

    # upstream drop: not completed, not disconnected
    rec = _call(False, False)
    assert rec.upstream_dropped is True and rec.client_disconnect is False
    assert rec.needs_review is False

    # client cancel mid-stream: not completed, disconnected
    rec = _call(False, True)
    assert rec.upstream_dropped is False and rec.client_disconnect is True
    assert rec.needs_review is False

    # normal completion with usage
    rec = _call(True, False, used)
    assert rec.upstream_dropped is False and rec.client_disconnect is False
    assert rec.needs_review is False

    # late disconnect after the stream delivered all data: not a cancel
    rec = _call(True, True, used)
    assert rec.upstream_dropped is False and rec.client_disconnect is False
    assert rec.needs_review is False

    # cancel where the upstream closed without emitting usage (completed race)
    rec = _call(True, True)
    assert rec.client_disconnect is True and rec.upstream_dropped is False
    assert rec.needs_review is False

    # genuine parser concern: completed, connected, zero usage
    rec = _call(True, False)
    assert rec.needs_review is True


# ─── speed_metrics ──────────────────────────────────────────────────


def test_speed_metrics_real_stream_uses_gen_time():
    # gen_time = 2s >= 1s min, so the rate uses the real generation phase.
    ttft, tps = speed_metrics(200, 0.4, 2.4, 500)
    assert ttft == 0.4
    assert tps == pytest.approx(250.0)  # 500 / (2.4 - 0.4), TTFT excluded


def test_speed_metrics_long_ttft_stream_excludes_wait():
    # The motivating case: ~38s time-to-first-token, then tokens stream.
    # The gen-based rate (~48 t/s) is what matters, not output/total (~3.5).
    _, tps = speed_metrics(200, 38.0, 41.0, 144)
    assert tps == pytest.approx(48.0)


def test_speed_metrics_queued_fast_stream_excludes_wait():
    # TTFT is queue wait (server was busy); the 10s queue is excluded and the
    # rate uses the 2s generation phase only (100, not 200 / 12 ~= 17).
    _, tps = speed_metrics(200, 10.0, 12.0, 200)
    assert tps == pytest.approx(100.0)


def test_speed_metrics_short_stream_is_null():
    # Tokens arrive in a ~14ms burst after a long TTFT: a buffered flush.
    # Raw gen_time would give an absurd rate, so the rate is not measurable.
    assert speed_metrics(200, 4.69, 4.704, 176)[1] is None


def test_speed_metrics_sub_min_stream_is_null():
    # gen_time < 1s: too short to yield a trustworthy rate -> None.
    assert speed_metrics(200, 0.1, 0.3, 60)[1] is None
    assert speed_metrics(200, 0.0, 0.05, 30)[1] is None


def test_speed_metrics_non_streaming_uses_duration():
    # Non-streaming: ttft=duration -> gen_time=0 -> no generation phase, so
    # the whole end-to-end duration is the denominator (no 1s floor).
    _, tps = speed_metrics(200, 2.0, 2.0, 100)
    assert tps == pytest.approx(50.0)  # 100 / 2.0


def test_speed_metrics_non_streaming_sub_second_uses_duration():
    # A sub-second non-streaming response still uses its real duration, never
    # the (streaming) 1s min -- it is a real end-to-end measurement.
    _, tps = speed_metrics(200, 0.5, 0.5, 50)
    assert tps == pytest.approx(100.0)  # 50 / 0.5


def test_speed_metrics_error_status_zeroes():
    assert speed_metrics(500, 1.0, 2.0, 100) == (0.0, 0.0)
    assert speed_metrics(429, 1.0, 2.0, 100) == (0.0, 0.0)


def test_speed_metrics_zero_tokens():
    _, tps = speed_metrics(200, 0.4, 2.4, 0)
    assert tps == 0.0


# ─── protocol_from_path ─────────────────────────────────────────────


def test_protocol_from_path():
    assert protocol_from_path("/v1/messages") == "anthropic"
    assert protocol_from_path("/v1/messages/count_tokens") == "anthropic"
    assert protocol_from_path("/v1/chat/completions") == "openai"
    assert protocol_from_path("/v1/completions") == "openai"
    assert protocol_from_path("/v1/embeddings") == "openai"
    # bytes-tolerant
    assert protocol_from_path(b"/v1/messages") == "anthropic"


# ─── Anthropic usage extraction ─────────────────────────────────────


def test_extract_usage_anthropic():
    obj = {"usage": {"input_tokens": 25, "output_tokens": 15, "cache_read_input_tokens": 5}}
    u = _extract_usage(obj, "anthropic")
    assert u.input_tokens == 25
    assert u.output_tokens == 15
    assert u.cached_input_tokens == 5


def test_extract_usage_anthropic_missing_fields():
    assert _extract_usage({}, "anthropic").input_tokens == 0
    assert _extract_usage({"usage": "junk"}, "anthropic").output_tokens == 0


def test_parse_json_usage_anthropic():
    body = (
        b'{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"Hi"}],'
        b'"model":"claude","stop_reason":"end_turn",'
        b'"usage":{"input_tokens":30,"output_tokens":7,"cache_read_input_tokens":3}}'
    )
    u = parse_json_usage(body, "anthropic")
    assert u.input_tokens == 30
    assert u.output_tokens == 7
    assert u.cached_input_tokens == 3


def test_parse_json_usage_anthropic_count_tokens_records_zero():
    """count_tokens returns top-level input_tokens (no `usage` key) -> 0."""
    u = parse_json_usage(b'{"input_tokens": 42}', "anthropic")
    assert u.input_tokens == 0 and u.output_tokens == 0


# ─── Anthropic header forwarding ─────────────────────────────────────


def test_forward_headers_anthropic_uses_x_api_key():
    h = filter_headers(
        {
            "x-api-key": "client-key",
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        provider_key="provider-secret",
        protocol="anthropic",
    )
    assert h["x-api-key"] == "provider-secret"  # upstream key, not client's
    assert h["anthropic-version"] == "2023-06-01"  # forwarded from client
    assert "Authorization" not in h
    assert h["Content-Type"] == "application/json"


def test_forward_headers_anthropic_defaults_version():
    h = filter_headers(
        {"Content-Type": "application/json"},
        provider_key="provider-secret",
        protocol="anthropic",
    )
    assert h["x-api-key"] == "provider-secret"
    assert h["anthropic-version"] == "2023-06-01"


def test_forward_headers_anthropic_strips_client_x_api_key():
    h = filter_headers(
        {"x-api-key": "client-leaked-key"},
        provider_key="provider-secret",
        protocol="anthropic",
    )
    assert h["x-api-key"] == "provider-secret"


# ─── Anthropic SSE parser (head+tail) ───────────────────────────────


ANTHROPIC_STREAM = (
    b'event: message_start\n'
    b'data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":9,"output_tokens":1,"cache_read_input_tokens":2}}}\n\n'
    b'event: content_block_start\n'
    b'data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n'
    b'event: content_block_delta\n'
    b'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}\n\n'
    b'event: content_block_delta\n'
    b'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" world"}}\n\n'
    b'event: content_block_stop\n'
    b'data: {"type":"content_block_stop","index":0}\n\n'
    b'event: message_delta\n'
    b'data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":4}}\n\n'
    b'event: message_stop\n'
    b'data: {"type":"message_stop"}\n\n'
)


def test_sse_parser_anthropic_single_chunk():
    p = SSEUsageParser("anthropic")
    p.feed(ANTHROPIC_STREAM)
    u, found = p.finalize()
    assert u.input_tokens == 9
    assert u.output_tokens == 4
    assert u.cached_input_tokens == 2
    assert found is True


def test_sse_parser_anthropic_split_across_boundary():
    """message_start (input) and message_delta (output) arrive in separate
    feeds; input is captured by the head buffer, output by the tail."""
    p = SSEUsageParser("anthropic")
    for i in range(0, len(ANTHROPIC_STREAM), 7):
        p.feed(ANTHROPIC_STREAM[i : i + 7])
    u, _ = p.finalize()
    assert u.input_tokens == 9
    assert u.output_tokens == 4
    assert u.cached_input_tokens == 2


def test_sse_parser_anthropic_large_middle_evicts_start_from_tail():
    """A large middle blob exceeds the tail cap, so message_start would be
    lost by a tail-only buffer. The head buffer must still capture it."""
    start = ANTHROPIC_STREAM.split(b"event: content_block_start")[0]
    middle = (
        b'event: content_block_delta\n'
        b'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"'
        + b"x" * 20000
        + b'"}}\n\n'
    )
    end = (
        b'event: message_delta\n'
        b'data: {"type":"message_delta","delta":{},"usage":{"output_tokens":4}}\n\n'
        b'event: message_stop\n'
        b'data: {"type":"message_stop"}\n\n'
    )
    p = SSEUsageParser("anthropic")
    p.feed(start)
    p.feed(middle)
    p.feed(end)
    u, found = p.finalize()
    assert u.input_tokens == 9  # would be 0 without the head buffer
    assert u.output_tokens == 4
    assert found is True


def test_sse_parser_anthropic_no_usage():
    p = SSEUsageParser("anthropic")
    p.feed(b'event: ping\ndata: {"type":"ping"}\n\n')
    u, found = p.finalize()
    assert u.input_tokens == 0 and u.output_tokens == 0
    assert found is False


def test_sse_parser_openai_default_protocol_unchanged():
    """SSEUsageParser() with no arg still parses OpenAI streams as before."""
    p = SSEUsageParser()
    p.feed(
        b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":7,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":3}}}\n\n'
        b'data: [DONE]\n\n'
    )
    u, found = p.finalize()
    assert u.input_tokens == 7
    assert u.output_tokens == 2
    assert u.cached_input_tokens == 3
    assert found is True
