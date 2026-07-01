"""HTTP response helpers shared by the endpoint layer."""
from __future__ import annotations

import msgspec
from blacksheep import Content, Response


def error_json(status: int, message: str, type: str = "proxen_error") -> Response:
    body = msgspec.json.encode({"error": {"message": message, "type": type}})
    return Response(
        status,
        [(b"content-type", b"application/json")],
        Content(b"application/json", body),
    )


def json_response(data, status: int = 200) -> Response:
    body = msgspec.json.encode(data)
    return Response(
        status,
        [(b"content-type", b"application/json")],
        Content(b"application/json", body),
    )
