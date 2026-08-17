"""Browser authentication for subscription-backed upstreams.

The flow is deliberately separate from client authentication.  A browser
auth provider returns short-lived request headers; the proxy's normal route,
queue, and streaming code remains shared with API-key upstreams.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import secrets
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import parse_qs, urlencode, urlsplit

from ..core.config import Upstream
from ..core.upstream_profiles import OPENAI_RESPONSES_PROFILE
from ..services.proxy.context import UpstreamAuthUnavailable
from .telemetry import Database

log = logging.getLogger("proxen.upstream_auth")

OPENAI_ISSUER = "https://auth.openai.com"
OPENAI_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
OAUTH_PORT = 1455
OAUTH_REDIRECT = f"http://localhost:{OAUTH_PORT}/auth/callback"
FLOW_TTL = 5 * 60
TOKEN_REFRESH_MARGIN = 60


class BrowserAuthError(Exception):
    """An administrator-facing browser authentication error."""

    def __init__(self, message: str, *, status: int = 400) -> None:
        self.message = message
        self.status = status
        super().__init__(message)


@dataclass
class _Flow:
    flow_id: str
    upstream_name: str
    created_at: float
    expires_at: float
    status: str = "pending"
    authorization_url: str = ""
    state: str = ""
    verifier: str = ""
    error: str = ""


class BrowserAuthService:
    """Own browser flows and refreshable OAuth credentials."""

    def __init__(
        self,
        db: Database,
        requester: Callable[..., Awaitable[object]],
    ) -> None:
        self._db = db
        self._requester = requester
        self._flows: dict[str, _Flow] = {}
        self._states: dict[str, str] = {}
        self._refresh_locks: dict[str, asyncio.Lock] = {}
        self._tasks: set[asyncio.Task] = set()
        self._callback_server: asyncio.AbstractServer | None = None

    async def start(self, upstream: Upstream) -> dict:
        self._check_upstream(upstream)
        return await self._start_browser(upstream)

    async def headers(self, upstream: Upstream) -> dict[str, str]:
        """Return fresh provider headers, refreshing one account at a time."""
        if upstream.profile != OPENAI_RESPONSES_PROFILE:
            return {}
        self._check_upstream(upstream)
        row = await self._load_token(upstream.name)
        if row is None:
            raise UpstreamAuthUnavailable(
                f"upstream '{upstream.name}' is not authenticated",
                upstream=upstream.name,
            )

        if row["expires_at"] <= time.time() + TOKEN_REFRESH_MARGIN:
            lock = self._refresh_locks.setdefault(upstream.name, asyncio.Lock())
            async with lock:
                row = await self._load_token(upstream.name)
                if row is None:
                    raise UpstreamAuthUnavailable(
                        f"upstream '{upstream.name}' is not authenticated",
                        upstream=upstream.name,
                    )
                if row["expires_at"] <= time.time() + TOKEN_REFRESH_MARGIN:
                    try:
                        tokens = await self._refresh_openai(row["refresh_token"])
                        row = await self._save_tokens(
                            upstream.name, tokens, previous=row
                        )
                    except BrowserAuthError as exc:
                        raise UpstreamAuthUnavailable(
                            f"upstream '{upstream.name}' authentication refresh failed",
                            upstream=upstream.name,
                        ) from exc

        headers = {"Authorization": f"Bearer {row['access_token']}"}
        if row.get("account_id"):
            headers["ChatGPT-Account-Id"] = row["account_id"]
        return headers

    async def status(self, upstream: Upstream) -> dict:
        row = await self._load_token(upstream.name)
        latest = max(
            (flow for flow in self._flows.values() if flow.upstream_name == upstream.name),
            key=lambda flow: flow.created_at,
            default=None,
        )
        if (
            latest is not None
            and latest.status in {"pending", "exchanging", "failed"}
            and (row is None or latest.created_at >= row["updated_at"])
        ):
            return self._public_flow(latest)
        if row is None:
            return {"status": "not_connected"}
        return {
            "status": "expired" if row["expires_at"] <= time.time() else "connected",
            "expires_at": row["expires_at"],
            "account_id": row.get("account_id"),
        }

    async def revoke(self, upstream_name: str) -> None:
        lock = self._refresh_locks.setdefault(upstream_name, asyncio.Lock())
        async with lock:
            await self._db.execute_commit(
                "DELETE FROM upstream_auth_tokens WHERE upstream_name = ?",
                (upstream_name,),
            )

    async def aclose(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._states.clear()
        await self._stop_callback_server()

    # ── Browser flow --------------------------------------------------

    async def _start_browser(self, upstream: Upstream) -> dict:
        await self._ensure_callback_server()
        verifier = secrets.token_urlsafe(32)
        challenge = _base64url(hashlib.sha256(verifier.encode()).digest())
        state = secrets.token_urlsafe(32)
        now = time.time()
        flow = _Flow(
            flow_id=secrets.token_urlsafe(18),
            upstream_name=upstream.name,
            created_at=now,
            expires_at=now + FLOW_TTL,
            state=state,
            verifier=verifier,
        )
        params = {
            "response_type": "code",
            "client_id": OPENAI_CLIENT_ID,
            "redirect_uri": OAUTH_REDIRECT,
            "scope": "openid profile email offline_access",
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "id_token_add_organizations": "true",
            "codex_cli_simplified_flow": "true",
            "state": state,
            "originator": "proxen",
        }
        flow.authorization_url = f"{OPENAI_ISSUER}/oauth/authorize?{urlencode(params)}"
        self._flows[flow.flow_id] = flow
        self._states[state] = flow.flow_id
        self._schedule(self._expire_flow(flow.flow_id, state))
        return self._public_flow(flow)

    async def _handle_callback(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        flow: _Flow | None = None
        try:
            request_line = await asyncio.wait_for(reader.readline(), 10)
            parts = request_line.split()
            target = parts[1].decode("utf-8", "replace") if len(parts) > 1 else "/"
            while await asyncio.wait_for(reader.readline(), 10) not in (b"\r\n", b"\n", b""):
                pass
            query = parse_qs(urlsplit(target).query)
            state = query.get("state", [""])[0]
            flow_id = self._states.get(state)
            flow = self._flows.get(flow_id) if flow_id else None
            if flow is None or flow.expires_at <= time.time():
                html = _html_page("Authentication failed", "Invalid or expired authentication flow.")
                await self._write_callback(writer, 400, html)
                return
            if query.get("error"):
                flow.status = "failed"
                flow.error = (
                    query.get("error_description")
                    or query.get("error")
                    or ["authorization was denied"]
                )[0]
                self._states.pop(flow.state, None)
                html = _html_page("Authentication failed", flow.error)
                await self._write_callback(writer, 400, html)
                await self._stop_callback_server()
                return
            code = query.get("code", [""])[0]
            if not code:
                flow.status = "failed"
                flow.error = "authorization code was missing"
                self._states.pop(flow.state, None)
                html = _html_page("Authentication failed", flow.error)
                await self._write_callback(writer, 400, html)
                await self._stop_callback_server()
                return
            flow.status = "exchanging"
            await self._complete_browser(flow, code)
            html = _html_page("Authentication complete", "You can close this tab and return to Proxen.")
            await self._write_callback(writer, 200, html)
        except Exception:
            log.exception("browser authentication callback failed")
            if flow is not None:
                flow.status = "failed"
                flow.error = "authentication callback failed"
            await self._write_callback(
                writer,
                500,
                _html_page("Authentication failed", "The authentication flow failed."),
            )
        finally:
            writer.close()
            with_context = getattr(writer, "wait_closed", None)
            if with_context is not None:
                try:
                    await with_context()
                except Exception:
                    pass

    async def _complete_browser(self, flow: _Flow, code: str) -> None:
        try:
            status, tokens = await self._oauth_json(
                "POST",
                f"{OPENAI_ISSUER}/oauth/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                form={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": OAUTH_REDIRECT,
                    "client_id": OPENAI_CLIENT_ID,
                    "code_verifier": flow.verifier,
                },
            )
            if status != 200:
                raise BrowserAuthError("token exchange failed", status=502)
            await self._save_tokens(flow.upstream_name, tokens)
            flow.status = "connected"
            self._states.pop(flow.state, None)
            self._flows.pop(flow.flow_id, None)
        except Exception as exc:
            flow.status = "failed"
            flow.error = str(exc)
            self._states.pop(flow.state, None)
            raise
        finally:
            await self._stop_callback_server()

    # ── OAuth and persistence ----------------------------------------

    async def _refresh_openai(self, refresh_token: str) -> dict:
        status, data = await self._oauth_json(
            "POST",
            f"{OPENAI_ISSUER}/oauth/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            form={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": OPENAI_CLIENT_ID,
            },
        )
        if status != 200:
            raise BrowserAuthError("token refresh failed", status=502)
        return data

    async def _oauth_json(self, method: str, url: str, *, headers: dict, payload=None, form=None) -> tuple[int, dict]:
        body = None
        if form is not None:
            body = urlencode(form).encode()
        elif payload is not None:
            body = json.dumps(payload).encode()
        try:
            response = await self._requester(
                method,
                url,
                headers=headers,
                content=body,
                read_timeout=30.0,
            )
        except Exception as exc:
            raise BrowserAuthError(
                "authentication provider request failed",
                status=502,
            ) from exc
        try:
            status = response.status
            data = json.loads((await response.aread()).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            data = {}
        except Exception as exc:
            raise BrowserAuthError(
                "invalid authentication provider response",
                status=502,
            ) from exc
        finally:
            await response.aclose()
        return status, data if isinstance(data, dict) else {}

    async def _save_tokens(self, upstream_name: str, tokens: dict, previous=None) -> dict:
        access = str(tokens.get("access_token", ""))
        refresh = str(tokens.get("refresh_token") or (previous or {}).get("refresh_token", ""))
        if not access or not refresh:
            raise BrowserAuthError("OAuth token response was incomplete", status=502)
        account_id = _extract_account_id(tokens) or (previous or {}).get("account_id")
        expires_at = time.time() + float(tokens.get("expires_in", 3600) or 3600)
        await self._db.execute_commit(
            """INSERT INTO upstream_auth_tokens
               (upstream_name, access_token, refresh_token, expires_at, account_id, updated_at)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(upstream_name) DO UPDATE SET
                 access_token=excluded.access_token,
                 refresh_token=excluded.refresh_token,
                 expires_at=excluded.expires_at,
                 account_id=excluded.account_id,
                 updated_at=excluded.updated_at""",
            (upstream_name, access, refresh, expires_at, account_id, time.time()),
        )
        return {
            "access_token": access,
            "refresh_token": refresh,
            "expires_at": expires_at,
            "account_id": account_id,
        }

    async def _load_token(self, upstream_name: str) -> dict | None:
        async with await self._db.execute(
            "SELECT * FROM upstream_auth_tokens WHERE upstream_name = ?",
            (upstream_name,),
        ) as cur:
            row = await cur.fetchone()
        return dict(row) if row is not None else None

    # ── Flow helpers --------------------------------------------------

    def _check_upstream(self, upstream: Upstream) -> None:
        if upstream.profile != OPENAI_RESPONSES_PROFILE:
            raise BrowserAuthError("browser auth is only available for openai-responses")

    async def _ensure_callback_server(self) -> None:
        if self._callback_server is not None:
            return
        try:
            self._callback_server = await asyncio.start_server(
                self._handle_callback,
                "127.0.0.1",
                OAUTH_PORT,
            )
        except OSError as exc:
            raise BrowserAuthError(
                f"could not bind browser auth callback on localhost:{OAUTH_PORT}"
            ) from exc

    async def _stop_callback_server(self) -> None:
        if self._states or self._callback_server is None:
            return
        self._callback_server.close()
        await self._callback_server.wait_closed()
        self._callback_server = None

    async def _expire_flow(self, flow_id: str, state: str) -> None:
        await asyncio.sleep(FLOW_TTL)
        flow = self._flows.get(flow_id)
        if flow is not None and flow.status in {"pending", "exchanging"}:
            flow.status = "failed"
            flow.error = "authentication flow expired"
        self._states.pop(state, None)
        await self._stop_callback_server()

    def _schedule(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _public_flow(self, flow: _Flow) -> dict:
        return {
            "flow_id": flow.flow_id,
            "status": flow.status,
            "authorization_url": flow.authorization_url or None,
            "expires_at": flow.expires_at,
            "error": flow.error or None,
        }

    @staticmethod
    async def _write_callback(writer, status: int, body: str) -> None:
        reason = "OK" if status == 200 else "Bad Request" if status == 400 else "Server Error"
        raw = body.encode()
        writer.write(
            f"HTTP/1.1 {status} {reason}\r\n"
            "Content-Type: text/html; charset=utf-8\r\n"
            f"Content-Length: {len(raw)}\r\n"
            "Connection: close\r\n\r\n".encode() + raw
        )
        await writer.drain()


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _extract_account_id(tokens: dict) -> str | None:
    for key in ("id_token", "access_token"):
        token = tokens.get(key)
        if not token or not isinstance(token, str):
            continue
        parts = token.split(".")
        if len(parts) != 3:
            continue
        try:
            payload = base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4))
            claims = json.loads(payload)
        except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
            continue
        if not isinstance(claims, dict):
            continue
        auth_claims = claims.get("https://api.openai.com/auth")
        organizations = claims.get("organizations")
        account = (
            claims.get("chatgpt_account_id")
            or (auth_claims.get("chatgpt_account_id") if isinstance(auth_claims, dict) else None)
            or (
                organizations[0].get("id")
                if isinstance(organizations, list)
                and organizations
                and isinstance(organizations[0], dict)
                else None
            )
        )
        if account:
            return str(account)
    return None


def _html_page(title: str, message: str) -> str:
    safe_title = title.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    safe_message = message.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>{safe_title}</title></head><body>"
        f"<h1>{safe_title}</h1><p>{safe_message}</p></body></html>"
    )
