from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Callable
from pathlib import Path

import aiosqlite

from ..core.models import RequestRecord

log = logging.getLogger("proxen.telemetry")

_SCHEMA = Path(__file__).parent / "schema.sql"

_COST_EXPR = (
    "round("
    "MAX(0, r.input_tokens - r.cached_input_tokens) / 1000000.0 * COALESCE(pm.input_per_1m, 0)"
    " + MAX(0, r.cached_input_tokens) / 1000000.0 * COALESCE(pm.cached_input_per_1m, 0)"
    " + MAX(0, r.output_tokens) / 1000000.0 * COALESCE(pm.output_per_1m, 0)"
    ", 6)"
)

_CTX_JOIN = (
    "LEFT JOIN request_contexts rc ON rc.id = r.ctx_id "
    "LEFT JOIN request_tags mt ON mt.tag = rc.model_tag "
    "LEFT JOIN request_tags ut ON ut.tag = rc.upstream_tag "
    "LEFT JOIN request_tags kt ON kt.tag = rc.key_tag "
    "LEFT JOIN models pm ON pm.id = mt.name"
)

_FLAG_STREAM = 1
_FLAG_DISCONNECT = 2
_FLAG_DROPPED = 4
_FLAG_REVIEW = 8

_FLAGS_BOOLS = (
    (_FLAG_STREAM, "stream"),
    (_FLAG_DISCONNECT, "client_disconnect"),
    (_FLAG_DROPPED, "upstream_dropped"),
    (_FLAG_REVIEW, "needs_review"),
)


def _encode_flags(rec: RequestRecord) -> int:
    return sum(getattr(rec, name) * flag for flag, name in _FLAGS_BOOLS)


def _flags_select(tbl: str = "r") -> str:
    return ", ".join(f"({tbl}.flags & {flag}) AS {name}" for flag, name in _FLAGS_BOOLS)


_REQUESTS_COLS = "timestamp, ctx_id, ttft_ms, tps_centi, input_tokens, cached_input_tokens, output_tokens, status, duration_ms, flags"
_REQUESTS_PLACEHOLDERS = ",".join("?" for _ in _REQUESTS_COLS.split(","))

# Generation phase (ms) per row, floored to 1s to mirror speed_metrics'
# _GEN_TIME_MIN as a rounding guard (a 1.0001s gen_time can round to 999ms).
# Only streaming rows with gen_time >= _GEN_TIME_MIN reach the aggregate --
# shorter streams record NULL tps_centi and are filtered out below. The
# floor prevents any residual rounding noise from inflating the weighted
# average, keeping the aggregate consistent with the per-row tps.
_GEN_MS = "r.duration_ms - r.ttft_ms"
_FLOORED_GEN_MS = f"MAX({_GEN_MS}, 1000)"

# Per-row assumed generation time (ms), matching speed_metrics' denominator:
# non-streaming rows (gen_time == 0, i.e. ttft == duration) use the whole
# end-to-end duration; streaming rows use the floored generation phase.
_ROW_GEN_MS = (
    f"CASE WHEN {_GEN_MS} <= 0 THEN r.duration_ms ELSE {_FLOORED_GEN_MS} END"
)

# Weighted throughput: SUM(output_tokens) / SUM(gen_time) over non-error
# rows with a measurable rate (tps_centi > 0, which excludes NULL short-stream
# rows and zero/error rows). Non-streaming rows contribute their end-to-end
# duration, streaming rows their (floored) generation phase. Unlike AVG(tps),
# this is true throughput -- invariant to the traffic mix of short vs long
# responses -- and is robust to individual outlier rows.
_WEIGHTED_TPS = (
    "COALESCE("
    f"SUM(CASE WHEN r.tps_centi > 0 THEN r.output_tokens END) * 1000.0 / "
    f"NULLIF(SUM(CASE WHEN r.tps_centi > 0 THEN {_ROW_GEN_MS} END), 0)"
    ", 0)"
)


class Database:
    def __init__(self, path: str) -> None:
        self._path = path
        self._db: aiosqlite.Connection | None = None

    def _conn(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("Database not initialized")
        return self._db

    async def init(self) -> None:
        preexisting = Path(self._path).exists()
        self._db = await aiosqlite.connect(self._path)
        if not preexisting:
            try:
                os.chmod(self._path, 0o600)
            except OSError:
                pass
        conn = self._conn()
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = aiosqlite.Row
        await conn.executescript(_SCHEMA.read_text())
        await conn.commit()

    async def close(self) -> None:
        if self._db is not None:
            try:
                await self._db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                pass
            await self._db.close()
            self._db = None

    async def execute(self, sql: str, params=()) -> aiosqlite.Cursor:
        return await self._conn().execute(sql, params)

    async def execute_commit(self, sql: str, params=()) -> aiosqlite.Cursor:
        cur = await self._conn().execute(sql, params)
        await self._conn().commit()
        return cur

    async def executemany_commit(self, sql: str, params_seq) -> None:
        await self._conn().executemany(sql, params_seq)
        await self._conn().commit()

    async def commit(self) -> None:
        await self._conn().commit()

    async def _resolve_tags(self, names: set[str]) -> dict[str, int]:
        if not names:
            return {}
        conn = self._conn()
        await conn.executemany(
            "INSERT OR IGNORE INTO request_tags(name) VALUES (?)",
            [(name,) for name in names],
        )
        await conn.commit()
        placeholders = ",".join("?" for _ in names)
        async with await self.execute(
            f"SELECT name, tag FROM request_tags WHERE name IN ({placeholders})", tuple(names)
        ) as cur:
            rows = await cur.fetchall()
        return {row[0]: row[1] for row in rows}

    async def _resolve_contexts(
        self, entries: list[tuple[int | None, int | None, int | None]]
    ) -> dict[tuple[int | None, int | None, int | None], int]:
        if not entries:
            return {}
        conn = self._conn()
        await conn.executemany(
            "INSERT OR IGNORE INTO request_contexts(model_tag, upstream_tag, key_tag) VALUES (?, ?, ?)",
            entries,
        )
        await conn.commit()
        placeholders = ",".join("(?,?,?)" for _ in entries)
        flat = [val for entry in entries for val in entry]
        async with await self.execute(
            f"SELECT id, model_tag, upstream_tag, key_tag FROM request_contexts "
            f"WHERE (model_tag, upstream_tag, key_tag) IN ({placeholders})", flat
        ) as cur:
            rows = await cur.fetchall()
        return {(row[1], row[2], row[3]): row[0] for row in rows}

    async def insert_records(self, records: list[RequestRecord]) -> None:
        if not records:
            return
        model_tags = await self._resolve_tags({rec.model for rec in records})
        upstream_tags = await self._resolve_tags({rec.upstream for rec in records if rec.upstream})
        key_tags = await self._resolve_tags({rec.key_id for rec in records if rec.key_id})

        ctx_keys = [
            (model_tags.get(rec.model), upstream_tags.get(rec.upstream) if rec.upstream else None, key_tags.get(rec.key_id) if rec.key_id else None)
            for rec in records
        ]
        ctx_ids = await self._resolve_contexts(ctx_keys)

        rows = [
            (
                int(rec.timestamp), ctx_ids[ctx_key],
                int(round(rec.ttft * 1000)),
                int(round(rec.tps * 100)) if rec.tps is not None else None,
                rec.input_tokens, rec.cached_input_tokens, rec.output_tokens,
                rec.status, int(round(rec.duration * 1000)), _encode_flags(rec),
            )
            for rec, ctx_key in zip(records, ctx_keys)
        ]
        await self._conn().executemany(
            f"INSERT INTO requests ({_REQUESTS_COLS}) VALUES ({_REQUESTS_PLACEHOLDERS})", rows
        )
        await self._conn().commit()

    async def totals(self) -> dict:
        async with await self.execute(
            """SELECT COUNT(*) AS total_requests,
                      COALESCE(SUM(input_tokens), 0) AS total_input,
                      COALESCE(SUM(cached_input_tokens), 0) AS total_cached,
                      COALESCE(SUM(output_tokens), 0) AS total_output
               FROM requests"""
        ) as cur:
            row = await cur.fetchone()
        return dict(row)

    async def total_cost(self) -> float:
        async with await self.execute(
            f"SELECT COALESCE(SUM({_COST_EXPR}), 0) FROM requests r {_CTX_JOIN}"
        ) as cur:
            row = await cur.fetchone()
        return row[0]

    async def _since(self, sql: str, days: int, params=()) -> list[dict]:
        async with await self.execute(sql, (int(time.time()) - days * 86400, *params)) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def recent(self, limit: int = 50) -> list[dict]:
        async with await self.execute(
            f"""SELECT r.id, r.timestamp, mt.name AS model, ut.name AS upstream,
                       kt.name AS key_id,
                       r.ttft_ms / 1000.0 AS ttft, r.tps_centi / 100.0 AS tps,
                       r.input_tokens, r.cached_input_tokens, r.output_tokens,
                       {_COST_EXPR} AS cost,
                       r.status, r.duration_ms / 1000.0 AS duration,
                       {_flags_select()}
                FROM requests r {_CTX_JOIN}
                ORDER BY r.id DESC LIMIT ?""", (limit,)
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def tps_ttft_24h(self) -> list[dict]:
        # 15-min buckets: at ~65 req/h this averages ~16 requests per bucket,
        # enough for a stable weighted average while keeping the last point
        # at most 15 min stale (vs up to 60 min with hourly buckets).
        rows = await self._since(
            f"""SELECT CAST(r.timestamp / 900 AS INTEGER) * 900 AS bucket,
                      {_WEIGHTED_TPS} AS tps,
                      AVG(r.ttft_ms) / 1000.0 AS ttft
               FROM requests r WHERE r.timestamp >= ? AND r.status < 400
               GROUP BY bucket ORDER BY bucket ASC""",
            1,
        )
        return [{"timestamp": r["bucket"], "tps": r["tps"] or 0, "ttft": r["ttft"] or 0} for r in rows]

    async def daily_tokens(self, days: int = 7) -> list[dict]:
        return await self._since(
            """SELECT date(timestamp, 'unixepoch') AS day,
                      SUM(input_tokens) AS input_tokens,
                      SUM(cached_input_tokens) AS cached_input_tokens,
                      SUM(output_tokens) AS output_tokens
               FROM requests WHERE timestamp >= ?
               GROUP BY day ORDER BY day ASC""",
            days,
        )

    async def daily_requests(self, days: int = 7) -> list[dict]:
        return await self._since(
            """SELECT date(timestamp, 'unixepoch') AS day, COUNT(*) AS requests
               FROM requests WHERE timestamp >= ?
               GROUP BY day ORDER BY day ASC""",
            days,
        )

    async def model_breakdown(self, days: int = 30) -> list[dict]:
        return await self._since(
            f"""SELECT mt.name AS model, COUNT(*) AS requests,
                      round(COALESCE(SUM({_COST_EXPR}), 0), 6) AS total_cost,
                      COALESCE(SUM(r.input_tokens), 0) AS total_input,
                      COALESCE(SUM(r.cached_input_tokens), 0) AS total_cached,
                      COALESCE(SUM(r.output_tokens), 0) AS total_output,
                      {_WEIGHTED_TPS} AS avg_tps,
                      COALESCE(AVG(r.ttft_ms), 0) / 1000.0 AS avg_ttft,
                      COALESCE(AVG(r.duration_ms), 0) / 1000.0 AS avg_duration
               FROM requests r {_CTX_JOIN}
               WHERE r.timestamp >= ?
               GROUP BY rc.model_tag ORDER BY requests DESC""",
            days,
        )

    async def key_breakdown(self, days: int = 30) -> list[dict]:
        return await self._since(
            f"""SELECT kt.name AS key_id, COUNT(*) AS requests,
                      round(COALESCE(SUM({_COST_EXPR}), 0), 6) AS total_cost,
                      COALESCE(SUM(r.input_tokens), 0) AS total_input,
                      COALESCE(SUM(r.cached_input_tokens), 0) AS total_cached,
                      COALESCE(SUM(r.output_tokens), 0) AS total_output,
                      {_WEIGHTED_TPS} AS avg_tps,
                      COALESCE(AVG(r.ttft_ms), 0) / 1000.0 AS avg_ttft
               FROM requests r {_CTX_JOIN}
               WHERE r.timestamp >= ?
               GROUP BY rc.key_tag ORDER BY total_cost DESC""",
            days,
        )

    async def error_stats(self, days: int = 30) -> list[dict]:
        return await self._since(
            """SELECT mt.name AS model, r.status, COUNT(*) AS count
               FROM requests r
               LEFT JOIN request_contexts rc ON rc.id = r.ctx_id
               LEFT JOIN request_tags mt ON mt.tag = rc.model_tag
               WHERE r.timestamp >= ? AND r.status >= 400
               GROUP BY rc.model_tag, r.status ORDER BY count DESC""",
            days,
        )

    async def daily_errors(self, days: int = 30) -> list[dict]:
        return await self._since(
            f"""SELECT date(timestamp, 'unixepoch') AS day,
                      SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) AS errors_5xx,
                      SUM(CASE WHEN status >= 400 AND status < 500 THEN 1 ELSE 0 END) AS errors_4xx,
                      SUM(CASE WHEN flags & {_FLAG_DISCONNECT} THEN 1 ELSE 0 END) AS cancelled,
                      SUM(CASE WHEN flags & {_FLAG_DROPPED} THEN 1 ELSE 0 END) AS dropped,
                      COUNT(*) AS total
               FROM requests WHERE timestamp >= ?
               GROUP BY day ORDER BY day ASC""",
            days,
        )

    async def daily_cost(self, days: int = 30) -> list[dict]:
        return await self._since(
            f"""SELECT date(r.timestamp, 'unixepoch') AS day,
                      round(COALESCE(SUM({_COST_EXPR}), 0), 6) AS cost
               FROM requests r {_CTX_JOIN}
               WHERE r.timestamp >= ?
               GROUP BY day ORDER BY day ASC""",
            days,
        )


class TelemetryWriter:
    """Drains an :class:`asyncio.Queue` of records into the DB in batches.

    Proxy code only does a non-blocking `put_nowait`; disk I/O happens here,
    off the hot path. Records are flushed immediately after draining so the
    dashboard always sees the latest data. Running totals are maintained in
    memory so the dashboard never queries the DB for aggregates.

    Set `on_flush` to a callback (e.g. `broadcaster.mark_dirty`) to
    notify the dashboard that new records are available.
    """

    def __init__(
        self,
        db: Database,
        batch_size: int = 50,
        flush_interval: float = 0.5,
        max_queue: int = 10000,
    ) -> None:
        self._db = db
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._queue: asyncio.Queue[RequestRecord] = asyncio.Queue(maxsize=max_queue)
        self.dropped: int = 0
        self.on_flush: Callable[[], None] | None = None
        self._consecutive_failures = 0
        self._max_consecutive_failures = 10
        self._totals: dict = {
            "total_requests": 0,
            "total_input": 0,
            "total_cached": 0,
            "total_output": 0,
        }

    async def init_totals(self) -> None:
        totals = await self._db.totals()
        self._totals.update(totals)

    @property
    def totals(self) -> dict:
        return dict(self._totals)

    def enqueue(self, record: RequestRecord) -> None:
        try:
            self._queue.put_nowait(record)
        except asyncio.QueueFull:
            self.dropped += 1

    async def run(self) -> None:
        batch: list[RequestRecord] = []
        while True:
            try:
                try:
                    record = await asyncio.wait_for(
                        self._queue.get(), self._flush_interval
                    )
                except asyncio.TimeoutError:
                    if batch:
                        if await self._flush_and_update(batch):
                            batch.clear()
                            self._consecutive_failures = 0
                        else:
                            self._consecutive_failures += 1
                            if self._consecutive_failures > self._max_consecutive_failures:
                                log.error(
                                    "dropping %d telemetry records after %d consecutive flush failures",
                                    len(batch), self._consecutive_failures,
                                )
                                batch.clear()
                                self._consecutive_failures = 0
                    if self.dropped:
                        log.warning(
                            "telemetry queue overflow: %d records dropped",
                            self.dropped,
                        )
                        self.dropped = 0
                    continue

                batch.append(record)
                while len(batch) < self._batch_size:
                    try:
                        batch.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                if await self._flush_and_update(batch):
                    batch.clear()
                    self._consecutive_failures = 0
                else:
                    self._consecutive_failures += 1
                    if self._consecutive_failures > self._max_consecutive_failures:
                        log.error(
                            "dropping %d telemetry records after %d consecutive flush failures",
                            len(batch), self._consecutive_failures,
                        )
                        batch.clear()
                        self._consecutive_failures = 0
            except Exception:
                log.exception("telemetry writer error")
                await asyncio.sleep(1)

    def _update_totals(self, record: RequestRecord) -> None:
        self._totals["total_requests"] += 1
        self._totals["total_input"] += record.input_tokens
        self._totals["total_cached"] += record.cached_input_tokens
        self._totals["total_output"] += record.output_tokens

    async def _flush_and_update(self, batch: list[RequestRecord]) -> bool:
        if not await self._flush(batch):
            return False
        for r in batch:
            self._update_totals(r)
        return True

    async def _flush(self, batch: list[RequestRecord]) -> bool:
        try:
            await self._db.insert_records(batch)
        except Exception:
            log.exception("db insert failed (batch of %d)", len(batch))
            return False
        if self.on_flush:
            try:
                self.on_flush()
            except Exception:
                log.debug("on_flush callback error", exc_info=True)
        return True
