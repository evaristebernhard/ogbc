from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_token_id(token_id: Any) -> str | None:
    if token_id is None:
        return None
    value = str(token_id).strip().strip('"').strip("'")
    if not value:
        return None
    if value.startswith("0x"):
        return str(int(value, 16))
    if value.isdigit():
        return str(int(value))
    return value


def upsert_event(conn: sqlite3.Connection, payload: dict[str, Any]) -> int:
    now = _utc_now_iso()
    conn.execute(
        """
        INSERT INTO events(event_id, slug, title, description, neg_risk, active, closed, created_at, updated_at)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(slug) DO UPDATE SET
            event_id = excluded.event_id,
            title = excluded.title,
            description = excluded.description,
            neg_risk = excluded.neg_risk,
            active = excluded.active,
            closed = excluded.closed,
            updated_at = excluded.updated_at
        """,
        (
            payload.get("event_id"),
            payload["slug"],
            payload.get("title"),
            payload.get("description"),
            int(bool(payload.get("neg_risk", False))),
            int(bool(payload.get("active", True))),
            int(bool(payload.get("closed", False))),
            payload.get("created_at"),
            now,
        ),
    )
    row = conn.execute("SELECT id FROM events WHERE slug = ?", (payload["slug"],)).fetchone()
    conn.commit()
    return int(row["id"])


def upsert_market(conn: sqlite3.Connection, payload: dict[str, Any]) -> int:
    now = _utc_now_iso()
    conn.execute(
        """
        INSERT INTO markets(
            event_id, slug, title, description, condition_id, question_id, oracle,
            collateral_token, yes_token_id, no_token_id, enable_neg_risk,
            status, created_at, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(slug) DO UPDATE SET
            event_id = excluded.event_id,
            title = excluded.title,
            description = excluded.description,
            condition_id = excluded.condition_id,
            question_id = excluded.question_id,
            oracle = excluded.oracle,
            collateral_token = excluded.collateral_token,
            yes_token_id = excluded.yes_token_id,
            no_token_id = excluded.no_token_id,
            enable_neg_risk = excluded.enable_neg_risk,
            status = excluded.status,
            updated_at = excluded.updated_at
        """,
        (
            payload.get("event_id"),
            payload["slug"],
            payload.get("title"),
            payload.get("description"),
            payload.get("condition_id"),
            payload.get("question_id"),
            payload.get("oracle"),
            payload.get("collateral_token"),
            normalize_token_id(payload.get("yes_token_id")),
            normalize_token_id(payload.get("no_token_id")),
            int(bool(payload.get("enable_neg_risk", False))),
            payload.get("status", "unknown"),
            payload.get("created_at"),
            now,
        ),
    )
    row = conn.execute("SELECT id FROM markets WHERE slug = ?", (payload["slug"],)).fetchone()
    conn.commit()
    return int(row["id"])


def insert_trades(conn: sqlite3.Connection, trades: list[dict[str, Any]]) -> int:
    if not trades:
        return 0

    before = conn.total_changes
    conn.executemany(
        """
        INSERT OR IGNORE INTO trades(
            market_id, tx_hash, log_index, block_number, block_hash, timestamp,
            maker, taker, side, outcome, price, size, token_id, maker_asset_id,
            taker_asset_id, maker_amount_filled, taker_amount_filled, exchange_address
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                t["market_id"],
                t["tx_hash"],
                t["log_index"],
                t.get("block_number"),
                t.get("block_hash"),
                t.get("timestamp"),
                t.get("maker"),
                t.get("taker"),
                t.get("side"),
                t.get("outcome"),
                str(t.get("price", "0")),
                str(t.get("size", "0")),
                normalize_token_id(t.get("token_id")),
                str(t.get("maker_asset_id")) if t.get("maker_asset_id") is not None else None,
                str(t.get("taker_asset_id")) if t.get("taker_asset_id") is not None else None,
                str(t.get("maker_amount_filled")) if t.get("maker_amount_filled") is not None else None,
                str(t.get("taker_amount_filled")) if t.get("taker_amount_filled") is not None else None,
                t.get("exchange_address"),
            )
            for t in trades
        ],
    )
    conn.commit()
    return conn.total_changes - before


def update_sync_state(conn: sqlite3.Connection, key: str, last_block: int) -> None:
    conn.execute(
        """
        INSERT INTO sync_state(key, last_block, updated_at)
        VALUES(?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET
            last_block = excluded.last_block,
            updated_at = CURRENT_TIMESTAMP
        """,
        (key, int(last_block)),
    )
    conn.commit()


def get_sync_state(conn: sqlite3.Connection, key: str) -> int | None:
    row = conn.execute("SELECT last_block FROM sync_state WHERE key = ?", (key,)).fetchone()
    return int(row["last_block"]) if row else None


def fetch_event_by_slug(conn: sqlite3.Connection, slug: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM events WHERE slug = ?", (slug,)).fetchone()
    return dict(row) if row else None


def fetch_markets_for_event(conn: sqlite3.Connection, event_slug: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT m.*
        FROM markets m
        JOIN events e ON m.event_id = e.id
        WHERE e.slug = ?
        ORDER BY m.id ASC
        """,
        (event_slug,),
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_market_by_slug(conn: sqlite3.Connection, slug: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM markets WHERE slug = ?", (slug,)).fetchone()
    return dict(row) if row else None


def fetch_market_by_token_id(conn: sqlite3.Connection, token_id: Any) -> dict[str, Any] | None:
    normalized = normalize_token_id(token_id)
    if normalized is None:
        return None
    row = conn.execute(
        "SELECT * FROM markets WHERE yes_token_id = ? OR no_token_id = ?",
        (normalized, normalized),
    ).fetchone()
    return dict(row) if row else None


def _build_trade_where(from_block: int | None, to_block: int | None) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if from_block is not None:
        clauses.append("block_number >= ?")
        params.append(from_block)
    if to_block is not None:
        clauses.append("block_number <= ?")
        params.append(to_block)
    if not clauses:
        return "", params
    return " AND " + " AND ".join(clauses), params


def fetch_trades_for_market(
    conn: sqlite3.Connection,
    market_id: int,
    limit: int,
    offset: int,
    from_block: int | None = None,
    to_block: int | None = None,
) -> list[dict[str, Any]]:
    where_extra, params = _build_trade_where(from_block, to_block)
    rows = conn.execute(
        f"""
        SELECT *
        FROM trades
        WHERE market_id = ? {where_extra}
        ORDER BY block_number DESC, log_index DESC
        LIMIT ? OFFSET ?
        """,
        [market_id, *params, int(limit), int(offset)],
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_trades_for_token(
    conn: sqlite3.Connection,
    token_id: Any,
    limit: int,
    offset: int,
    from_block: int | None = None,
    to_block: int | None = None,
) -> list[dict[str, Any]]:
    normalized = normalize_token_id(token_id)
    where_extra, params = _build_trade_where(from_block, to_block)
    rows = conn.execute(
        f"""
        SELECT *
        FROM trades
        WHERE token_id = ? {where_extra}
        ORDER BY block_number DESC, log_index DESC
        LIMIT ? OFFSET ?
        """,
        [normalized, *params, int(limit), int(offset)],
    ).fetchall()
    return [dict(r) for r in rows]


def dump_rows_json(rows: list[dict[str, Any]]) -> str:
    return json.dumps(rows, ensure_ascii=False, indent=2)
