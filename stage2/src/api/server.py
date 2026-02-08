from __future__ import annotations

import argparse
import sqlite3
from typing import Any

from src.db.store import (
    fetch_event_by_slug,
    fetch_market_by_slug,
    fetch_markets_for_event,
    fetch_trades_for_market,
    fetch_trades_for_token,
)


def create_app(db_path: str) -> Any:
    try:
        from fastapi import FastAPI, HTTPException, Query
    except ImportError as exc:
        raise RuntimeError("Missing dependency: fastapi. Run `pip install -r requirements.txt`.") from exc

    app = FastAPI(title="Polymarket Stage2 API", version="1.0.0")

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @app.get("/events/{slug}")
    def get_event(slug: str) -> dict[str, Any]:
        conn = _connect()
        try:
            event = fetch_event_by_slug(conn, slug)
            if event is None:
                raise HTTPException(status_code=404, detail="event not found")
            return event
        finally:
            conn.close()

    @app.get("/events/{slug}/markets")
    def get_event_markets(slug: str) -> list[dict[str, Any]]:
        conn = _connect()
        try:
            return fetch_markets_for_event(conn, slug)
        finally:
            conn.close()

    @app.get("/markets/{slug}")
    def get_market(slug: str) -> dict[str, Any]:
        conn = _connect()
        try:
            market = fetch_market_by_slug(conn, slug)
            if market is None:
                raise HTTPException(status_code=404, detail="market not found")
            return {
                "market_id": market["id"],
                "slug": market["slug"],
                "condition_id": market["condition_id"],
                "question_id": market["question_id"],
                "oracle": market["oracle"],
                "collateral_token": market["collateral_token"],
                "yes_token_id": market["yes_token_id"],
                "no_token_id": market["no_token_id"],
                "status": market["status"],
            }
        finally:
            conn.close()

    @app.get("/markets/{slug}/trades")
    def get_market_trades(
        slug: str,
        limit: int = Query(default=100, ge=1, le=1000),
        cursor: int = Query(default=0, ge=0),
        fromBlock: int | None = Query(default=None),
        toBlock: int | None = Query(default=None),
    ) -> list[dict[str, Any]]:
        conn = _connect()
        try:
            market = fetch_market_by_slug(conn, slug)
            if market is None:
                raise HTTPException(status_code=404, detail="market not found")
            return fetch_trades_for_market(
                conn=conn,
                market_id=int(market["id"]),
                limit=limit,
                offset=cursor,
                from_block=fromBlock,
                to_block=toBlock,
            )
        finally:
            conn.close()

    @app.get("/tokens/{token_id}/trades")
    def get_token_trades(
        token_id: str,
        limit: int = Query(default=100, ge=1, le=1000),
        cursor: int = Query(default=0, ge=0),
        fromBlock: int | None = Query(default=None),
        toBlock: int | None = Query(default=None),
    ) -> list[dict[str, Any]]:
        conn = _connect()
        try:
            return fetch_trades_for_token(
                conn=conn,
                token_id=token_id,
                limit=limit,
                offset=cursor,
                from_block=fromBlock,
                to_block=toBlock,
            )
        finally:
            conn.close()

    return app


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage2 API server")
    parser.add_argument("--db", type=str, required=True, help="SQLite db path")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    return parser


def main() -> None:
    args = _parser().parse_args()

    try:
        import uvicorn
    except ImportError as exc:
        raise RuntimeError("Missing dependency: uvicorn. Run `pip install -r requirements.txt`.") from exc

    app = create_app(args.db)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
