from __future__ import annotations

import sqlite3
from pathlib import Path


def init_db(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _create_schema(conn)
    return conn


def _create_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE,
            slug TEXT UNIQUE NOT NULL,
            title TEXT,
            description TEXT,
            neg_risk INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            closed INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER,
            slug TEXT UNIQUE NOT NULL,
            title TEXT,
            description TEXT,
            condition_id TEXT,
            question_id TEXT,
            oracle TEXT,
            collateral_token TEXT,
            yes_token_id TEXT,
            no_token_id TEXT,
            enable_neg_risk INTEGER DEFAULT 0,
            status TEXT,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY (event_id) REFERENCES events(id)
        )
        """
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_markets_condition_id ON markets(condition_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_markets_yes_token_id ON markets(yes_token_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_markets_no_token_id ON markets(no_token_id)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id INTEGER NOT NULL,
            tx_hash TEXT NOT NULL,
            log_index INTEGER NOT NULL,
            block_number INTEGER,
            block_hash TEXT,
            timestamp TEXT,
            maker TEXT,
            taker TEXT,
            side TEXT,
            outcome TEXT,
            price TEXT,
            size TEXT,
            token_id TEXT,
            maker_asset_id TEXT,
            taker_asset_id TEXT,
            maker_amount_filled TEXT,
            taker_amount_filled TEXT,
            exchange_address TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (market_id) REFERENCES markets(id),
            UNIQUE(tx_hash, log_index)
        )
        """
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_market_timestamp ON trades(market_id, timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_token_id ON trades(token_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_block_number ON trades(block_number)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_state (
            key TEXT PRIMARY KEY,
            last_block INTEGER NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    conn.commit()
