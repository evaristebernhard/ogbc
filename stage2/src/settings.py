from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None


@dataclass(frozen=True)
class Settings:
    rpc_url: str
    db_path: str
    gamma_api_base: str
    polymarket_exchange: str
    polymarket_neg_risk_exchange: str
    polymarket_ctf: str
    collateral_usdc: str
    log_chunk_size: int
    request_timeout: int
    max_retries: int
    retry_base_delay: float


def _as_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


def _as_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    return float(value)


def load_settings(db_override: str | None = None) -> Settings:
    load_dotenv()

    db_path = db_override or os.getenv("DB_PATH", "./data/indexer.db")

    return Settings(
        rpc_url=os.getenv("RPC_URL", "https://polygon-rpc.com"),
        db_path=db_path,
        gamma_api_base=os.getenv("GAMMA_API_BASE", "https://gamma-api.polymarket.com"),
        polymarket_exchange=os.getenv(
            "POLYMARKET_EXCHANGE", "0x4bFb41d5B3570DeFd03C39a9A4d8dE6bd8B8982E"
        ),
        polymarket_neg_risk_exchange=os.getenv(
            "POLYMARKET_NEG_RISK_EXCHANGE",
            "0xC5D563A36AE78145C45A50134D48A1215220E0A8",
        ),
        polymarket_ctf=os.getenv("POLYMARKET_CTF", "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"),
        collateral_usdc=os.getenv(
            "COLLATERAL_USDC", "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        ),
        log_chunk_size=_as_int("LOG_CHUNK_SIZE", 4000),
        request_timeout=_as_int("REQUEST_TIMEOUT", 20),
        max_retries=_as_int("MAX_RETRIES", 4),
        retry_base_delay=_as_float("RETRY_BASE_DELAY", 1.5),
    )


def ensure_db_directory(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
