from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from web3 import Web3

from src.db.store import (
    fetch_market_by_token_id,
    get_sync_state,
    insert_trades,
    normalize_token_id,
    update_sync_state,
    upsert_event,
    upsert_market,
)
from src.gamma.client import (
    GammaClient,
    normalize_event_payload,
    normalize_market_payload,
)
from src.indexer.trade_decoder import (
    ORDER_FILLED_EVENT_SIGNATURE,
    decode_order_filled_log,
)
from src.settings import Settings


def _to_iso_utc(unix_ts: int) -> str:
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).replace(tzinfo=None).isoformat()


def _int_token(value: Any) -> str | None:
    return normalize_token_id(value)


def discover_markets(settings: Settings, conn: Any, event_slug: str | None) -> dict[str, Any]:
    if not event_slug:
        return {"event_slug": None, "event_id": None, "markets": []}

    gamma = GammaClient(settings)
    event_obj, markets = gamma.get_markets_for_event(event_slug)

    event_payload = normalize_event_payload(event_obj)
    if not event_payload.get("slug"):
        event_payload["slug"] = event_slug

    event_row_id = upsert_event(conn, event_payload)

    discovered_market_ids: list[int] = []
    for market in markets:
        normalized = normalize_market_payload(
            market=market,
            event_row_id=event_row_id,
            event_neg_risk=bool(event_payload.get("neg_risk", False)),
            default_collateral_token=settings.collateral_usdc,
        )
        if normalized is None:
            continue
        market_row_id = upsert_market(conn, normalized)
        discovered_market_ids.append(market_row_id)

    return {
        "event_slug": event_payload.get("slug"),
        "event_id": event_row_id,
        "markets": discovered_market_ids,
    }


def _ensure_market_cached(
    settings: Settings,
    conn: Any,
    token_id: str,
    event_slug: str | None,
    market_discovery_attempted: bool,
) -> tuple[dict[str, Any] | None, bool]:
    market = fetch_market_by_token_id(conn, token_id)
    if market:
        return market, market_discovery_attempted

    if event_slug and not market_discovery_attempted:
        discover_markets(settings, conn, event_slug)
        market_discovery_attempted = True
        market = fetch_market_by_token_id(conn, token_id)

    return market, market_discovery_attempted


def _get_block_timestamp_iso(w3: Web3, cache: dict[int, str], block_number: int) -> str:
    if block_number not in cache:
        block = w3.eth.get_block(block_number)
        cache[block_number] = _to_iso_utc(int(block["timestamp"]))
    return cache[block_number]


def _collect_logs(
    w3: Web3,
    addresses: list[str],
    from_block: int,
    to_block: int,
    chunk_size: int,
) -> list[dict[str, Any]]:
    topic0 = Web3.keccak(text=ORDER_FILLED_EVENT_SIGNATURE).hex()
    all_logs: list[dict[str, Any]] = []

    cursor = from_block
    while cursor <= to_block:
        end = min(cursor + chunk_size - 1, to_block)
        payload = {
            "address": addresses,
            "topics": [topic0],
            "fromBlock": cursor,
            "toBlock": end,
        }
        logs = w3.eth.get_logs(payload)
        all_logs.extend(logs)
        cursor = end + 1

    return all_logs


def run_indexer(
    w3: Web3,
    conn: Any,
    settings: Settings,
    from_block: int,
    to_block: int,
    exchange_address: str,
    neg_risk_exchange: str,
    ctf_address: str,
    exchange_abi: list[dict[str, Any]] | None,
    ctf_abi: list[dict[str, Any]] | None,
    include_ctf: bool = False,
    include_exchange: bool = True,
    include_neg_risk: bool = True,
    event_slug: str | None = None,
) -> dict[str, Any]:
    del ctf_address, exchange_abi, ctf_abi, include_ctf

    discovery_result = discover_markets(settings, conn, event_slug)

    addresses: list[str] = []
    if include_exchange and exchange_address:
        addresses.append(Web3.to_checksum_address(exchange_address))
    if include_neg_risk and neg_risk_exchange:
        addresses.append(Web3.to_checksum_address(neg_risk_exchange))

    if not addresses:
        raise ValueError("At least one exchange address must be enabled")

    logs = _collect_logs(
        w3=w3,
        addresses=addresses,
        from_block=from_block,
        to_block=to_block,
        chunk_size=max(1, int(settings.log_chunk_size)),
    )

    timestamp_cache: dict[int, str] = {}
    market_discovery_attempted = False
    to_insert: list[dict[str, Any]] = []

    for raw_log in logs:
        decoded = decode_order_filled_log(raw_log)

        token_id = _int_token(decoded["token_id"])
        if token_id is None:
            continue

        market, market_discovery_attempted = _ensure_market_cached(
            settings=settings,
            conn=conn,
            token_id=token_id,
            event_slug=event_slug,
            market_discovery_attempted=market_discovery_attempted,
        )

        if market is None:
            continue

        outcome = "UNKNOWN"
        if token_id == _int_token(market.get("yes_token_id")):
            outcome = "YES"
        elif token_id == _int_token(market.get("no_token_id")):
            outcome = "NO"

        decoded["market_id"] = int(market["id"])
        decoded["outcome"] = outcome
        decoded["timestamp"] = _get_block_timestamp_iso(w3, timestamp_cache, int(decoded["block_number"]))

        to_insert.append(decoded)

    inserted = insert_trades(conn, to_insert)
    update_sync_state(conn, "trade_sync", int(to_block))

    sample_trades = [
        {
            "tx_hash": item["tx_hash"],
            "log_index": item["log_index"],
            "block_number": item["block_number"],
            "timestamp": item["timestamp"],
            "side": item["side"],
            "outcome": item["outcome"],
            "price": item["price"],
            "size": item["size"],
            "token_id": item["token_id"],
        }
        for item in to_insert[:5]
    ]

    market_id = sample_trades and to_insert[0].get("market_id")

    return {
        "from_block": int(from_block),
        "to_block": int(to_block),
        "inserted_trades": int(inserted),
        "market_slug": event_slug,
        "market_id": int(market_id) if market_id else None,
        "sample_trades": sample_trades,
        "db_path": settings.db_path,
        "market_discovery": discovery_result,
    }


def determine_block_range(
    w3: Web3,
    conn: Any,
    from_block: int | None,
    to_block: int | None,
    tx_block: int | None,
) -> tuple[int, int]:
    if tx_block is not None:
        if from_block is None:
            from_block = tx_block
        if to_block is None:
            to_block = tx_block

    if from_block is not None and to_block is None:
        to_block = from_block

    if from_block is None:
        last = get_sync_state(conn, "trade_sync")
        if last is not None:
            from_block = last + 1
        else:
            head = int(w3.eth.block_number)
            from_block = max(0, head - 10)

    if to_block is None:
        to_block = int(w3.eth.block_number)

    if from_block > to_block:
        raise ValueError("from_block must be <= to_block")

    return int(from_block), int(to_block)
