from __future__ import annotations

from decimal import Decimal, getcontext
from typing import Any

getcontext().prec = 40

USDC_SCALE = Decimal("1000000")
ORDER_FILLED_EVENT_SIGNATURE = "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"


def _topic_to_address(topic_hex: str) -> str:
    compact = topic_hex[2:] if topic_hex.startswith("0x") else topic_hex
    return "0x" + compact[-40:]


def _hex_to_int(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, bytes):
        return int.from_bytes(value, byteorder="big")
    if hasattr(value, "hex"):
        return int(value.hex(), 16)
    if value.startswith("0x"):
        return int(value, 16)
    return int(value)


def _chunk_data_words(data_hex: str) -> list[int]:
    data = data_hex[2:] if data_hex.startswith("0x") else data_hex
    if len(data) % 64 != 0:
        raise ValueError("unexpected log data length")
    return [int(data[i : i + 64], 16) for i in range(0, len(data), 64)]


def decode_order_filled_log(log: dict[str, Any]) -> dict[str, Any]:
    topics = log.get("topics", [])
    if len(topics) < 3:
        raise ValueError("OrderFilled requires at least 3 topics")

    words = _chunk_data_words(log.get("data", "0x"))
    if len(words) < 6:
        raise ValueError("OrderFilled data payload too short")

    order_hash_word = words[0]
    maker_asset_id = words[1]
    taker_asset_id = words[2]
    maker_amount_filled = words[3]
    taker_amount_filled = words[4]
    fee = words[5]

    maker = _topic_to_address(topics[1])
    taker = _topic_to_address(topics[2])

    # In CTF Exchange, assetId 0 represents collateral (USDC); non-zero is outcome token id.
    side = "BUY" if maker_asset_id == 0 and taker_asset_id != 0 else "SELL"
    token_id = taker_asset_id if side == "BUY" else maker_asset_id
    token_amount = taker_amount_filled if side == "BUY" else maker_amount_filled
    usdc_amount = maker_amount_filled if side == "BUY" else taker_amount_filled

    size = Decimal(token_amount) / USDC_SCALE if token_amount else Decimal("0")
    price = Decimal(usdc_amount) / Decimal(token_amount) if token_amount else Decimal("0")

    tx_hash = log.get("transactionHash")
    if isinstance(tx_hash, str):
        tx_hash_value = tx_hash
    elif hasattr(tx_hash, "hex"):
        tx_hash_value = tx_hash.hex()
    else:
        tx_hash_value = str(tx_hash)

    block_hash = log.get("blockHash")
    if isinstance(block_hash, str):
        block_hash_value = block_hash
    elif hasattr(block_hash, "hex"):
        block_hash_value = block_hash.hex()
    else:
        block_hash_value = str(block_hash)

    return {
        "order_hash": f"0x{order_hash_word:064x}",
        "maker": maker,
        "taker": taker,
        "maker_asset_id": maker_asset_id,
        "taker_asset_id": taker_asset_id,
        "maker_amount_filled": maker_amount_filled,
        "taker_amount_filled": taker_amount_filled,
        "fee": fee,
        "side": side,
        "token_id": str(token_id),
        "price": str(price.normalize()),
        "size": str(size.normalize()),
        "tx_hash": tx_hash_value,
        "log_index": _hex_to_int(log.get("logIndex")),
        "block_number": _hex_to_int(log.get("blockNumber")),
        "block_hash": block_hash_value,
        "exchange_address": log.get("address"),
    }
