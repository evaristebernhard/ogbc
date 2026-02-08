from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.db.schema import init_db
from src.settings import ensure_db_directory, load_settings


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket stage2 demo runner")
    parser.add_argument("--tx-hash", type=str, default=None, help="Reference tx hash to infer block range")
    parser.add_argument("--event-slug", type=str, default=None, help="Gamma event slug")
    parser.add_argument("--from-block", type=int, default=None)
    parser.add_argument("--to-block", type=int, default=None)
    parser.add_argument("--db", type=str, default=None, help="SQLite db path override")
    parser.add_argument("--output", type=str, default=None, help="Output json path")
    parser.add_argument("--reset-db", action="store_true", help="Delete DB before run")
    parser.add_argument("--include-ctf", action="store_true", help="Reserved for extension")
    parser.add_argument("--no-exchange", action="store_true", help="Disable regular exchange logs")
    parser.add_argument("--no-neg-risk", action="store_true", help="Disable neg risk exchange logs")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        from web3 import Web3
    except ImportError as exc:
        raise RuntimeError("Missing dependency: web3. Run `pip install -r requirements.txt`.") from exc

    from src.indexer.run import determine_block_range, run_indexer

    settings = load_settings(db_override=args.db)
    ensure_db_directory(settings.db_path)

    db_file = Path(settings.db_path)
    if args.reset_db and db_file.exists():
        db_file.unlink()

    conn = init_db(settings.db_path)

    w3 = Web3(Web3.HTTPProvider(settings.rpc_url, request_kwargs={"timeout": 30}))
    if not w3.is_connected():
        raise RuntimeError(f"Unable to connect RPC: {settings.rpc_url}")

    tx_block = None
    if args.tx_hash:
        receipt = w3.eth.get_transaction_receipt(args.tx_hash)
        tx_block = int(receipt["blockNumber"])

    from_block, to_block = determine_block_range(
        w3=w3,
        conn=conn,
        from_block=args.from_block,
        to_block=args.to_block,
        tx_block=tx_block,
    )

    result = run_indexer(
        w3=w3,
        conn=conn,
        settings=settings,
        from_block=from_block,
        to_block=to_block,
        exchange_address=settings.polymarket_exchange,
        neg_risk_exchange=settings.polymarket_neg_risk_exchange,
        ctf_address=settings.polymarket_ctf,
        exchange_abi=None,
        ctf_abi=None,
        include_ctf=args.include_ctf,
        include_exchange=not args.no_exchange,
        include_neg_risk=not args.no_neg_risk,
        event_slug=args.event_slug,
    )

    payload = {"stage2": result}
    output_json = json.dumps(payload, indent=2, ensure_ascii=False)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_json, encoding="utf-8")

    print(output_json)


if __name__ == "__main__":
    main()
