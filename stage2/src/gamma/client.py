from __future__ import annotations

import json
import time
from typing import Any

import requests

from src.db.store import normalize_token_id
from src.settings import Settings


class GammaClient:
    def __init__(self, settings: Settings) -> None:
        self.base = settings.gamma_api_base.rstrip("/")
        self.timeout = settings.request_timeout
        self.max_retries = settings.max_retries
        self.retry_base_delay = settings.retry_base_delay

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.base}{path}"
        last_err: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                if attempt + 1 == self.max_retries:
                    break
                sleep_seconds = self.retry_base_delay * (2**attempt)
                time.sleep(sleep_seconds)
        if last_err is not None:
            raise last_err
        raise RuntimeError(f"Gamma request failed for {url}")

    def get_event_with_markets(self, slug: str) -> dict[str, Any]:
        # Preferred endpoint.
        try:
            data = self._get_json(f"/events/{slug}")
            if isinstance(data, dict):
                return data
        except Exception:
            pass

        # Fallback endpoint with query param.
        data = self._get_json("/events", params={"slug": slug, "limit": 1})
        if isinstance(data, list) and data:
            return data[0]
        if isinstance(data, dict):
            if isinstance(data.get("data"), list) and data["data"]:
                return data["data"][0]
            if isinstance(data.get("events"), list) and data["events"]:
                return data["events"][0]
        raise ValueError(f"Gamma event not found for slug={slug}")

    def get_markets_for_event(self, event_slug: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        event_obj = self.get_event_with_markets(event_slug)
        markets = event_obj.get("markets")

        if not isinstance(markets, list) or not markets:
            # Fallback by global markets endpoint when embedded markets are absent.
            markets_payload = self._get_json("/markets", params={"eventSlug": event_slug, "limit": 500})
            if isinstance(markets_payload, list):
                markets = markets_payload
            elif isinstance(markets_payload, dict):
                if isinstance(markets_payload.get("data"), list):
                    markets = markets_payload["data"]
                elif isinstance(markets_payload.get("markets"), list):
                    markets = markets_payload["markets"]
                else:
                    markets = []
            else:
                markets = []

        return event_obj, [m for m in markets if isinstance(m, dict)]


def _parse_clob_token_ids(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        value = raw.strip()
        if value.startswith("["):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return [normalize_token_id(v) for v in parsed if normalize_token_id(v)]
            except json.JSONDecodeError:
                pass
        if value:
            normalized = normalize_token_id(value)
            return [normalized] if normalized else []
    if isinstance(raw, list):
        out: list[str] = []
        for item in raw:
            normalized = normalize_token_id(item)
            if normalized:
                out.append(normalized)
        return out
    return []


def _extract_value(item: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in item and item[key] is not None:
            return item[key]
    return None


def _detect_status(market: dict[str, Any]) -> str:
    status = _extract_value(market, "status", "state")
    if status:
        return str(status)
    if bool(market.get("closed")):
        return "closed"
    if bool(market.get("active", True)):
        return "active"
    return "unknown"


def normalize_event_payload(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": str(_extract_value(event, "id", "eventId") or ""),
        "slug": str(_extract_value(event, "slug") or ""),
        "title": _extract_value(event, "title", "question", "name"),
        "description": _extract_value(event, "description"),
        "neg_risk": bool(_extract_value(event, "negRisk", "enableNegRisk", "neg_risk") or False),
        "active": bool(_extract_value(event, "active") if _extract_value(event, "active") is not None else True),
        "closed": bool(_extract_value(event, "closed") or False),
        "created_at": _extract_value(event, "createdAt", "created_at"),
    }


def normalize_market_payload(
    market: dict[str, Any],
    event_row_id: int,
    event_neg_risk: bool,
    default_collateral_token: str,
) -> dict[str, Any] | None:
    slug = _extract_value(market, "slug", "marketSlug")
    condition_id = _extract_value(market, "conditionId", "condition_id")

    if not slug or not condition_id:
        return None

    outcomes_raw = _extract_value(market, "outcomes")
    outcomes: list[str] = []
    if isinstance(outcomes_raw, str):
        try:
            parsed = json.loads(outcomes_raw)
            if isinstance(parsed, list):
                outcomes = [str(x) for x in parsed]
        except json.JSONDecodeError:
            outcomes = []
    elif isinstance(outcomes_raw, list):
        outcomes = [str(x) for x in outcomes_raw]

    clob_ids = _parse_clob_token_ids(_extract_value(market, "clobTokenIds", "clob_token_ids"))

    yes_token_id = None
    no_token_id = None

    if len(clob_ids) >= 2 and len(outcomes) >= 2:
        pairs = {outcomes[idx].strip().lower(): clob_ids[idx] for idx in range(min(len(outcomes), len(clob_ids)))}
        yes_token_id = pairs.get("yes")
        no_token_id = pairs.get("no")

    if not yes_token_id and len(clob_ids) >= 1:
        yes_token_id = clob_ids[0]
    if not no_token_id and len(clob_ids) >= 2:
        no_token_id = clob_ids[1]

    return {
        "event_id": event_row_id,
        "slug": str(slug),
        "title": _extract_value(market, "question", "title", "name"),
        "description": _extract_value(market, "description"),
        "condition_id": str(condition_id),
        "question_id": _extract_value(market, "questionId", "question_id"),
        "oracle": _extract_value(
            market,
            "oracle",
            "oracleAddress",
            "umaResolutionContractAddress",
            "resolutionSource",
        ),
        "collateral_token": _extract_value(market, "collateralToken", "collateral_token")
        or default_collateral_token,
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id,
        "enable_neg_risk": bool(
            _extract_value(market, "enableNegRisk", "negRisk", "enable_neg_risk") or event_neg_risk
        ),
        "status": _detect_status(market),
        "created_at": _extract_value(market, "createdAt", "created_at"),
    }
