from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from loguru import logger


class ApiTemporaryError(RuntimeError):
    pass


def _retryable(code: int) -> bool:
    return code in (429, 500, 502, 503, 504)


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((requests.RequestException, ApiTemporaryError)),
)
def _get(url: str, params: Dict[str, Any], timeout: int) -> List[Dict[str, Any]]:
    resp = requests.get(url, params=params, timeout=timeout)

    if _retryable(resp.status_code):
        raise ApiTemporaryError(f"Retryable status={resp.status_code}, body={resp.text[:200]}")

    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected response type: {type(data)}")
    return data


def fetch_top_markets(settings, snapshot_ts: datetime) -> pd.DataFrame:
    logger.info("Extract: start")

    params = {
        "vs_currency": settings.vs_currency,
        "order": "market_cap_desc",
        "per_page": settings.top_n,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }

    logger.info(f"Extract: GET {settings.coingecko_url} params={params}")
    rows = _get(settings.coingecko_url, params=params, timeout=settings.timeout)

    out = []
    for r in rows:
        out.append(
            {
                "snapshot_ts": snapshot_ts,
                "coin_id": r.get("id"),
                "symbol": r.get("symbol"),
                "name": r.get("name"),
                "current_price": r.get("current_price"),
                "market_cap": r.get("market_cap"),
                "total_volume": r.get("total_volume"),
                "price_change_24h": r.get("price_change_24h"),
                "price_change_percentage_24h": r.get("price_change_percentage_24h"),
                "raw": r,
            }
        )

    df = pd.DataFrame(out)
    logger.info(f"Extract: fetched rows={len(df)}")
    return df
