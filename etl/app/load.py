from __future__ import annotations
import json
from typing import Any, Dict, List

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from loguru import logger


def _records(df: pd.DataFrame, cols: List[str]) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    return df[cols].to_dict(orient="records")


def load_stg(engine: Engine, raw_df: pd.DataFrame) -> int:
    logger.info("Load STG: start")

    if raw_df is None or raw_df.empty:
        logger.warning("Load STG: empty input")
        return 0

    df = raw_df.copy()
   
    df["raw"] = df["raw"].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else None)

    
    df = df.drop_duplicates(subset=["snapshot_ts", "coin_id"], keep="last")

    cols = [
        "snapshot_ts", "coin_id", "symbol", "name",
        "current_price", "market_cap", "total_volume",
        "price_change_24h", "price_change_percentage_24h", "raw",
    ]
    recs = _records(df, cols)
    if not recs:
        logger.warning("Load STG: nothing to insert")
        return 0

    sql = text("""
        INSERT INTO stg_currency_rates (
            snapshot_ts, coin_id, symbol, name,
            current_price, market_cap, total_volume,
            price_change_24h, price_change_percentage_24h,
            raw
        )
        VALUES (
            :snapshot_ts, :coin_id, :symbol, :name,
            :current_price, :market_cap, :total_volume,
            :price_change_24h, :price_change_percentage_24h,
            CAST(:raw AS jsonb)
        )
        ON CONFLICT (snapshot_ts, coin_id) DO UPDATE SET
            symbol = EXCLUDED.symbol,
            name = EXCLUDED.name,
            current_price = EXCLUDED.current_price,
            market_cap = EXCLUDED.market_cap,
            total_volume = EXCLUDED.total_volume,
            price_change_24h = EXCLUDED.price_change_24h,
            price_change_percentage_24h = EXCLUDED.price_change_percentage_24h,
            raw = EXCLUDED.raw,
            loaded_at = now()
    """)

    with engine.begin() as conn:
        conn.execute(sql, recs)

    logger.info(f"Load STG: upserted rows={len(recs)}")
    return len(recs)


def upsert_fct(engine: Engine, fct_df: pd.DataFrame) -> int:
    logger.info("Load FCT: start")

    cols = ["snapshot_ts", "coin_id", "symbol", "price", "market_cap", "volume_24h"]
    recs = _records(fct_df, cols)

    if not recs:
        logger.warning("Load FCT: nothing to upsert")
        return 0

    sql = text("""
        INSERT INTO fct_currency_rates (
            snapshot_ts, coin_id, symbol, price, market_cap, volume_24h
        )
        VALUES (
            :snapshot_ts, :coin_id, :symbol, :price, :market_cap, :volume_24h
        )
        ON CONFLICT (snapshot_ts, coin_id) DO UPDATE SET
            symbol = EXCLUDED.symbol,
            price = EXCLUDED.price,
            market_cap = EXCLUDED.market_cap,
            volume_24h = EXCLUDED.volume_24h,
            loaded_at = now()
    """)

    with engine.begin() as conn:
        conn.execute(sql, recs)

    logger.info(f"Load FCT: upserted rows={len(recs)}")
    return len(recs)

