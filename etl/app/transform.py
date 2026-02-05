from __future__ import annotations
import pandas as pd
from loguru import logger


def clean_for_fct(raw_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transform: start")

    cols_out = ["snapshot_ts", "coin_id", "symbol", "price", "market_cap", "volume_24h"]
    if raw_df is None or raw_df.empty:
        logger.warning("Transform: empty input")
        return pd.DataFrame(columns=cols_out)

    df = raw_df.copy()

    df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"], utc=True, errors="coerce")
    df["coin_id"] = df["coin_id"].astype("string")
    df["symbol"] = df["symbol"].astype("string").str.lower()

    df["current_price"] = pd.to_numeric(df.get("current_price"), errors="coerce")
    df["market_cap"] = pd.to_numeric(df.get("market_cap"), errors="coerce")
    df["total_volume"] = pd.to_numeric(df.get("total_volume"), errors="coerce")

    before = len(df)
    df = df.dropna(subset=["snapshot_ts", "coin_id", "symbol", "current_price"])
    df = df[df["current_price"] > 0]
    logger.info(f"Transform: filtered bad rows={before - len(df)}")

    df["_vol_rank"] = df["total_volume"].fillna(-1)
    df = df.sort_values(
        by=["snapshot_ts", "coin_id", "_vol_rank"],
        ascending=[True, True, False],
        kind="mergesort",
    )
    before_dups = len(df)
    df = df.drop_duplicates(subset=["snapshot_ts", "coin_id"], keep="first")
    logger.info(f"Transform: deduped rows={before_dups - len(df)}")

    fct = pd.DataFrame(
        {
            "snapshot_ts": df["snapshot_ts"],
            "coin_id": df["coin_id"],
            "symbol": df["symbol"],
            "price": df["current_price"],
            "market_cap": df["market_cap"],
            "volume_24h": df["total_volume"],
        }
    ).sort_values(["snapshot_ts", "coin_id"], kind="mergesort").reset_index(drop=True)

    logger.info(f"Transform: done rows={len(fct)}")
    return fct
