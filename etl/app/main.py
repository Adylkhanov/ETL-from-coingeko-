from datetime import datetime, timezone
from loguru import logger

from app.config import get_settings
from app.db import make_engine, ping_db
from app.extract import fetch_top_markets
from app.transform import clean_for_fct
from app.load import load_stg, upsert_fct


def main() -> None:
    s = get_settings()
    logger.info("ETL started")

    engine = make_engine(s.sqlalchemy_url)
    ping_db(engine)
    logger.info("DB connection OK")

    snapshot_ts = datetime.now(timezone.utc)

    raw_df = fetch_top_markets(s, snapshot_ts)
    load_stg(engine, raw_df)

    fct_df = clean_for_fct(raw_df)
    upsert_fct(engine, fct_df)

    logger.info("ETL finished successfully")


if __name__ == "__main__":
    main()
