BEGIN;

CREATE OR REPLACE VIEW v_daily_crypto_stats AS
WITH last_24h AS (
    SELECT *
    FROM fct_currency_rates
    WHERE snapshot_ts >= now() - interval '24 hours'
),
w AS (
    SELECT
        coin_id,
        symbol,
        snapshot_ts,
        price,

        AVG(price) OVER (PARTITION BY coin_id) AS avg_price_24h,
        MIN(price) OVER (PARTITION BY coin_id) AS min_price_24h,
        MAX(price) OVER (PARTITION BY coin_id) AS max_price_24h,

        FIRST_VALUE(price) OVER (
            PARTITION BY coin_id
            ORDER BY snapshot_ts DESC
        ) AS latest_price
    FROM last_24h
)
SELECT DISTINCT
    coin_id,
    symbol,
    latest_price,
    avg_price_24h,
    min_price_24h,
    max_price_24h,
    ROUND(
        ((latest_price - avg_price_24h) / NULLIF(avg_price_24h, 0)) * 100,
        2
    ) AS deviation_pct
FROM w;

COMMENT ON VIEW v_daily_crypto_stats IS '24h crypto stats with window functions';

COMMIT;

