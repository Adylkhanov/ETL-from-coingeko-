BEGIN;

CREATE INDEX IF NOT EXISTS idx_fct_coin_ts
    ON fct_currency_rates (coin_id, snapshot_ts DESC);

CREATE INDEX IF NOT EXISTS idx_fct_ts
    ON fct_currency_rates (snapshot_ts DESC);

COMMIT;
