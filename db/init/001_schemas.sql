BEGIN;


CREATE TABLE IF NOT EXISTS stg_currency_rates (
    snapshot_ts TIMESTAMPTZ NOT NULL,
    coin_id TEXT NOT NULL,
    symbol TEXT,
    name TEXT,

    current_price NUMERIC,
    market_cap NUMERIC,
    total_volume NUMERIC,

    price_change_24h NUMERIC,
    price_change_percentage_24h NUMERIC,

    raw JSONB,

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT stg_currency_rates_pk
        UNIQUE (snapshot_ts, coin_id)
);

COMMENT ON TABLE stg_currency_rates IS 'Raw crypto market snapshots from CoinGecko';


CREATE TABLE IF NOT EXISTS fct_currency_rates (
    snapshot_ts TIMESTAMPTZ NOT NULL,
    coin_id TEXT NOT NULL,
    symbol TEXT NOT NULL,

    price NUMERIC NOT NULL CHECK (price > 0),
    market_cap NUMERIC,
    volume_24h NUMERIC,

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT fct_currency_rates_pk
        UNIQUE (snapshot_ts, coin_id)
);

COMMENT ON TABLE fct_currency_rates IS 'Cleaned crypto prices (fact table)';

COMMIT;
BEGIN;


CREATE TABLE IF NOT EXISTS public.stg_currency_rates (
    snapshot_ts TIMESTAMPTZ NOT NULL,
    coin_id TEXT NOT NULL,
    symbol TEXT,
    name TEXT,

    current_price NUMERIC,
    market_cap NUMERIC,
    total_volume NUMERIC,

    price_change_24h NUMERIC,
    price_change_percentage_24h NUMERIC,

    raw JSONB,

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT stg_currency_rates_pk
        UNIQUE (snapshot_ts, coin_id)
);

COMMENT ON TABLE public.stg_currency_rates IS 'Raw crypto market snapshots from CoinGecko';


CREATE TABLE IF NOT EXISTS public.fct_currency_rates (
    snapshot_ts TIMESTAMPTZ NOT NULL,
    coin_id TEXT NOT NULL,
    symbol TEXT NOT NULL,

    price NUMERIC NOT NULL CHECK (price > 0),
    market_cap NUMERIC,
    volume_24h NUMERIC,

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT fct_currency_rates_pk
        UNIQUE (snapshot_ts, coin_id)
);

COMMENT ON TABLE public.fct_currency_rates IS 'Cleaned crypto prices (fact table)';

COMMIT;
