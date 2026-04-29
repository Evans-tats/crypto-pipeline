-- Migration 001: core schema + hypertables
-- This file runs automatically when the postgres container starts (initdb.d)

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS raw_trades (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    notional_value DOUBLE PRECISION GENERATED ALWAYS AS (price * quantity) STORED,
    buyer_market_maker BOOLEAN,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('raw_trades', 'time', if_not_exists => TRUE);
CREATE  INDEX IF NOT EXISTS idx_raw_trades_symbol_time ON raw_trades (symbol, time DESC);

-- ── Enriched 1-minute indicators ────────────────────────────
CREATE TABLE IF NOT EXISTS trade_indicators (
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    symbol          TEXT        NOT NULL,
    open_price      DOUBLE PRECISION,
    high_price      DOUBLE PRECISION,
    low_price       DOUBLE PRECISION,
    close_price     DOUBLE PRECISION,
    volume          DOUBLE PRECISION,
    trade_count     BIGINT,
    vwap            DOUBLE PRECISION,
    PRIMARY KEY (window_start, symbol)
);

SELECT create_hypertable('trade_indicators', 'window_start', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_indicators_symbol ON trade_indicators (symbol, window_start DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_5min WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', window_start) AS bucket,
    symbol,
    first(open_price, window_start) AS open,
    max(high_price) AS high,
    min(low_price) AS low,
    last(close_price, window_start) AS close,
    sum(volume) AS volume,
    sum(trade_count) AS trades,
    avg(vwap) AS avg_vwap
FROM trade_indicators
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_5min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minute');

SELECT  add_retention_policy('raw_trades', INTERVAL '7 days');
SELECT  add_retention_policy('trade_indicators', INTERVAL '30 days');