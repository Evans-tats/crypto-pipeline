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