-- Dimension Tables
CREATE TABLE dim_coins (
    coin_id VARCHAR(50) PRIMARY KEY,
    symbol VARCHAR(20) UNIQUE,
    name VARCHAR(100),
    nameid VARCHAR(100),
    rank INTEGER,
    price_usd DOUBLE PRECISION,
    percent_change_24h DOUBLE PRECISION,
    percent_change_1h DOUBLE PRECISION,
    percent_change_7d DOUBLE PRECISION,
    price_btc DOUBLE PRECISION,
    market_cap_usd DOUBLE PRECISION,
    csupply DOUBLE PRECISION,
    tsupply DOUBLE PRECISION,
    msupply DOUBLE PRECISION,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
CREATE INDEX idx_dim_coins_symbol ON dim_coins (symbol);

CREATE TABLE dim_exchanges (
    exchange_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE dim_dates (
    date_id INTEGER PRIMARY KEY,
    full_date TIMESTAMP,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    week INTEGER,
    is_weekend BOOLEAN
);
CREATE INDEX idx_dim_dates_year_month ON dim_dates (year, month);

CREATE TABLE dim_currencies (
    currency_id VARCHAR(10) PRIMARY KEY,
    currency_code VARCHAR(10),
    description VARCHAR(100)
);

-- Fact Tables
CREATE TABLE fact_price_history (
    history_id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) REFERENCES dim_coins(coin_id),
    date_id INTEGER REFERENCES dim_dates(date_id),
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE fact_price_history ADD CONSTRAINT unique_coin_date UNIQUE (coin_id, date_id);
CREATE INDEX idx_fact_price_history_coin_date ON fact_price_history (coin_id, date_id);
-- Partition theo date_id (nếu dữ liệu lớn)
-- PARTITION BY RANGE (date_id) -- Thêm partitions con nếu cần

CREATE TABLE fact_market_global (
    market_id SERIAL PRIMARY KEY,
    date_id INTEGER REFERENCES dim_dates(date_id),
    active_cryptocurrencies INTEGER,
    upcoming_icos INTEGER,
    ongoing_icos INTEGER,
    ended_icos INTEGER,
    markets INTEGER,
    market_cap_change_percentage_24h_usd DOUBLE PRECISION,
    updated_at TIMESTAMP,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fact_market_cap (
    market_cap_id SERIAL PRIMARY KEY,
    market_id INTEGER REFERENCES fact_market_global(market_id),
    currency_id VARCHAR(10) REFERENCES dim_currencies(currency_id),
    value DOUBLE PRECISION
);

CREATE TABLE fact_market_volume (
    market_volume_id SERIAL PRIMARY KEY,
    market_id INTEGER REFERENCES fact_market_global(market_id),
    currency_id VARCHAR(10) REFERENCES dim_currencies(currency_id),
    value DOUBLE PRECISION
);

CREATE TABLE fact_market_cap_percentage (
    market_cap_percentage_id SERIAL PRIMARY KEY,
    market_id INTEGER REFERENCES fact_market_global(market_id),
    coin_symbol VARCHAR(20) REFERENCES dim_coins(symbol),
    percentage DOUBLE PRECISION
);

CREATE TABLE fact_coin_metrics (
    metric_id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) REFERENCES dim_coins(coin_id),
    date_id INTEGER REFERENCES dim_dates(date_id),
    price_usd DOUBLE PRECISION,
    percent_change_24h DOUBLE PRECISION,
    percent_change_1h DOUBLE PRECISION,
    percent_change_7d DOUBLE PRECISION,
    market_cap_usd DOUBLE PRECISION,
    volume24 DOUBLE PRECISION,
    csupply DOUBLE PRECISION,
    api_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
