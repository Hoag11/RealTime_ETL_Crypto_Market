CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT,
    week INT
);

CREATE TABLE IF NOT EXISTS dim_currency (
    currency_id VARCHAR(10) PRIMARY KEY,
    currency_name VARCHAR(100),
    symbol VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dim_coin (
    coin_id VARCHAR(100) PRIMARY KEY,
    coin_name VARCHAR(255),
    symbol VARCHAR(50),
    category VARCHAR(100),
    launch_date DATE
);

CREATE TABLE IF NOT EXISTS dim_market (
    market_id INT PRIMARY KEY,
    market_name VARCHAR(255),
    market_type VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS fact_market_global (
    date_id INT PRIMARY KEY,
    active_cryptocurrencies INT,
    upcoming_icos INT,
    ongoing_icos INT,
    ended_icos INT,
    markets INT,
    market_cap_change_percentage_24h_usd FLOAT,
    updated_at TIMESTAMP,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_market_cap (
    market_id INT NOT NULL,
    date_id INT NOT NULL,
    currency_id VARCHAR(10) NOT NULL,
    value NUMERIC,
    updated_at TIMESTAMP,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (market_id, currency_id)
);


CREATE TABLE IF NOT EXISTS fact_market_cap_percentage (
    market_id INT NOT NULL,
    date_id INT NOT NULL,
    coin_symbol VARCHAR(50) NOT NULL,
    percentage NUMERIC,
    updated_at TIMESTAMP,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (market_id, coin_symbol)
);


CREATE TABLE IF NOT EXISTS fact_market_volume (
    market_id INT NOT NULL,
    date_id INT NOT NULL,
    currency_id VARCHAR(10) NOT NULL,
    volume NUMERIC,
    updated_at TIMESTAMP,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (market_id, currency_id)
);


CREATE TABLE IF NOT EXISTS fact_coin_metrics (
    coin_id VARCHAR(100) NOT NULL,
    date_id INT NOT NULL,
    price_usd FLOAT,
    percent_change_1h FLOAT,
    percent_change_24h FLOAT,
    percent_change_7d FLOAT,
    market_cap_usd FLOAT,
    volume24 FLOAT,
    csupply FLOAT,
    tsupply FLOAT,
    msupply FLOAT,
    updated_at TIMESTAMP,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (coin_id, date_id)
);


CREATE INDEX IF NOT EXISTS idx_fact_market_cap_date ON fact_market_cap (date_id);
CREATE INDEX IF NOT EXISTS idx_fact_market_volume_date ON fact_market_volume (date_id);
CREATE INDEX IF NOT EXISTS idx_fact_market_cap_percentage_date ON fact_market_cap_percentage (date_id);

