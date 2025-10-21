-- Dimension Tables
CREATE TABLE IF NOT EXISTS dim_exchanges (
    exchange_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_dates (
    date_id INTEGER PRIMARY KEY,
    full_date TIMESTAMP,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    week INTEGER,
    is_weekend BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_dim_dates_year_month ON dim_dates (year, month);

CREATE TABLE IF NOT EXISTS dim_currencies (
    currency_id VARCHAR(10) PRIMARY KEY,
    currency_code VARCHAR(10),
    description VARCHAR(100)
);

-- Fact Tables
CREATE TABLE IF NOT EXISTS fact_price_history (
    history_id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50),
    date_id INTEGER REFERENCES dim_dates(date_id),
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_fact_price_history_coin_date ON fact_price_history (coin_id, date_id);
-- Partition theo date_id (nếu dữ liệu lớn)
-- PARTITION BY RANGE (date_id) -- Thêm partitions con nếu cần

CREATE TABLE IF NOT EXISTS fact_market_global (
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

CREATE TABLE IF NOT EXISTS fact_market_cap (
    market_cap_id SERIAL PRIMARY KEY,
    market_id INTEGER REFERENCES fact_market_global(market_id),
    currency_id VARCHAR(10) REFERENCES dim_currencies(currency_id),
    value DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS fact_market_volume (
    market_volume_id SERIAL PRIMARY KEY,
    market_id INTEGER REFERENCES fact_market_global(market_id),
    currency_id VARCHAR(10) REFERENCES dim_currencies(currency_id),
    value DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS fact_market_cap_percentage (
    market_cap_percentage_id SERIAL PRIMARY KEY,
    market_id INTEGER REFERENCES fact_market_global(market_id),
    coin_symbol VARCHAR(20),
    percentage DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS fact_coin_metrics (
    coin_id VARCHAR(50) PRIMARY KEY,

    date_id INTEGER REFERENCES dim_dates(date_id),

    price_usd DOUBLE PRECISION,
    percent_change_1h DOUBLE PRECISION,
    percent_change_24h DOUBLE PRECISION,
    percent_change_7d DOUBLE PRECISION,

    market_cap_usd DOUBLE PRECISION,
    volume24 DOUBLE PRECISION,
    volume24a DOUBLE PRECISION,

    csupply DOUBLE PRECISION,
    tsupply DOUBLE PRECISION,
    msupply DOUBLE PRECISION,

    api_timestamp TIMESTAMP,

    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS fact_news (
    news_id SERIAL PRIMARY KEY,
    type VARCHAR(20),
    guid VARCHAR(500),                     -- Tăng từ 100 → 500 vì nhiều URL dài
    published_on TIMESTAMP,
    title VARCHAR(500),                    -- Tăng từ 100 → 500 (title dài)
    subtitle TEXT,                         -- Có thể null, không giới hạn độ dài
    authors VARCHAR(255),                  -- Có thể chứa nhiều tên
    url VARCHAR(1000),                     -- Một số URL RSS dài
    source_id VARCHAR(50),
    body TEXT,                             -- Nội dung bài viết dài
    keywords VARCHAR(500),                 -- Có thể chứa nhiều từ khoá nối bằng |
    lang VARCHAR(10),
    upvotes INT,
    downvotes INT,
    score DOUBLE PRECISION,
    sentiment VARCHAR(20),
    status VARCHAR(20),
    created_on TIMESTAMP,
    updated_on TIMESTAMP,
    source_data JSONB,                     -- dữ liệu lồng từ SOURCE_DATA
    category_data JSONB,                   -- danh sách CATEGORY_DATA
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
