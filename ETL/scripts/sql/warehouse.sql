CREATE TABLE IF NOT EXISTS dim_dates (
    date_id integer NOT NULL,
    full_date timestamp without time zone,
    year integer,
    quarter integer,
    month integer,
    day integer,
    week integer,
    is_weekend boolean
);

CREATE TABLE IF NOT EXISTS dim_currencies (
    currency_id character varying(10) NOT NULL,
    currency_code character varying(10),
    description character varying(100)
);

CREATE TABLE IF NOT EXISTS dim_coin (
    coin_id VARCHAR(100) PRIMARY KEY,
    coin_name VARCHAR(255),
    symbol VARCHAR(50),
    category VARCHAR(100),
    launch_date DATE
);

CREATE TABLE IF NOT EXISTS dim_exchanges (
    id text,
    name text
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
    coin_id character varying(50) NOT NULL,
    date_id integer NOT NULL,
    price_usd double precision,
    percent_change_1h double precision,
    percent_change_24h double precision,
    percent_change_7d double precision,
    market_cap_usd double precision,
    volume24 double precision,
    volume24a double precision,
    csupply double precision,
    tsupply double precision,
    msupply double precision,
    api_timestamp timestamp without time zone,
    ingestion_timestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (coin_id, date_id, api_timestamp)
);


CREATE TABLE IF NOT EXISTS fact_news (
    news_id integer NOT NULL,
    type character varying(20),
    guid character varying(500),
    published_on timestamp without time zone,
    title character varying(500),
    subtitle text,
    authors character varying(255),
    url character varying(1000),
    source_id character varying(50),
    body text,
    keywords character varying(500),
    lang character varying(10),
    upvotes integer,
    downvotes integer,
    score double precision,
    sentiment character varying(20),
    status character varying(20),
    created_on timestamp without time zone,
    updated_on timestamp without time zone,
    source_data jsonb,
    category_data jsonb,
    ingestion_timestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (news_id, ingestion_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_fact_market_cap_date ON fact_market_cap (date_id);
CREATE INDEX IF NOT EXISTS idx_fact_market_volume_date ON fact_market_volume (date_id);
CREATE INDEX IF NOT EXISTS idx_fact_market_cap_percentage_date ON fact_market_cap_percentage (date_id);

