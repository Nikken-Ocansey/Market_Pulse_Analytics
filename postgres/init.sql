CREATE TABLE IF NOT EXISTS stock_quotes (
    symbol VARCHAR(10) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    price DOUBLE PRECISION NOT NULL,
    volume BIGINT,
    latest_trading_day DATE NOT NULL,
    previous_close DOUBLE PRECISION,
    change DOUBLE PRECISION,
    change_percent DOUBLE PRECISION,
    ingestion_time TIMESTAMP,
    UNIQUE (symbol, latest_trading_day, price)
);