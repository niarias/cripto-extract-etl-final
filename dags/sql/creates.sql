CREATE TABLE IF NOT EXISTS fact_crypto_trading (
    trading_id VARCHAR(100) PRIMARY KEY,
    coin_id INT NOT NULL,
    exchange_id INT NOT NULL,
    date VARCHAR(30) NOT NULL DISTKEY,
    time VARCHAR(30) NOT NULL,
    qty_low FLOAT NOT NULL,
    high FLOAT NOT NULL,
    low FLOAT NOT NULL,
    qty_high FLOAT NOT NULL,
    volume FLOAT NOT NULL,
    exchange_trade_id VARCHAR(100) NOT NULL
    created_at TIMESTAMP NOT NULL DEFAULT GETDATE()
)
COMPOUND SORTKEY(coin_id, exchange_id, date);


CREATE TABLE IF NOT EXISTS fact_crypto_trading_stg (
    trading_id VARCHAR(100) PRIMARY KEY,
    coin_id INT NOT NULL,
    exchange_id INT NOT NULL,
    date VARCHAR(30) NOT NULL DISTKEY,
    time VARCHAR(30) NOT NULL,
    qty_low FLOAT NOT NULL,
    high FLOAT NOT NULL,
    low FLOAT NOT NULL,
    qty_high FLOAT NOT NULL,
    volume FLOAT NOT NULL,
    exchange_trade_id VARCHAR(100) NOT NULL
    created_at TIMESTAMP NOT NULL DEFAULT GETDATE()
)
COMPOUND SORTKEY(coin_id, exchange_id, date);


CREATE TABLE IF NOT EXISTS dim_dates (
    date VARCHAR(100) PRIMARY KEY,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL
)
DISTSTYLE ALL 
SORTKEY(date);



CREATE TABLE IF NOT EXISTS dim_dates_stg (
    date VARCHAR(100) PRIMARY KEY,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL
)
DISTSTYLE ALL 
SORTKEY(date);
