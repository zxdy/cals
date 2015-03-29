CREATE TABLE IF NOT EXISTS quote.historical_prices (
    id timeuuid,
    date timestamp,
    open decimal,
    high decimal,
    low decimal,
    close decimal,
    volume bigint,
    adj_close decimal,
    lucene text,
    PRIMARY KEY (id, date)
);