-- Insert bitcoin in table, no compression applied
DROP TABLE IF EXISTS bitcoin_prices_default;


CREATE TABLE bitcoin_prices_default
(
    BtcTime DateTime,
    BtcVolume Int64,
    Close Float64,
    High Float64,
    Low Float64,
    Open Float64
)
ENGINE = MergeTree()
ORDER BY BtcTime;

INSERT INTO bitcoin_prices_default
SELECT 
    fromUnixTimestamp(toInt64(Timestamp)) AS BtcTime,
    Volume AS BtcVolume,
    Close,
    High,
    Low,
    Open
FROM url('https://savanhclickhouse.blob.core.windows.net/datasets/bitcoin-dataset.csv', 'csv');

DROP TABLE IF EXISTS bitcoin_prices_deltadate;
-- Insert bitcoin in table, no compression applied
CREATE TABLE bitcoin_prices_deltadate
(
    BtcTime DateTime CODEC(Delta, ZSTD),
    BtcVolume Int64,
    Close Float64,
    High Float64,
    Low Float64,
    Open Float64
)
ENGINE = MergeTree()
ORDER BY BtcTime;

INSERT INTO bitcoin_prices_deltadate
SELECT 
    fromUnixTimestamp(toInt64(Timestamp)) AS BtcTime,
    Volume AS BtcVolume,
    Close,
    High,
    Low,
    Open
FROM url('https://savanhclickhouse.blob.core.windows.net/datasets/bitcoin-dataset.csv', 'csv');
SELECT
    database,
    table,
    sum(rows) AS total_rows,
    formatReadableSize( sum(data_compressed_bytes)) AS compressed_size,
    sum(data_uncompressed_bytes) AS uncompressed_size,
    round((sum(data_compressed_bytes) / sum(data_uncompressed_bytes)) * 100, 2) AS compression_ratio_percent
FROM system.parts
WHERE active 
  AND table LIKE 'bitcoin%'
  AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
GROUP BY
    database,
    table
ORDER BY
    sum(data_compressed_bytes) DESC;