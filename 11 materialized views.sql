-- Number of raw JSON array based events
SELECT
    length(records) AS json_record_count
FROM cotacol_metrics_raw;

-- Query to extract json array values from raw EventHub input
SELECT *
FROM cotacol_metrics_raw
ARRAY JOIN records AS json_item;
-- Flatten all records from table
SELECT
    -- Extract fields from json
    JSONExtractString(json_item, 'metricName') AS metricName,
    parseDateTimeBestEffort(JSONExtractString(json_item, 'time')) AS time,
    JSONExtractString(json_item, 'resourceId') AS resourceId,
    -- Numerieke waardes
    JSONExtractInt(json_item, 'count') AS count,
    JSONExtractFloat(json_item, 'total') AS total,
    JSONExtractFloat(json_item, 'minimum') AS minimum,
    JSONExtractFloat(json_item, 'maximum') AS maximum,
    JSONExtractFloat(json_item, 'average') AS average
FROM cotacol_metrics_raw
ARRAY JOIN records AS json_item;

-- Create table to keep normalized json data
CREATE TABLE cotacol_metrics
(
    MetricName LowCardinality(String),
    Time DateTime CODEC(Delta, ZSTD),
    ResourceId LowCardinality(String),
    Count Int32,
    Total Float64,
    Minimum Float64,
    Maximum Float64,
    Average Float64
)
ENGINE = MergeTree()
ORDER BY (ResourceId, MetricName, Time);

-- CREATE Materialized view
CREATE MATERIALIZED VIEW cotacol_metrics_normalize_mv TO cotacol_metrics AS
SELECT
    -- Extract fields from json
    JSONExtractString(json_item, 'metricName') AS MetricName,
    parseDateTimeBestEffort(JSONExtractString(json_item, 'time')) AS Time,
    JSONExtractString(json_item, 'resourceId') AS ResourceId,
    JSONExtractInt(json_item, 'count') AS Count,
    JSONExtractFloat(json_item, 'total') AS Total,
    JSONExtractFloat(json_item, 'minimum') AS Minimum,
    JSONExtractFloat(json_item, 'maximum') AS Maximum,
    JSONExtractFloat(json_item, 'average') AS Average
FROM cotacol_metrics_raw
ARRAY JOIN records AS json_item;

-- INSERT INTO destination table
INSERT INTO cotacol_metrics
SELECT
    -- Extract fields from json
    JSONExtractString(json_item, 'metricName') AS MetricName,
    parseDateTimeBestEffort(JSONExtractString(json_item, 'time')) AS Time,
    JSONExtractString(json_item, 'resourceId') AS ResourceId,
    JSONExtractInt(json_item, 'count') AS Count,
    JSONExtractFloat(json_item, 'total') AS Total,
    JSONExtractFloat(json_item, 'minimum') AS Minimum,
    JSONExtractFloat(json_item, 'maximum') AS Maximum,
    JSONExtractFloat(json_item, 'average') AS Average
FROM cotacol_metrics_raw
ARRAY JOIN records AS json_item;

-- SELECT From destination table
SELECT * FROM cotacol_metrics;

-- Create table to keep normalized json data
CREATE TABLE cotacol_metrics_hour
(
    MetricName LowCardinality(String),
    Hour DateTime CODEC(Delta, ZSTD),
    ResourceId LowCardinality(String),
    Count Int32,
    Total Float64,
    Minimum Float64,
    Maximum Float64,
    Average Float64
)
ENGINE = MergeTree()
ORDER BY (ResourceId, MetricName, Hour);

-- Insert records group records by hour, metricname, resourceid with aggregations
INSERT INTO cotacol_metrics_hour
SELECT MetricName, toStartOfHour(Time) AS Hour, ResourceId,
        sum(Count) AS Count, avg(Average) AS Average, min(Minimum) AS Minimum,
        max(Maximum) AS Maximum, sum(Total) AS Total
FROM cotacol_metrics
GROUP BY MetricName, Hour, ResourceId
ORDER BY ResourceId, MetricName, Hour;

-- Create materialized view for aggregations
CREATE MATERIALIZED VIEW cotacol_metrics_hour_mv TO cotacol_metrics_hour AS
SELECT MetricName, toStartOfHour(Time) AS Hour, ResourceId,
        sum(Count) AS Count, avg(Average) AS Average, min(Minimum) AS Minimum,
        max(Maximum) AS Maximum, sum(Total) AS Total
FROM cotacol_metrics
GROUP BY MetricName, Hour, ResourceId
ORDER BY ResourceId, MetricName, Hour;

-- Using the final key word !
SELECT * FROM cotacol_metrics_hour FINAL;

-- Exceptions per hour
SELECT * FROM cotacol_metrics_hour FINAL
WHERE MetricName = 'exceptions/count';