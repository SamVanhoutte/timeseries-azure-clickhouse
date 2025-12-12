DROP TABLE IF EXISTS timegaps ;

CREATE TABLE timegaps
(
    DeviceId    LowCardinality(String),
    Timestamp   DateTime,
    Value       Float32
)
ENGINE = MergeTree()
ORDER BY (DeviceId, Timestamp);


-- Insert random series, but with gaps
INSERT INTO timegaps
SELECT
    concat('DEVICE_0', toString(DeviceId)) AS device_name,
    Timestamp,
    Value
FROM
(
    SELECT
        toUInt8(devices.number) AS DeviceId,
        ticks.number AS seq_index,
        
        -- 1. Generate Time: Start + (15 mins * index)
        toDateTime('2024-01-01 10:00:00') + toIntervalMinute(ticks.number * 15) AS Timestamp,
        
        -- 2. Generate Logical Value: Sine wave + Noise
        round(
            20 
            + 10 * sin((ticks.number + devices.number * 10) / 20.0) -- Main wave
            + 2 * cos(ticks.number / 3.0)                           -- Ripple
            + (rand() % 100) / 200.0,                               -- Small Jitter
        2) AS Value,

        -- 3. Calculate Unique Gap Parameters per Device
        -- We hash the DeviceId so every device gets a different random number
        -- Modulo 70 ensures the gap starts between index 10 and 80 (safe zone)
        10 + (sipHash64(devices.number) % 70) AS gap_start,
        
        -- Gap length between 2 and 10
        2 + (sipHash64(devices.number, 'len') % 9) AS gap_len

    FROM numbers(1, 5) AS devices       -- 5 Devices
    CROSS JOIN numbers(0, 100) AS ticks -- 100 Time entries (0-99)
)
WHERE 
    -- 4. Apply the Gap: Exclude rows that fall in the device's specific gap window
    NOT (seq_index >= gap_start AND seq_index < gap_start + gap_len)
ORDER BY 
    DeviceId, Timestamp;


select * from timegaps;



-- Query by half_hour - but we see gaps
SELECT
    DeviceId,
    toStartOfInterval(Timestamp, INTERVAL 30 MINUTE) AS halfhour,
    count() AS count,
    sum(Value) AS total
FROM timegaps
GROUP BY DeviceId, halfhour
ORDER BY DeviceId, halfhour ASC;

-- Query by half_hour - but we fill gaps, with 0 values
SELECT
    DeviceId,
    toStartOfInterval(Timestamp, INTERVAL 30 MINUTE) AS halfhour,
    count() AS count,
    sum(Value) AS total
FROM timegaps
GROUP BY DeviceId, halfhour
ORDER BY DeviceId, halfhour ASC
WITH FILL STEP toIntervalMinute(30);

-- Query by half_hour - but we fill gaps, with LOCF (Last Observation Carried Forward)
SELECT
    DeviceId,
    toStartOfInterval(Timestamp, INTERVAL 15 MINUTE) AS halfhour,
    count() AS count,
    sum(Value) AS total
FROM timegaps
GROUP BY DeviceId, halfhour
ORDER BY DeviceId, halfhour ASC
WITH FILL STEP toIntervalMinute(30)
INTERPOLATE (total);