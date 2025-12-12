-- LINEAR REGRESSION FOR PREDICTION

-- Select all predicted values and the actual table values all together in one query
-- Combine actual values and predicted values for all devices in one query
WITH device_models AS (
    SELECT 
        DeviceId,
        simpleLinearRegression(toUnixTimestamp(Timestamp), Value) AS model,
        max(Timestamp) AS max_timestamp
    FROM timegaps
    GROUP BY DeviceId
),
predicted_values AS (
    SELECT 
        dm.DeviceId,
        dm.max_timestamp + toIntervalMinute((n.number + 1) * 15) AS timestamp,
        dm.model.1 * toUnixTimestamp(dm.max_timestamp + toIntervalMinute((n.number + 1) * 15)) + dm.model.2 AS value,
        'predicted' AS value_type,
        n.number + 1 AS prediction_step
    FROM device_models dm
    CROSS JOIN numbers(0, 10) n
),
actual_values AS (
    SELECT 
        DeviceId,
        Timestamp AS timestamp,
        Value AS value,
        'actual' AS value_type,
        NULL AS prediction_step
    FROM timegaps
)
SELECT 
    DeviceId,
    timestamp,
    value,
    value_type,
    prediction_step
FROM (
    SELECT * FROM actual_values
    UNION ALL
    SELECT * FROM predicted_values
)
ORDER BY DeviceId, timestamp;


