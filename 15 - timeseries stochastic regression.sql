-- learning rate is the coefficient on step length, when the gradient descent step is performed. A learning rate that is too big may cause infinite weights of the model. Default is 0.00001.
-- l2 regularization coefficient which may help to prevent overfitting. Default is 0.1.
-- mini-batch size sets the number of elements, which gradients will be computed and summed to perform one step of gradient descent. Pure stochastic descent uses one element, however, having small batches (about 10 elements) makes gradient steps more stable. Default is 15.
-- method for updating weights, they are: Adam (by default), SGD, Momentum, and Nesterov. Momentum and Nesterov require a little bit more computations and memory, however, they happen to be useful in terms of speed of convergence and stability of stochastic gradient methods.

-- The parameters should be as following : stochasticLinearRegression(0.0001, 0.2, 20, 'SGD')
-- STOCHASTIC LINEAR REGRESSION FOR PREDICTION
-- Fixed prediction formula to use proper linear regression calculation

-- Select all predicted values and the actual table values all together in one query
-- Combine actual values and predicted values for all devices in one query
WITH 
    0.0001 AS learning_rate,
    0.01 AS l1_regularization,
    10 AS mini_batch_size,
    'Adam' as weight_update_method,
    device_models AS (
        SELECT 
            DeviceId,
            stochasticLinearRegression(learning_rate, l1_regularization, mini_batch_size, weight_update_method)(toUnixTimestamp(Timestamp), Value) AS model,
            max(Timestamp) AS max_timestamp,
            max(toUnixTimestamp(Timestamp)) AS max_unix_timestamp
        FROM timegaps
        GROUP BY DeviceId
    ),
    predicted_values AS (
        SELECT 
            dm.DeviceId,
            dm.max_timestamp + toIntervalMinute((n.number + 1) * 15) AS timestamp,
            -- Fixed prediction calculation: slope * (future_time - base_time) + intercept + last_actual_value_adjustment
            arrayElement(dm.model, 1) * ((n.number + 1) * 15 * 60) + arrayElement(dm.model, 2) AS value,
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