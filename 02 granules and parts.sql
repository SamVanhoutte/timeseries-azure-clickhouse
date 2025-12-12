-- Showing all granules and their first/last ordering key for that granule
SELECT
    part_name,
    granule_id + 1 as granule,
    min((programming_language, creation_date))                                    AS first_tuple_in_granule,
    max((programming_language, creation_date))                                    AS last_tuple_in_granule,
    count()                                    AS rows_in_granule
FROM (
    SELECT
        _part                                      AS part_name,        -- physical part
        intDiv(
            row_number() OVER (
                PARTITION BY _part
                ORDER BY (programming_language, creation_date)                        -- use your ORDER BY key here
            ) - 1,
            8192
        )                                          AS granule_id,
        programming_language,
        creation_date
    FROM stackoverflow_q
)
GROUP BY part_name, granule_id
ORDER BY granule_id;


-- all granules read;
SELECT COUNT(*) FROM stackoverflow_q 
WHERE stackoverflow_q.creation_date > '2025-10-01'; 

-- 2 granules read
SELECT COUNT(*) FROM stackoverflow_q 
WHERE stackoverflow_q.programming_language = 'javascript'; 

-- Let's now demonstrate on a larger dataset

SELECT formatReadableQuantity(avg(price))
FROM uk_price_paid
WHERE town = 'LONDON';

-- Let's now demonstrate on a larger dataset
SELECT formatReadableQuantity(avg(price))
FROM uk_price_paid
WHERE postcode1 = 'W10' and postcode2 = '4QH';

-- Let's now demonstrate on a larger dataset
SELECT formatReadableQuantity(avg(price))
FROM uk_price_paid
WHERE postcode1 = 'E14';

-- Let's now demonstrate on a larger dataset
SELECT formatReadableQuantity(avg(price))
FROM uk_price_paid
WHERE postcode2 = '4QH';