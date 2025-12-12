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


ALTER TABLE uk_price_paid ADD PROJECTION uk_price_paid_town (
SELECT *
ORDER BY town
);

ALTER TABLE uk_price_paid 
(MATERIALIZE PROJECTION uk_price_paid_town)
SETTINGS mutations_sync = 1;
;

SELECT * from system.mutations
ORDER BY create_time DESC;

SELECT formatReadableQuantity(avg(price))
FROM uk_price_paid
WHERE town = 'LONDON';

EXPLAIN
SELECT formatReadableQuantity(avg(price))
FROM uk_price_paid
WHERE town = 'LONDON';