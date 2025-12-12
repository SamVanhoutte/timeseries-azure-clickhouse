-- Create table with Sign for CollapsingMerge
DROP TABLE IF EXISTS UserActivity_Collapsing;
CREATE TABLE UserActivity_Collapsing
(
    UserId UInt64,
    UserType Enum('B2C', 'B2B'),
    PageViews Int16,
    Duration Int16,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserId;

-- Insert sample data
INSERT INTO UserActivity_Collapsing VALUES(4324182021466249494, 'B2C', 5,  146,  1);
SELECT * FROM UserActivity_Collapsing FINAL;

-- Performing the update by creating cancel row first and state row after
INSERT INTO UserActivity_Collapsing VALUES(4324182021466249494, 'B2C', 0, 0, -1);
INSERT INTO UserActivity_Collapsing VALUES(4324182021466249494,  'B2C', 6,  185,  1);

SELECT * FROM UserActivity_Collapsing;
-- Three records
-- Select, using the FINAL keyword
SELECT * FROM UserActivity_Collapsing FINAL;

-- Force merge
OPTIMIZE TABLE UserActivity_Collapsing FINAL;
-- 1 record
SELECT * FROM UserActivity_Collapsing;

-- Showing aggregations
INSERT INTO UserActivity_Collapsing VALUES(4324182021466240001, 'B2B', 3,  204,  1);
INSERT INTO UserActivity_Collapsing VALUES(4324182021466240001, 'B2B', 3,  14,  -1);
INSERT INTO UserActivity_Collapsing VALUES(4324182021466240001, 'B2B', 5,  64,  1);
INSERT INTO UserActivity_Collapsing VALUES(4324182021466240000, 'B2C', 23,  224,  1);
INSERT INTO UserActivity_Collapsing VALUES(4324182021466240000, 'B2C', 23,  224,  -1);
INSERT INTO UserActivity_Collapsing VALUES(4324182021466240000, 'B2C', 42,  434,  1);


SELECT * FROM UserActivity_Collapsing ;
SELECT * FROM UserActivity_Collapsing FINAL;

-- Aggregations done without any FINAL: includes all values
SELECT UserType, SUM(PageViews)
FROM UserActivity_Collapsing
GROUP BY UserType;

-- FINAL keyword is more expensive, but guarantees right results
SELECT UserType, SUM(PageViews)
FROM UserActivity_Collapsing FINAL
GROUP BY UserType;

-- When doing aggregations on the table, leveraging the Sign column is smart
SELECT SUM(Sign) as Count, UserType, SUM(PageViews * Sign)
FROM UserActivity_Collapsing FINAL
GROUP BY UserType;