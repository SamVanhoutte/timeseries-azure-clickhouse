-- Create table with Version for ReplacingMerge
DROP TABLE IF EXISTS UserActivity_Replacing;
CREATE TABLE UserActivity_Replacing
(
    UserId UInt64,
    UserType Enum('B2C', 'B2B'),
    PageViews Int16,
    Duration Int16,
    Version Int8
)
ENGINE = ReplacingMergeTree(Version)
ORDER BY UserId;

-- Insert sample data
INSERT INTO UserActivity_Replacing VALUES(4324182021466249494, 'B2C', 5,  146,  1);
SELECT * FROM UserActivity_Replacing FINAL;

-- Performing the update by creating cancel row first and state row after
INSERT INTO UserActivity_Replacing VALUES(4324182021466249494,  'B2C', 6,  185,  2);

SELECT * FROM UserActivity_Replacing;
-- Three records
-- Select, using the FINAL keyword
SELECT * FROM UserActivity_Replacing FINAL;

-- Force merge
OPTIMIZE TABLE UserActivity_Replacing FINAL;
-- 1 record
SELECT * FROM UserActivity_Replacing;

-- Showing aggregations
INSERT INTO UserActivity_Replacing VALUES(4324182021466240001, 'B2B', 3,  204,  1);
INSERT INTO UserActivity_Replacing VALUES(4324182021466240001, 'B2B', 5,  64,  2);
INSERT INTO UserActivity_Replacing VALUES(4324182021466240000, 'B2C', 23,  224,  1);
INSERT INTO UserActivity_Replacing VALUES(4324182021466240000, 'B2C', 42,  434,  2);


SELECT * FROM UserActivity_Replacing ;
SELECT * FROM UserActivity_Replacing FINAL;

-- Aggregations done without any FINAL: includes all values
SELECT UserType, SUM(PageViews)
FROM UserActivity_Replacing
GROUP BY UserType;

-- FINAL keyword is more expensive, but guarantees right results
SELECT UserType, SUM(PageViews)
FROM UserActivity_Replacing FINAL
GROUP BY UserType;
