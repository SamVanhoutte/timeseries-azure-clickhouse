-- add statement to show most recent 10 merges in a table
-- add statement to optimize table and force merge
-- make these insert statements into a batch insert statement
DROP TABLE IF EXISTS UserActivity_Inserts;
CREATE TABLE UserActivity_Inserts
(
    UserId UInt64,
    UserType Enum('B2C', 'B2B'),
    PageViews Int16,
    Duration Int16
)
ENGINE = MergeTree()
ORDER BY UserId;

INSERT INTO UserActivity_Inserts VALUES (4324182021466240000, 'B2B', 3, 204);

-- Insert test data
INSERT INTO UserActivity_Inserts VALUES
    (4324182021466240001, 'B2B', 3, 204),
    (4324182021466240002, 'B2B', 3, 14),
    (4324182021466240003, 'B2B', 5, 64),
    (4324182021466240004, 'B2C', 23, 224),
    (4324182021466240005, 'B2C', 23, 224),
    (4324182021466240006, 'B2C', 42, 434);

SELECT * FROM system.parts WHERE table = 'UserActivity_Inserts' and active;

-- Optimize table and force merge
OPTIMIZE TABLE UserActivity_Inserts FINAL;

SELECT * FROM system.parts WHERE table = 'UserActivity_Inserts' and active;

-- Show most recent 10 merges in the table
SELECT 
    event_time,
    table,
    part_name,
    bytes_uncompressed,
    rows,
    merge_reason,
    merge_algorithm
FROM system.part_log
WHERE table = 'UserActivity_Inserts'
    AND event_type = 'MergeParts'
ORDER BY event_time DESC
LIMIT 10;