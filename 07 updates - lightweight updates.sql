-- how to enable lightweight updates in this update
-- fix the last query so that every record is updated
-- add statement to optimize table and force merge
-- enable lightweight updates
DROP TABLE IF EXISTS UserActivity_Lightweight;
CREATE TABLE UserActivity_Lightweight
(
    UserId UInt64,
    UserType Enum('B2C', 'B2B'),
    PageViews Int16,
    Duration Int16
)
ENGINE = SharedMergeTree()
ORDER BY (UserId, UserType)
SETTINGS enable_mixed_granularity_parts = 1;

-- Insert test data
INSERT INTO UserActivity_Lightweight VALUES
    (4324182021466240001, 'B2B', 3, 204),
    (4324182021466240002, 'B2B', 3, 14),
    (4324182021466240002, 'B2B', 5, 64),
    (4324182021466240004, 'B2C', 23, 224),
    (4324182021466240005, 'B2C', 23, 224),
    (4324182021466240006, 'B2C', 42, 434);

SELECT * FROM system.parts WHERE table = 'UserActivity_Lightweight' and active;

-- Enable lightweight updates for this session
SET mutations_sync = 2;

SET enable_lightweight_update = 1;
SET apply_mutations_on_fly = 1;

UPDATE UserActivity_Lightweight
SET PageViews = (PageViews + 1)
WHERE UserId = 4324182021466240001
SETTINGS enable_lightweight_update = 1;

SET enable_lightweight_update = 1;

SELECT * FROM system.parts WHERE table = 'UserActivity_Lightweight';

SELECT name, value 
FROM system.settings 
WHERE name = 'enable_lightweight_update';