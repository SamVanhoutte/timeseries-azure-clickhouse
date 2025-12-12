-- fix the last query so that every record is updated
-- add statement to optimize table and force merge
-- make these insert statements into a batch insert statement
DROP TABLE IF EXISTS UserActivity_Mutation;
CREATE TABLE UserActivity_Mutation
(
    UserId UInt64,
    UserType Enum('B2C', 'B2B'),
    PageViews Int16,
    Duration Int16
)
ENGINE = MergeTree()
ORDER BY (UserId, UserType);


-- Insert test data
INSERT INTO UserActivity_Mutation VALUES
    (4324182021466240001, 'B2B', 3, 204),
    (4324182021466240002, 'B2B', 3, 14),
    (4324182021466240002, 'B2B', 5, 64),
    (4324182021466240004, 'B2C', 23, 224),
    (4324182021466240005, 'B2C', 23, 224),
    (4324182021466240006, 'B2C', 42, 434);

SELECT * FROM system.parts WHERE table = 'UserActivity_Mutation' and active;

-- Optimize table and force merge
ALTER TABLE UserActivity_Mutation
UPDATE PageViews = (PageViews + 1)
WHERE 1 = 1;

SELECT * FROM system.parts WHERE table = 'UserActivity_Mutation' ;
