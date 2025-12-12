-- if the tags or categories string is null or empty, the resulting array should be an empty one
-- Stack overflow - small dataset
DROP TABLE IF EXISTS stackoverflow_q;

CREATE TABLE stackoverflow_q
(
    question_id Int32, 
    title String,
    body String, 
    tags Array(String), 
    tag_count Int8 ALIAS length(tags),
    programming_language LowCardinality(String), 
    categories Array(String),
    creation_date DateTime,
    view_count Int32,
    score Int32,
    answer_count Int32, 
    comment_count Int32,
    favorite_count Int32,
    answered boolean,
    accepted boolean,
    has_code boolean,
    code_block_count Int32,
    first_response_time_seconds Int32
)
ENGINE = MergeTree()
ORDER BY (programming_language, creation_date);


INSERT INTO stackoverflow_q
SELECT 
    question_id, title, body, splitByChar(',', assumeNotNull(tags)) as tags,
    programming_language, splitByChar(',', assumeNotNull(categories)) as categories,
    creation_date, view_count, score, answer_count, comment_count, favorite_count, 
    is_answered, has_accepted_answer, has_code, code_block_count, first_response_time_seconds 
FROM url('https://savanhclickhouse.blob.core.windows.net/datasets/stackoverflow-questions.csv', 'csv');