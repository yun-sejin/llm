-- ...existing code...
INSERT INTO dsllm_raw (
    id,
    src_type,
    src_sub_type,
    raw_content,
    created_at
)
VALUES (
    :id,
    :src_type,
    :src_sub_type,
    :raw_content,
    CURRENT_TIMESTAMP
); 
-- ...existing code...
