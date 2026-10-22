-- Template executed once per source file by the ELT raw-ingestion task.
-- All source fields are read as VARCHAR. Empty strings are not converted to NULL.
INSERT INTO {{TABLE_NAME}} (
    {{SOURCE_COLUMNS}}, _loaded_at, _source_file, _source_row_number, _run_id
)
SELECT
    {{SOURCE_COLUMNS}}, current_timestamp,
    '{{SOURCE_FILE}}', ordinality + 1, '{{RUN_ID}}'
FROM read_csv(
    '{{ABSOLUTE_PATH}}',
    header=true,
    all_varchar=true,
    nullstr='__NMM_RAW_NULL_SENTINEL_7F3A__',
    force_not_null=[{{SOURCE_COLUMN_NAMES}}],
    sample_size=-1
) WITH ORDINALITY
WHERE NOT EXISTS (
    SELECT 1 FROM {{TABLE_NAME}} AS existing
    WHERE existing._run_id='{{RUN_ID}}'
      AND existing._source_file='{{SOURCE_FILE}}'
      AND existing._source_row_number=ordinality + 1
);
