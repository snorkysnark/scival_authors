{% set columns = [
    Column("has_external_authors", "BOOL DEFAULT FALSE"),
] %}

UPDATE {{table}}
SET has_external_authors = TRUE
FROM (
    SELECT DISTINCT publication_id FROM {{pairs}}
    WHERE author_id IS NULL
) AS filtered
WHERE {{table}}.publication_id = filtered.publication_id
