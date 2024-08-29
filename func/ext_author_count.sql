{% set columns = [Column("external_author_count", "INT")] %}

UPDATE {{table}}
SET external_author_count = count
FROM (
    SELECT publication_id, COUNT(*) AS count FROM {{table}}
    WHERE external_author_id IS NOT NULL
    GROUP BY publication_id
) AS agg
WHERE {{table}}.publication_id = agg.publication_id
AND external_author_id IS NOT NULL
