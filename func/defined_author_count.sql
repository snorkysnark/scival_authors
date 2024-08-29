{% set columns = [Column("defined_external_author_count", "INT")] %}

UPDATE {{table}}
SET defined_external_author_count = 0
WHERE external_author_id IS NOT NULL

{%split%}

UPDATE {{table}}
SET defined_external_author_count = count
FROM (
    SELECT external_author_id, COUNT(*) AS count
    FROM {{table}}
    WHERE external_author_count = 1
    GROUP BY external_author_id
) AS agg
WHERE {{table}}.external_author_id = agg.external_author_id
