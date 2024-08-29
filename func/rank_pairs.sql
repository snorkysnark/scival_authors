{% set columns = [Column("rank", "INT")] %}

UPDATE {{table}}
SET rank = MAX(external_author_count - defined_external_author_count, 0)
WHERE external_author_id IS NOT NULL
