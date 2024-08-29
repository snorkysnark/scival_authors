{% set columns = [Column("rank_sum", "INT")] %}

UPDATE {{table}}
SET rank_sum = agg.rank_sum
FROM (
    SELECT publication_id, SUM(rank) AS rank_sum
    FROM {{pairs}}
    GROUP BY publication_id
) AS agg
WHERE {{table}}.publication_id = agg.publication_id
