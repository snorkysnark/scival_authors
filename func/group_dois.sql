CREATE TABLE {{table}}(
    query_id INTEGER PRIMARY KEY,
    dois TEXT
)

{%-split-%}

INSERT INTO {{table}}(dois)
WITH rows AS (
    SELECT row_number() over (order by publication_id) row, 'https://doi.org/' || doi AS doi
    FROM {{publications}} WHERE has_external_authors AND doi IS NOT NULL
    )
SELECT group_concat(doi, '|') FROM rows
GROUP BY (row - 1) / {{per_page}}
