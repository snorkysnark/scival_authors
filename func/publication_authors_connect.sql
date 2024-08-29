{% set columns = [
    Column("author_id", "INT REFERENCES {{authors}}"),
] %}

UPDATE {{table}}
SET author_id = {{authors}}.author_id
FROM {{authors}}
WHERE {{table}}.scopus_author_id = {{authors}}.scopus_author_id
