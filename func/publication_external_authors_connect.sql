{% set columns = [
    Column("external_author_id", "INT REFERENCES {{ext_authors}}"),
] %}

UPDATE {{table}}
SET external_author_id = {{ext_authors}}.external_author_id
FROM {{ext_authors}}
WHERE {{table}}.scopus_author_id = {{ext_authors}}.scopus_author_id
