CREATE TABLE {{table}}(
    external_author_id INTEGER PRIMARY KEY,
    author_names TEXT,
    scopus_author_id INT UNIQUE
)
{%split%}
INSERT INTO {{table}}(author_names, scopus_author_id)
WITH ext_authors_raw AS (
    SELECT DISTINCT author_name, scopus_author_id
    FROM {{pairs}}
    WHERE author_id IS NULl
),
ext_authors AS (
    SELECT group_concat(author_name, '| ') AS author_names, scopus_author_id
    FROM ext_authors_raw
    GROUP BY scopus_author_id
)
SELECT author_names, scopus_author_id
FROM ext_authors
