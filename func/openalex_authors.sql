CREATE TABLE {{table}}(
    openalex_author_id INTEGER PRIMARY KEY,
    openalex TEXT UNIQUE,
    display_name TEXT,
    orcid TEXT,
    author_position TEXT,
    institutions TEXT
);

{%split%}

INSERT INTO {{table}}(
    openalex,
    display_name,
    orcid,
    author_position,
    institutions
)
SELECT
    openalex,
    display_name,
    orcid,
    author_position,
    institutions
FROM {{raw}}
GROUP BY openalex;
