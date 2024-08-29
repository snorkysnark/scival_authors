CREATE TABLE {{table}}(
    author_id                             INTEGER PRIMARY KEY,
    name                                  TEXT,
    scholarly_output                      INT,
    most_recent_publication               INT,
    citations                             INT,
    citations_per_publication             INT,
    field_weighted_citation_impact        FLOAT,
    h_index                               INT,
    output_in_top_10_citation_percentiles INT,
    oldest_publication                    INT,
    scopus_author_id                      INT UNIQUE,
    scopus_author_profile                 TEXT
)
{%split%}
INSERT INTO {{table}}(
    name,
    scholarly_output,
    most_recent_publication,
    citations,
    citations_per_publication,
    field_weighted_citation_impact,
    h_index,
    output_in_top_10_citation_percentiles,
    oldest_publication,
    scopus_author_id,
    scopus_author_profile
)
SELECT DISTINCT
    name,
    scholarly_output,
    most_recent_publication,
    citations,
    citations_per_publication,
    field_weighted_citation_impact,
    h_index,
    output_in_top_10_citation_percentiles,
    oldest_publication,
    scopus_author_id,
    scopus_author_profile
FROM {{raw}}
