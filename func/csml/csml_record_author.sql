create table {{table}}
(
    id_record_author       INTEGER not null
        primary key autoincrement,
    id_record              integer not null
        constraint csml_contributor_record_fk
            references {{record}},
    display_name           text,
    full_name              text,
    wos_standard           text,
    first_name             text,
    last_name              text,
    email_addr             text,
    dais_id                text,
    reprint                varchar(10),
    daisng_id              text,
    role                   text,
    seq_no                 integer,
    auid                   text,
    initials               text,
    suffix                 text,
    degrees                text,
    preferred_initials     text,
    preferred_indexed_name text,
    preferred_surname      text,
    preferred_givenname    text,
    id_pure_person         integer,
    rank INT,

    work_author_id INT,
    work_external_author_id INT,
    work_openalex_author_id INT
);

{%split%}

INSERT INTO {{table}}(
    id_record,
    display_name,
    reprint,
    work_author_id
)
SELECT
    id_record,
    {{authors}}.name,
    {{authors}}.scopus_author_id,
    {{authors}}.author_id
FROM {{authors}}
JOIN {{pairs}} ON {{pairs}}.author_id = {{authors}}.author_id
JOIN {{record}} ON {{pairs}}.publication_id = {{record}}.work_publication_id
WHERE {{pairs}}.author_id IS NOT NULL

{%split%}

INSERT INTO {{table}}(
    id_record,
    display_name,
    reprint,
    work_external_author_id,
    rank
)
SELECT
    id_record,
    ext.author_names,
    ext.scopus_author_id,
    ext.external_author_id,
    rank
FROM {{external_authors}} AS ext
JOIN {{pairs}} ON {{pairs}}.external_author_id = ext.external_author_id
JOIN {{record}} ON {{pairs}}.publication_id = {{record}}.work_publication_id
WHERE {{pairs}}.external_author_id IS NOT NULL

{%split%}

INSERT INTO {{table}} (
    id_record,
    display_name,
    reprint,
    work_openalex_author_id
)
SELECT
    id_record,
    authors.display_name,
    authors.orcid,
    openalex_author_id
FROM {{openalex_authors}} AS authors
JOIN {{openalex_authors_raw}} AS pairs
    ON pairs.openalex = authors.openalex
JOIN {{record}} ON pairs.openalex_publication_id = {{record}}.work_openalex_id
