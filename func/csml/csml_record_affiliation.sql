create table {{table}}
(
    id_record_affiliation INTEGER not null
        primary key autoincrement,
    id_record             integer                                                                                not null
        constraint csml_record_affiliation_record_fk
            references {{record}},
    addr_no               integer,
    full_address          text,
    city                  text,
    country               text,
    zip                   text,
    ziplocation           text,
    affiliation_city      text,
    affiliation_country   text,
    afid                  text,
    state                 text,
    city_group            text,
    country_code          text,
    id_pure_org           integer,
    only_record_level     integer default 0,
    reprint               varchar(10),
    type_institutions     text
);

{%split%}

INSERT INTO {{table}}(
    id_record,
    full_address,
    country_code,
    type_institutions,
    afid
)
SELECT
    id_record,
    full_address,
    country_code,
    type_institutions,
    afid
FROM {{affiliations}}
JOIN {{csml_author}} ON {{affiliations}}.openalex_author_id = {{csml_author}}.work_openalex_author_id
GROUP BY id_record, afid
