create table {{table}}
(
    id_record_author      integer not null
        constraint csml_record_author_rel_affiliation_csml_record_author_id_record
            references {{csml_author}},
    id_record_affiliation integer not null
        constraint csml_record_author_rel_affiliation_csml_record_affiliation_id_r
            references {{csml_record_affiliation}},
    order_aff             integer,
    reprint               varchar(10),
    constraint csml_record_author_rel_affiliation_id_record_author_id_record_a
        unique (id_record_author, id_record_affiliation)
);

{%split%}

INSERT INTO {{table}}(id_record_author, id_record_affiliation, order_aff)
SELECT
    id_record_author, id_record_affiliation, order_
FROM {{openalex}}
JOIN {{csml_author}} ON {{openalex}}.openalex_author_id = {{csml_author}}.work_openalex_author_id
JOIN {{csml_record_affiliation}} AS aff ON aff.id_record = {{csml_author}}.id_record AND aff.afid = {{openalex}}.afid
