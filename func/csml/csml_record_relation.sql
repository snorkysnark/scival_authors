create table {{table}}
(
    id_record     integer not null
        constraint csml_record_relation_csml_record_id_record_fk
            references {{records}},
    id_slice_to   integer not null,
    id_record_rel integer not null
        constraint csml_record_relation_csml_record_id_record_rel_fk
            references {{records}},
    id_slice_from integer not null
);

{%split%}

create unique index csml_record_relation_id_slice_from_id_record_id_slice_to_id_rec
    on {{table}} (id_slice_from, id_record, id_slice_to, id_record_rel);

{%split%}

INSERT INTO {{table}}
SELECT DISTINCT
    p1.id_record,
    p1.id_slice,
    p2.id_record,
    p2.id_slice
FROM {{pairs}}
JOIN {{records}} AS p1
    ON {{pairs}}.publication_id = p1.work_publication_id
JOIN {{records}} AS p2
    ON {{pairs}}.openalex_publication_id = p2.work_openalex_id
WHERE {{pairs}}.publication_id IS NOT NULL
