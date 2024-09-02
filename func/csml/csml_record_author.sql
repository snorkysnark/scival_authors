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
    id_pure_person         integer
);
