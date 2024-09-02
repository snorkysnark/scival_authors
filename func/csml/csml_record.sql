create table {{table}}
(
    id_record                                 INTEGER not null
        primary key autoincrement,
    id_slice                                  integer           not null,
    num_record                                varchar(50)       not null,
    type_database_record                      integer           not null
        constraint csml_record_csml_type_database_record_type_database_record_fk
            references {{type_database_record}},
    year_publ                                 text,
    cited_from_record                         integer,
    cited_verified                            integer,
    cited_verified_without_selfciting         integer,
    lang_document                             text,
    document_type                             text,
    source_type                               text,
    id_source                                 integer
        constraint csml_record_csml_source_id_source_fk
            references {{source}},
    source_title                              text,
    publisher                                 text,
    source_country                            text,
    cover_sort_date                           date,
    doi                                       text,
    issn_norm                                 text,
    hlp_short_name_org_for_counting           text,
    refs_count                                integer,
    fund_text                                 text,
    delivered_date                            date,
    type_state_load                           integer default 0 not null
        constraint csml_record_csml_type_state_load_type_state_load_fk
            references {{type_state_load}},
    scopus_sourceid                           text,
    qs_subj_count_rank                        integer,
    elibrary_sourceid                         text,
    cited_verified_without_selfciting_authors integer default 0 not null,
    keywords_count                            integer,
    authors_count                             integer,
    oa                                        text,
    international                             text,
    collaboration                             text,
    title_record                              text
);
{%split%}
create index csml_record_id_slice_num_record_index
    on {{table}} (id_slice, num_record);
{%split%}
create index csml_record_id_slice_type_state_load_index
    on {{table}} (id_slice, type_state_load);
{%split%}
create index csml_record_id_slice_year_publ_source_type_index
    on {{table}} (id_slice, year_publ, source_type);
