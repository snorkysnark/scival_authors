create table {{table}}
(
    type_database_record      integer not null
        primary key,
    name_type_database_record text
);{%split%}

INSERT INTO {{table}} (type_database_record, name_type_database_record) VALUES (1, 'WoS');{%split%}
INSERT INTO {{table}} (type_database_record, name_type_database_record) VALUES (2, 'Scopus');{%split%}
INSERT INTO {{table}} (type_database_record, name_type_database_record) VALUES (3, 'РИНЦ');{%split%}
INSERT INTO {{table}} (type_database_record, name_type_database_record) VALUES (4, 'Pure');
