create table {{table}}
(
    type_state_load      integer not null
        primary key,
    name_type_state_load text
);

{%split%}

INSERT INTO {{table}} (type_state_load, name_type_state_load) VALUES (0, 'запись создана'); {%split%}
INSERT INTO {{table}} (type_state_load, name_type_state_load) VALUES (1, 'загружена базовая информация о записи (журнал, категории, выходные данные)'); {%split%}
INSERT INTO {{table}} (type_state_load, name_type_state_load) VALUES (2, 'загружены авторы'); {%split%}
INSERT INTO {{table}} (type_state_load, name_type_state_load) VALUES (3, 'обновлено цитирование'); {%split%}
INSERT INTO {{table}} (type_state_load, name_type_state_load) VALUES (4, 'требуется обновление цитирования'); {%split%}
INSERT INTO {{table}} (type_state_load, name_type_state_load) VALUES (5, 'троебуется загрузка записи из Scopus'); {%split%}
INSERT INTO {{table}} (type_state_load, name_type_state_load) VALUES (6, 'загрузили записи из Scopus'); {%split%}
INSERT INTO {{table}} (type_state_load, name_type_state_load) VALUES (7, 'отложена загрузка из Scopus'); {%split%}
INSERT INTO {{table}} (type_state_load, name_type_state_load) VALUES (8, 'требуется обновление цитирования для новой загруженной записи'); {%split%}
INSERT INTO {{table}} (type_state_load, name_type_state_load) VALUES (9, 'записи из Scopus загружены полностью'); {%split%}
INSERT INTO {{table}} (type_state_load, name_type_state_load) VALUES (10, 'запись WoS из RIS');
