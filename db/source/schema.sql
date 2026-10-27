-- Recorte das tabelas do AdventureWorks (porta para PostgreSQL) usadas
-- pela origem. Serve apenas para o sqlc inferir os tipos; não é executado.
CREATE SCHEMA sales;
CREATE SCHEMA person;

CREATE TABLE person.countryregion (
    countryregioncode VARCHAR(3)  NOT NULL PRIMARY KEY,
    name              VARCHAR(50) NOT NULL
);

CREATE TABLE sales.salesterritory (
    territoryid       INT         NOT NULL PRIMARY KEY,
    name              VARCHAR(50) NOT NULL,
    countryregioncode VARCHAR(3)  NOT NULL REFERENCES person.countryregion,
    "group"           VARCHAR(50) NOT NULL
);
