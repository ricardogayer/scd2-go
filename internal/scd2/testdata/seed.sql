-- Origem mínima para testar sem o AdventureWorks completo.
CREATE SCHEMA IF NOT EXISTS sales;
CREATE SCHEMA IF NOT EXISTS person;

CREATE TABLE IF NOT EXISTS person.countryregion (
    countryregioncode VARCHAR(3)  NOT NULL PRIMARY KEY,
    name              VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS sales.salesterritory (
    territoryid       INT         NOT NULL PRIMARY KEY,
    name              VARCHAR(50) NOT NULL,
    countryregioncode VARCHAR(3)  NOT NULL REFERENCES person.countryregion,
    "group"           VARCHAR(50) NOT NULL
);

INSERT INTO person.countryregion VALUES
    ('US', 'United States'), ('CA', 'Canada'), ('FR', 'France'),
    ('DE', 'Germany'), ('AU', 'Australia'), ('GB', 'United Kingdom')
ON CONFLICT DO NOTHING;

INSERT INTO sales.salesterritory VALUES
    (1, 'Northwest', 'US', 'North America'),
    (2, 'Northeast', 'US', 'North America'),
    (3, 'Central',   'US', 'North America'),
    (4, 'Southwest', 'US', 'North America'),
    (5, 'Southeast', 'US', 'North America'),
    (6, 'Canada',    'CA', 'North America'),
    (7, 'France',    'FR', 'Europe'),
    (8, 'Germany',   'DE', 'Europe'),
    (9, 'Australia', 'AU', 'Pacific'),
    (10, 'United Kingdom', 'GB', 'Europe')
ON CONFLICT DO NOTHING;
