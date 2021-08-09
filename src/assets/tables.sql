
CREATE TABLE IF NOT EXISTS precintos_electorales_pr_2020.vtd
(
    vtd text,
    geometry geometry NOT NULL,
    PRIMARY KEY (vtd)
);

ALTER TABLE precintos_electorales_pr_2020.vtd
    OWNER to postgres;

CREATE TABLE IF NOT EXISTS precintos_electorales_pr_2020.precintos
(
    precinto smallint,
    municipio text NOT NULL,
    representativo smallint NOT NULL,
    senatorial smallint NOT NULL,
    PRIMARY KEY (precinto)
);

ALTER TABLE precintos_electorales_pr_2020.precintos
    OWNER to postgres;
