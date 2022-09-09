
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

CREATE TABLE IF NOT EXISTS precintos_electorales_pr_2020.blks
(
    blockid CHAR(15),
    county CHAR(3) GENERATED ALWAYS AS (Substring(blockid, 3, 3)) STORED,
    tract CHAR(6) GENERATED ALWAYS AS (Substring(blockid, 6, 6)) STORED,
    blkgrp CHAR(1) GENERATED ALWAYS AS (Substring(blockid, 12, 1)) STORED,
    blk CHAR(3) GENERATED ALWAYS AS (Substring(blockid, 13, 3)) STORED,
    municipio varchar(13) NOT NULL,
    sldl SMALLINT,
    sldu SMALLINT,
    vtd SMALLINT,
    total SMALLINT NOT NULL,
    adult SMALLINT NOT NULL,
    grpqtrs SMALLINT NOT NULL,
    geometry geometry NOT NULL,
    PRIMARY KEY (blockid)
);
ALTER TABLE precintos_electorales_pr_2020.blks
    OWNER to postgres;
