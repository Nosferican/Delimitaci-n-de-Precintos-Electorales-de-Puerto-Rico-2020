CREATE MATERIALIZED VIEW precintos_electorales_pr_2020.current_blkgrp AS (
WITH A AS (
	SELECT blockid, county, tract, blkgrp, blk, municipio, sldl, sldu, vtd, total, adult, grpqtrs, geometry
	FROM precintos_electorales_pr_2020.blks
	WHERE vtd is not null
),
B AS (
	select county, tract, blkgrp, sldl, sum(total) total
	from a
	group by county, tract, blkgrp, sldl
),
C AS (
	select distinct vtd, county, municipio, sldl, sldu
	from a
),
D AS (
	select distinct on (county, tract, blkgrp) county, tract, blkgrp, sldl, total
	from B
	order by county, tract, blkgrp, total desc
),
E AS (
	select b.*, tract, blkgrp
	from d a
	join c b
	on A.county = B.county
	AND A.sldl = B.sldl
),
F AS (
	select b.*, total, adult, grpqtrs, geometry
	from a
	join e b
	on A.county = B.county
	AND A.tract = B.tract
	AND A.blkgrp = B.blkgrp
),
G AS (
	select municipio, tract, blkgrp, sldl, sldu, vtd, sum(total) total, sum(adult) adult, sum(grpqtrs) grpqtrs, st_union(geometry) geometry
	from F
	group by municipio, tract, blkgrp, sldl, sldu, vtd
)
SELECT *
FROM G
ORDER BY municipio, tract, blkgrp
)
;
