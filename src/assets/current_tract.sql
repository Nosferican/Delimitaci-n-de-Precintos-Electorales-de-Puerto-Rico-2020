CREATE MATERIALIZED VIEW precintos_electorales_pr_2020.current_tracts AS (
WITH A AS (
	SELECT blockid, county, tract, blk, municipio, sldl, sldu, vtd, total, adult, grpqtrs, geometry
	FROM precintos_electorales_pr_2020.blks
	WHERE vtd is not null
),
B AS (
	select county, tract, sldl, sum(total) total
	from a
	group by county, tract, sldl
),
C AS (
	SELECT distinct sldl, sldu
	from a
),
D AS (
	select distinct vtd, county, municipio, a.sldl, a.sldu
	from a
	join D b
	on a.sldl = b.sldl
	and a.sldu = b.sldu
),
D AS (
	select distinct on (county, tract) county, tract, sldl, total
	from B
	order by county, tract, total desc
),
E AS (
	select b.*, tract
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
),
G AS (
	select municipio, tract, sldl, sldu, vtd, sum(total) total, sum(adult) adult, sum(grpqtrs) grpqtrs, st_union(geometry) geometry
	from F
	group by municipio, tract, sldl, sldu, vtd
)
SELECT *
FROM G
ORDER BY municipio, tract
)
;
