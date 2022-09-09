with a as (
	select municipio, sum(total) total, st_union(geometry) geometry
	from precintos_electorales_pr_2020.current_blkgrp
	group by municipio
	order by municipio
),
b as (
	select sum(total) / 40 x
	from a
),
c as (
	select municipio, round(total / x :: decimal, 2) y, geometry
	from a
	join b
	on true
),
d as (
	select a.municipio a, b.municipio b, (a.y + b.y - 5) y, st_union(a.geometry, b.geometry)
	from c a
	join c b
	on a.municipio < b.municipio
	and st_intersects(a.geometry, b.geometry)
)
select a, b, y
from d
order by y
;
