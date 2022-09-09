with a as (
	SELECT a.municipio a_cty, a.tract a_tract, a.blkgrp a_blkgrp,
		   b.municipio b_cty, b.tract b_tract, b.blkgrp b_blkgrp
	FROM precintos_electorales_pr_2020.current_blkgrp20 a
	join precintos_electorales_pr_2020.current_blkgrp20 b
	on a.municipio < b.municipio
	and a.tract < b.tract
	and a.blkgrp < b.blkgrp
	and st_touches(a.geometry, b.geometry)
	union all
	select 'Ceiba' a_cty,
		   '160400' a_tract,
		   '7' a_blkgrp,
		   unnest('{Vieques,Culebra}'::text[]) b_cty,
		   unnest('{950600,950500}'::text[]) b_cty,
		   unnest('{3,2}'::char(1)[]) b_cty
),
b as (
	SELECT distinct county, municipio
	FROM precintos_electorales_pr_2020.blks
),
c as (
	select county || a_tract || a_blkgrp a, a.*
	from a
	join b
	on a.a_cty = b.municipio
),
d as (
	select county || b_tract || b_blkgrp b, a.*
	from c a
	join b
	on a.b_cty = b.municipio
)
select a, b
from d
order by a, b
;
