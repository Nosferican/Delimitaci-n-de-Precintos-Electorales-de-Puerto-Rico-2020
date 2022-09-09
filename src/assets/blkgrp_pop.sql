with a as (
	SELECT distinct county, municipio
	FROM precintos_electorales_pr_2020.blks
),
b as (
	SELECT county || tract || blkgrp blkgrp, total
	FROM precintos_electorales_pr_2020.current_blkgrp20 b
	join a
	on a.municipio = b.municipio
)
select *
from b
order by blkgrp
;
