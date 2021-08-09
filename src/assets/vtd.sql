SELECT vtd, precinto, municipio, representativo, senatorial, geometry
	FROM precintos_electorales_pr_2020.vtd A
	join precintos_electorales_pr_2020.precintos B
	ON precinto = floor(vtd::decimal)::smallint
	ORDER BY vtd
;
