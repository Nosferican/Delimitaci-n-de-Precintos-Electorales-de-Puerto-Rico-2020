using HTTP: HTTP, URI, request
using JSON3: JSON3
using LibPQ: LibPQ, Connection, execute, load!
using DataFrames: DataFrames, DataFrame
using CSV: CSV

conn = Connection("")
# Voting Districts
response = request("GET",
                   URI(scheme = "https",
                       host = "tigerweb.geo.census.gov",
                       path = "/arcgis/rest/services/Census2020/Legislative/MapServer/3/query",
                       query = ["where" => "state=72",
                                "outFields" => "VTD",
                                "f" => "geojson"]))
@assert response.status == 200
json = JSON3.read(response.body)
parse_voting_district(node) = (vtd = node.properties.VTD, geometry = JSON3.write(node.geometry))
voting_districts = parse_voting_district.(json.features);
sort!(getproperty.(voting_districts, :vtd))
filter!(elem -> elem.vtd ≠ "ZZZZZZ", voting_districts);
# execute(conn, "TRUNCATE TABLE precintos_electorales_pr_2020.vtd;")
load!(voting_districts,
      conn,
      string("INSERT INTO precintos_electorales_pr_2020.voting_districts VALUES(",
             "\$1, ST_SetSRID(ST_GeomFromGeoJSON(\$2), 4326)) ",
             "ON CONFLICT DO NOTHING;"))
# Precintos
precintos = CSV.read(joinpath("data", "precintos.tsv"), DataFrame)
# execute(conn, "TRUNCATE TABLE precintos_electorales_pr_2020.precintos;")
load!(precintos,
      conn,
      string("INSERT INTO precintos_electorales_pr_2020.precintos VALUES(",
             "\$1, \$2, \$3, \$4)",
             "ON CONFLICT DO NOTHING;"))
