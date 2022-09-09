using DataFrames: _rename_cols, replace
using HTTP: HTTP, URI, request
using JSON3: JSON3
using LibPQ: LibPQ, Connection, execute, load!, propertynames
using DataFrames
using CSV: CSV
using InfoZIP: InfoZIP, unzip
using Shapefile: Shapefile
using GeoJSON: GeoJSON

conn = Connection("")
# Blocks
response = request("GET",
                   URI(scheme = "https",
                       host = "tigerweb.geo.census.gov",
                       path = "/arcgis/rest/services/Census2020/Tracts_Blocks/MapServer/2/query",
                       query = ["where" => "STATE=72",
                                "outFields" => "GEOID",
                                "f" => "geojson"]))
sort(counties[!,:cty])
response = request("GET",
                   URI(scheme = "https",
                       host = "tigerweb.geo.census.gov",
                       path = "/arcgis/rest/services/Census2020/Tracts_Blocks/MapServer/1/query",
                       query = ["where" => "state=72",
                                "outFields" => "COUNTY,TRACT,BLKGRP",
                                "f" => "geojson"]))
https://tigerweb.geo.census.gov/arcgis/rest/services/Census2020/Tracts_Blocks/MapServer/2/query?where=state%3D72&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=40000&resultRecordCount=1000&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=html

# Block Groups
response = request("GET",
                   URI(scheme = "https",
                       host = "tigerweb.geo.census.gov",
                       path = "/arcgis/rest/services/Census2020/Tracts_Blocks/MapServer/1/query",
                       query = ["where" => "state=72",
                                "outFields" => "COUNTY,TRACT,BLKGRP",
                                "f" => "geojson"]))
@assert response.status == 200
json = JSON3.read(response.body)
json.features[1].properties
parse_voting_district(node) = (
      cty = node.properties.COUNTY,
      ct = node.properties.TRACT,
      bg = node.properties.BLKGRP,
      geometry = JSON3.write(node.geometry))
blkgrp = DataFrame(parse_voting_district(elem) for elem in json.features);
sort!(getproperty.(voting_districts, :vtd))
filter!(elem -> elem.vtd ≠ "ZZZZZZ", voting_districts);
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
# Counties
response = request("GET",
                   URI(scheme = "https",
                       host = "tigerweb.geo.census.gov",
                       path = "/arcgis/rest/services/Census2020/tigerWMS_Census2020/MapServer/84/query",
                       query = ["where" => "state=72",
                                "outFields" => "COUNTY,BASENAME",
                                "returnGeometry" => false,
                                "f" => "json"]))
json = JSON3.read(response.body)
counties = DataFrame(elem.attributes for elem in json.features)
rename!(counties, :COUNTY => :cty, :BASENAME => :municipio)
transform!(counties, :cty => ByRow(x -> parse(Int, x)), renamecols = false)
counties_kv = Dict(eachrow(counties))
# Block Assignment
download(string(URI(scheme = "https",
                    host = "www2.census.gov",
                    path = "/geo/docs/maps-data/data/baf2020/BlockAssign_ST72_PR.zip")),
         joinpath("data", "BlockAssign_ST72_PR.zip"))
unzip(joinpath("data", "BlockAssign_ST72_PR.zip"), joinpath("data", "BlockAssign_ST72_PR"))

house = CSV.read(joinpath("data", "BlockAssign_ST72_PR", "BlockAssign_ST72_PR_SLDL.txt"), DataFrame)
transform!(house,
           :DISTRICT => ByRow(x -> x == "ZZZ" ? 0 : parse(Int, x)) => :representativo,
           :BLOCKID => ByRow(x -> counties_kv[convert(Int, rem(x ÷ 1e10, 72e3))]) => :municipio,
           renamecols = false)
house = leftjoin(house, precintos, on = [:municipio, :representativo])

subset(house, :precinto => ByRow(ismissing), :representativo => ByRow(!iszero))

correct = select(house, [:BLOCKID, :representativo, :precinto])
weird = leftjoin(correct, select(blks, [:BLOCKID, :precinto]),
                 on = :BLOCKID, makeunique=true)
wrong = filter(x -> x.precinto ≠ x.precinto_1, dropmissing(weird))
wrong_x = innerjoin(wrong, house[!,[:BLOCKID, :municipio]], on = :BLOCKID)

# senate = CSV.read(joinpath("data", "BlockAssign_ST72_PR", "BlockAssign_ST72_PR_SLDU.txt"), DataFrame)
vtd = CSV.read(joinpath("data", "BlockAssign_ST72_PR", "BlockAssign_ST72_PR_VTD.txt"),
               DataFrame,
               select = [:BLOCKID, :DISTRICT])
precintos = copy(house)
rename!(precintos, :DISTRICT => :house)
precintos = innerjoin(precintos, senate, on = :BLOCKID)
rename!(precintos, :DISTRICT => :senate)
transform!(precintos,
           :BLOCKID => ByRow(x -> join(reverse(digits(x))[3:5])) => :cty,
           :BLOCKID => ByRow(x -> join(reverse(digits(x))[6:11])) => :ct,
           :BLOCKID => ByRow(x -> join(reverse(digits(x))[12:12])) => :bg,
           :BLOCKID => ByRow(x -> join(reverse(digits(x))[13:end])) => :blk)
precintos = innerjoin(precintos, vtd, on = :BLOCKID)
rename!(precintos, :DISTRICT => :vtd)
transform!(precintos,
           :vtd => ByRow(x -> match(r"\d+(?=\.)", x) |>
             (x -> isa(x, Nothing) ? missing : parse(Int, x.match))) => :precinto,
           :house => ByRow(x -> x == "ZZZ" ? missing : parse(Int, x)),
           :senate => ByRow(x -> x == "ZZZ" ? missing : parse(Int, x)),
           :cty => ByRow(x -> parse(Int, x)),
           :ct => ByRow(x -> parse(Int, x)),
           :bg => ByRow(x -> parse(Int, x)),
           :blk => ByRow(x -> parse(Int, x)),
           :vtd => ByRow(x -> x == "ZZZZZZ" ? missing : parse(Int, match(r"(?<=\.)\d+", x).match)),
           renamecols = false)
blks = select(precintos,
              [:BLOCKID, :house, :senate, :cty, :ct, :bg, :blk, :precinto, :vtd])
blks = innerjoin(blks, counties, on = :cty)
# Sanity
sort!(unique(dropmissing(blks[!,[:precinto, :municipio, :house, :senate]])))

# Precintos
precintos = CSV.read(joinpath("data", "precintos.tsv"), DataFrame)


vtd = CSV.read(joinpath("data", "BlockAssign_ST72_PR", "BlockAssign_ST72_PR_VTD.txt"), DataFrame)

xxx = innerjoin(vtd, wrong, on = :BLOCKID)

execute(conn, "BEGIN;")
load!(blks,
      conn,
      string("INSERT INTO precintos_electorales_pr_2020.blks VALUES(",
             join(("\$$i" for i in 1:size(blks, 2)), ','),
             ");"))
execute(conn, "COMMIT;")

precintos_electorales = subset(select(precintos, :precinto, :cty, :house, :senate),
                               :house => ByRow(≠("ZZZ")),
                               :senate => ByRow(≠("ZZZ"))) |>
      dropmissing! |>
      unique! |>
      sort! |>
      (df -> transform(df,
                       :house => ByRow(x -> parse(Int, x)),
                       :senate => ByRow(x -> parse(Int, x)),
                       renamecols = false))
precintos_electorales = innerjoin(precintos_electorales, counties, on = :cty)

verify = combine(nrow,
                 groupby(unique(select(precintos, [:cty, :ct, :bg, :vtd])), [:cty, :ct, :bg]))
unique(verify[!,:nrow])
sort!(verify, order(:nrow, rev = true))

subset(precintos,
       :cty => ByRow(==("035")),
       :ct => ByRow(==("260902")),
       :bg => ByRow(==("1"))) |>
      sort!
blk_data = Shapefile.Table(joinpath(homedir(), "Downloads", "tl_2020_72_all", "tl_2020_72_tabblock20.shp"))
tbl = DataFrame(blk_data)
transform!(tbl,
           :geometry => ByRow(GeoJSON.write) => :geometry_geojson,
           :GEOID20 => ByRow(x -> parse(Int, x)) => :GEOID20)

blkgrp = Shapefile.Table(joinpath(homedir(), "Downloads", "tl_2020_72_all", "tl_2020_72_bg20.shp"))
tbl = DataFrame(blkgrp)
transform!(tbl,
           :geometry => ByRow(GeoJSON.write) => :geometry_geojson,
           :GEOID20 => ByRow(x -> parse(Int, x)) => :GEOID20)
verify = innerjoin(blks, tbl[!,[:GEOID20, :geometry_geojson]], on = :BLKGRPCE20 => :GEOID20)

pop = CSV.read(joinpath("data", "pop.csv"), DataFrame)
transform!(pop,
           :BLOCK => ByRow(x -> x ÷ 1000) => :bg,
           :BLOCK => ByRow(x -> rem(x, 1_000)) => :blk)
rename!(pop, :COUNTY => :cty, :TRACT => :ct,
             :P0010001 => :total,
             :P0030001 => :adult,
             :P0050001 => :grp_quarters)
rem(1320, 1000)

verify2 = innerjoin(verify,
                    pop[!,[:cty, :ct, :bg, :blk, :total, :adult, :grp_quarters]],
                    on = [:cty, :ct, :bg, :blk])
verify2 = verify2[!, vcat(1:10, 12:14, 11)]
execute(conn, "BEGIN;")
load!(verify2,
      conn,
      string("INSERT INTO precintos_electorales_pr_2020.blks VALUES(",
             join(("\$$i" for i in 1:size(verify2, 2) - 1), ','),
             ", ST_SetSRID(ST_GeomFromGeoJSON(\$$(size(verify2, 2))), 4326)",
             ");"))
execute(conn, "COMMIT;")
