using Base: ident_cmp
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
counties = Dict(values(elem.attributes) for elem in json.features)
# Block Assignment
if !isfile(joinpath("data", "BlockAssign_ST72_PR.zip"))
    download(string(URI(scheme = "https",
                        host = "www2.census.gov",
                        path = "/geo/docs/maps-data/data/baf2020/BlockAssign_ST72_PR.zip")),
             joinpath("data", "BlockAssign_ST72_PR.zip"))
end
if !isdir(joinpath("data", "BlockAssign_ST72_PR"))
    unzip(joinpath("data", "BlockAssign_ST72_PR.zip"), joinpath("data", "BlockAssign_ST72_PR"))
end

sldl = CSV.read(joinpath("data", "BlockAssign_ST72_PR", "BlockAssign_ST72_PR_SLDL.txt"), DataFrame)
select!(sldl,
        :BLOCKID => ByRow(string) => :BLOCKID,
        :DISTRICT => ByRow(x -> x == "ZZZ" ? 0 : parse(Int, x)) => :sldl)
transform!(sldl,
           :BLOCKID => ByRow(x -> SubString(x, 3, 5)) => :cty)
transform!(sldl, :cty => ByRow(x -> counties[x]) => :municipio)
select!(sldl, Not(:cty))
# Precintos
precintos = CSV.read(joinpath("data", "precintos.tsv"), DataFrame)
rename!(precintos, :representativo => :sldl, :senatorial => :sldu)
sl = leftjoin(sldl, precintos, on = [:municipio, :sldl])
transform!(sl, :sldl => ByRow(x -> x == 0 ? missing : x), renamecols = false)
# Population
pop = CSV.read(joinpath("data", "pop.csv"), DataFrame)
select!(pop,
        [:COUNTY, :TRACT, :BLOCK] => ByRow(
            (x, y, z) -> string("72",
                                lpad(x, 3, '0'),
                                lpad(y, 6, '0'),
                                lpad(z, 4, '0'))) => :BLOCKID,
        :P0010001, :P0030001, :P0050001)
rename!(pop, [:BLOCKID, :total, :adult, :grpqtrs])
x = leftjoin(sl, pop, on = :BLOCKID)
# select!(output, [:BLOCKID, :municipio, :sldl, :sldu, :precinto, :total, :adult, :grpqtrs])

blk_data = DataFrame(Shapefile.Table(joinpath(homedir(), "Downloads", "tl_2020_72_all", "tl_2020_72_tabblock20.shp")))
select!(blk_data,
        :GEOID20 => identity => :BLOCKID,
        :geometry => ByRow(GeoJSON.write) => :geometry)
output = innerjoin(x, blk_data, on = :BLOCKID)
select!(output, [:BLOCKID, :municipio, :sldl, :sldu, :precinto, :total, :adult, :grpqtrs, :geometry])
execute(conn, "BEGIN;")
load!(output,
      conn,
      string("INSERT INTO precintos_electorales_pr_2020.blks (",
             "blockid, municipio, sldl, sldu, vtd, total, adult, grpqtrs, geometry) ",
             "VALUES(",
             join(("\$$i" for i in 1:size(output, 2) - 1), ','),
             ", ST_SetSRID(ST_GeomFromGeoJSON(\$$(size(output, 2))), 4326)",
             ");"))
execute(conn, "COMMIT;")
