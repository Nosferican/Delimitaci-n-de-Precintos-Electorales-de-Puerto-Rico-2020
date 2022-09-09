using HTTP, JSON3
using HTTP: URI, request
using Downloads: download
using ZipFile: ZipFile
using InfoZIP: InfoZIP, unzip
using CSV, DataFrames, Shapefile
using LightGraphs
using AbstractTrees
import AbstractTrees: children, printnode, print_tree, Leaves

response = request(
    "GET",
    URI(scheme = "https",
        host = "api.ipums.org",
        path = "/metadata/nhgis/datasets/2020_PL94171",
        query = ["version" => "v1"],
       ),
    ["Authorization" => ENV["API_IPUMS_TOKEN"]],
    )
response = request(
    "GET",
    URI(scheme = "https",
        host = "api.ipums.org",
        path = "/metadata/nhgis/datasets/2020_PL94171/data_tables/P1",
        query = ["version" => "v1"],
       ),
    ["Authorization" => ENV["API_IPUMS_TOKEN"]],
    )
json = JSON3.read(response.body)
json.data_tables
json.geog_levels
json.geographic_instances
# Request Extract
param = Dict(
    "datasets" => 
        Dict("2020_PL94171" =>
             Dict("data_tables" => ["P1"],
                  "geog_levels" => ["blck_grp"],
                 ),
             ),
    "geographic_extents" => ["720"],
    "shapefiles" => ["720_blck_grp_2020_tl2020"],
    "data_format" => "csv_no_header",
)
response = request(
    "POST",
    URI(scheme = "https",
        host = "api.ipums.org",
        path = "/extracts",
        query = ["product" => "nhgis", "version" => "v1"],
        ),
    ["Authorization" => ENV["API_IPUMS_TOKEN"],
     "Content-Type" => "application/json"],
    JSON3.write(param),
    )
json = JSON3.read(response.body)
num = json.number
json.datasets[Symbol("2020_PL94171")]
# Query Abstract
response = request(
    "GET",
    URI(scheme = "https",
        host = "api.ipums.org",
        path = "/extracts",
        query = ["product" => "nhgis", "version" => "v1"],
        ),
    ["Authorization" => ENV["API_IPUMS_TOKEN"],
     "Content-Type" => "application/json"],
    )
json = JSON3.read(response.body)
table_data = json[1].download_links["table_data"]
gis_data = json[1].download_links["gis_data"]

response = request("GET", table_data,
    ["Authorization" => ENV["API_IPUMS_TOKEN"],
     "Content-Type" => "application/json"],
    )
write(joinpath("data", "table_data.zip"), response.body)
response = request("GET", gis_data,
    ["Authorization" => ENV["API_IPUMS_TOKEN"],
     "Content-Type" => "application/json"],
    )
write(joinpath("data", "gis_data.zip"), response.body)

unzip(joinpath("data", "table_data.zip"), joinpath("data"))
mv(joinpath("data", only(filter(x -> startswith(x, "nhgis"), readdir(joinpath("data"))))),
   joinpath("data", "table_data"))
for file in filter(x -> startswith(x, "nhgis"), readdir(joinpath("data", "table_data")))
    mv(joinpath("data", "table_data", file),
       joinpath("data", "table_data", replace(file, r"nhgis\d+\_" => "")))
end
unzip(joinpath("data", "gis_data.zip"), joinpath("data"))
mv(joinpath("data", only(filter(x -> startswith(x, "nhgis"), readdir(joinpath("data"))))),
   joinpath("data", "gis_data"))
unzip(only(readdir(joinpath("data", "gis_data"), join = true)),
      joinpath("data", "gis_data"))
table_data = CSV.read(joinpath("data", "table_data", "ds248_2020_blck_grp.csv"), DataFrame,
                      select = [:GEOID, :U7B001])
rename!(table_data, [:id, :total])
transform!(table_data, :id => ByRow(x -> replace(x, r".*US" => "")), renamecols = false)
# Now GIS
# Run ArcGIS Pro model
edgelst = CSV.read(joinpath("data", "gis_data", "edgelist.csv"), DataFrame,
                   select = [:src_GEOID, :nbr_GEOID, :LENGTH])
subset!(edgelst, :LENGTH => ByRow(>(0)))
select!(edgelst, Not(:LENGTH))
rename!(edgelst, [:x, :y])
subset!(edgelst, [:x, :y] => ByRow(<))
# Add Ceiba Ferry
push!(edgelst, (720371604007, 720499505002)) # Ceiba-Culebra
push!(edgelst, (720371604007, 721479506003)) # Ceiba-Vieques

v = sort!(union(edgelst[!,:x], edgelst[!,:y]))
v = Dict(zip(v, eachindex(v)))
transform!(edgelst, [:x, :y] .=> ByRow(x -> v[x]), renamecols = false)

g = SimpleGraph(length(v))
for (x, y) in eachrow(edgelst)
    add_edge!(g, x, y)
end

pop = Dict(zip(axes(table_data, 1), sort!(table_data)[!,:total]))

pop_pr = sum(values(pop))
equal_dist = round(Int, pop_pr / 40)
max_dev = round(Int, 1.05 * equal_dist)
min_dev = round(Int, 0.95 * equal_dist)

struct SLDLPath0
    blkgrp :: Int
    nbhd :: Vector{Int}
    SLDLPath0(blkgrp::Integer) = new(blkgrp, SLDLPath.(setdiff!(neighborhood(g, blkgrp, 1), blkgrp)))
end
children(tree::SLDLPath0) = tree.nbhd


deletefirst!([1,4,2])
setdiff(neighborhood(g, 1, 1), 1)
SLDLPath0(1)

isLeaf(t:: IntTree) = t.childs == []

oneChild(t:: IntTree) = length(t.childs) == 1

tree(x:: Int64, ts:: Array{IntTree,1}) = IntTree(x,ts)

thepath = Vector{Vector{NTuple{2, Int}}}()
vertex = 1
total = pop[vertex]
hood_size = 1
push!(thepath, [(vertex, total)])
while total < max_dev
    for nbr in setdiff(neighborhood(g, vertex, hood_size), vertex)
        total += pop[nbr]
    end
end


push!(edgelst_clean, (x = nodes_rev[720371604007], y = nodes_rev[720499505002], LENGTH = 100))
push!(edgelst_clean, (x = nodes_rev[720371604007], y = nodes_rev[721479506003], LENGTH = 100))

for file in filter(x -> startswith(x, "nhgis"), readdir(joinpath("data", "gis_data")))
    mv(joinpath("data", "gis_data", file),
       joinpath("data", "gis_data", replace(file, r"nhgis\d+\_" => "")))
end

 
