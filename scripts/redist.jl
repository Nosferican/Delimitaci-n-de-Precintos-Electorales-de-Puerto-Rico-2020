using HTTP: HTTP, URI
using LightGraphs: length
using LibPQ: LibPQ, Connection, execute, load!
using DataFrames
using CSV: CSV
using LightGraphs

# County Adjacency File
download(string(URI(scheme = "https",
                    host = "www2.census.gov",
                    path = "/geo/docs/reference/county_adjacency.txt")),
         joinpath("data", "county_adjacency.txt"))
ln = readlines(joinpath("data", "county_adjacency.txt"))
filter!(ln -> endswith(ln, r"72\d{3}"), ln)



conn = Connection("")

pop = DataFrame(execute(conn,
                String(read(joinpath("src", "assets", "blkgrp_pop.sql"))),
                not_null = true))
edgelst = DataFrame(execute(conn,
                    String(read(joinpath("src", "assets", "edgelst.sql"))),
                    not_null = true))
nodes = sort!(unique!(vcat(edgelst[!,:a], edgelst[!,:b])))
g = SimpleGraph(length(nodes))
for row in eachrow(edgelst)
    x = findfirst(isequal(row.a), nodes)
    y = findfirst(isequal(row.b), nodes)
    add_edge!(g, (x, y))
end

setdiff(pop[!,:blkgrp], nodes)



conn = Connection("")
data = DataFrame(execute(conn,
                         String(read(joinpath("src", "assets", "pairwise.sql"))),
                         not_null = true))
transform!(data, :y => ByRow(abs) => :y)
sort!(data, :y)

tracts = DataFrame(execute(conn,
                           string("select municipio, tract, total, sldl ",
                                  "from precintos_electorales_pr_2020.current_tracts;")))
blkgrp = DataFrame(execute(conn,
                           string("select municipio, tract, blkgrp, total, sldl ",
                                  "from precintos_electorales_pr_2020.current_blkgrp;")))

only_choice = combine(nrow, groupby(data, :a))
subset!(only_choice, :nrow => ByRow(==(1)))
CSV.write("single_choice.csv", innerjoin(data, only_choice, on = :a))

function join_pair!(output, data)
    x, y = findmin(data[!,:y])
    a, b = data[y,[:a, :b]]
    filter!(row -> (row.a ∉ [a, b]) & (row.b ∉ [a, b]), data)
    push!(output, (;a, b, x))
end
output = DataFrame(a = String[], b = String[], x = Float64[])
while !isempty(data)
    join_pair!(output, data)
end

CSV.write(joinpath("test.csv"), output)
