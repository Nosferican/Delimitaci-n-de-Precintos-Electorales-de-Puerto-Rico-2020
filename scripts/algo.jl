using CSV, DataFrames
using LightGraphs
using AbstractTrees
import AbstractTrees: children, printnode, print_tree, Leaves
import Base: isequal, push!

table_data = CSV.read(joinpath("data", "table_data", "ds248_2020_blck_grp.csv"), DataFrame,
                      select = [:GEOID, :U7B001])
rename!(table_data, [:id, :total])
transform!(table_data, :id => ByRow(x -> replace(x, r".*US" => "")), renamecols = false)
# Now GIS
# Run ArcGIS Pro model
edgelst = CSV.read(joinpath("data", "gis_data", "edgelist.csv"), DataFrame,
                   select = [:src_GEOID, :nbr_GEOID, :LENGTH])
subset!(edgelst, :LENGTH => ByRow(>(0)))
select!(edgelst, Cols(r"GEOID"))
rename!(edgelst, [:x, :y])
subset!(edgelst, [:x, :y] => ByRow(<))
transform!(edgelst, [:x, :y] .=> ByRow(string), renamecols = false)
# Add Ceiba Ferry
push!(edgelst, ("720371604007", "720499505002")) # Ceiba-Culebra
push!(edgelst, ("720371604007", "721479506003")) # Ceiba-Vieques
push!(edgelst, ("720531504005", "720531501062")) # Fajardo-Palomino

v = sort!(union(edgelst[!,:x], edgelst[!,:y]))
cypher = sort!(innerjoin(DataFrame(id = v), table_data, on = :id))
v = Dict(zip(cypher[!,:id], eachindex(v)))
transform!(edgelst, [:x, :y] .=> ByRow(x -> v[x]), renamecols = false)

g = SimpleGraph(length(v))
for (x, y) in eachrow(edgelst)
    add_edge!(g, x, y)
end

pop = Dict(zip(axes(cypher, 1), sort!(cypher)[!,:total]))

pop_pr = sum(values(pop))
equal_dist = round(Int, pop_pr / 40)
max_dev = round(Int, 1.05 * equal_dist)
min_dev = round(Int, 0.95 * equal_dist)

struct SLDL
    blkgrps :: Set{Int}
    pop :: Ref{Int}
    nbr :: Set{Int}
    function SLDL(blkgrps::Set{Int})
        new(blkgrps,
            sum(pop[blkgrp] for blkgrp in blkgrps),
            Set(setdiff(reduce(union, neighborhood(g, blkgrp, 1) for blkgrp in blkgrps), blkgrps)))
    end
end
function push!(obj::SLDL, blkgrp::Integer)
    push!(obj.blkgrps, blkgrp)
    obj.pop.x += pop[blkgrp]
    union!(obj.nbr, setdiff(neighborhood(g, blkgrp, 1), obj.blkgrps))
    obj
end
function isequal(x::SLDL, y::SLDL)
    all(getfield(x, f) == getfield(y, f) for f in fieldnames(SLDL))
end
fieldnames(path)

paths = Set(SLDL(Set(blkgrp)) for blkgrp in keys(pop))
filter!(x -> length(x.nbr) == 1, paths)
filter(path -> length(path.nbr) == 1, paths)

path = first(paths)
for path in paths
    if path.pop.x ≤ 100
        hood = path.nbr
        if length(hood) == 1
            filter!(!isequal(path), paths)
        end
        for nbr in hood
            push!(paths, push!(deepcopy(path), nbr))
        end
        alldone = false
    end
    println(length(paths))
end

while true
    alldone = true
    for path in paths
        # if path.pop.x ≤ min_dev
        if path.pop.x ≤ 100
            for nbr in path.nbr
                push!(paths, push!(deepcopy(path), nbr))
            end
            alldone = false
        end
        println(length(paths))
    end
    alldone && break
end
