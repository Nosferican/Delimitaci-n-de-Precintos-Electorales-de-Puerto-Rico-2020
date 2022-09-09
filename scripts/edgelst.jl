using CSV
using DataFrames
using Statistics
import Base: show, getproperty

# Edgelist
edgelist = CSV.read(joinpath(homedir(), "OneDrive", "Desktop", "edgelst.csv"), DataFrame,
                    select = [:src_FID, :nbr_FID, :LENGTH, :GEOID20])
nodes = Dict(eachrow(unique!(select(edgelist, [:src_FID, :GEOID20]))))
select!(edgelist, Not(:GEOID20))
transform!(edgelist,
           :src_FID => ByRow(x -> nodes[x]),
           :nbr_FID => ByRow(x -> nodes[x]),
           renamecols = false)
push!(edgelist, (src_FID = 720371604007, nbr_FID = 720499505002, LENGTH = 100))
push!(edgelist, (src_FID = 720371604007, nbr_FID = 721479506003, LENGTH = 100))
subset!(edgelist, :LENGTH => ByRow(>(0)), [:src_FID, :nbr_FID] => ByRow(<))
select!(edgelist, Not(:LENGTH))
sort!(edgelist)
rename!(edgelist, [:x, :y])
edgelst = Dict(node => Set{Int}() for node in values(nodes))
for (x, y) in eachrow(edgelist)
    push!(edgelst[x], y)
    push!(edgelst[y], x)
end
# Population
pop = CSV.read(joinpath("data", "pop.csv"), DataFrame)
transform!(pop,
           :BLOCK => ByRow(x -> x ÷ 1000) => :bg,
           :BLOCK => ByRow(x -> rem(x, 1_000)) => :blk)
rename!(pop, :COUNTY => :cty, :TRACT => :ct,
             :P0010001 => :total,
             :P0030001 => :adult,
             :P0050001 => :grp_quarters)
transform!(pop,
           [:cty, :ct, :bg] =>
             ByRow((cty, ct, bg) -> parse(Int, string("72", lpad(cty, 3, '0'), rpad(ct, 6, '0'), bg))) => :id)
pop = Dict(pop[!,:id] .=> pop[!,:total])
# Block Groups
"""
    BlockGroup
"""
struct BlockGroup
    state     :: Int
    county    :: Int
    tract     :: Int
    group     :: Int
    pop       :: Int
    nbh       :: Set{Int}
    function BlockGroup(id::Integer,
                        pop::Dict{Int,Int},
                        nbh::Dict{Int,Set{Int}})
        x = digits(id)
        group = x[1]
        tract = sum(x[k] * 10^(k - 2) for k in 2:7)
        county = sum(x[k] * 10^(k - 8) for k in 8:10)
        state = sum(x[k] * 10^(k - 11) for k in 11:12)
        pop = get(pop, id, 0)
        nbh = get(nbh, id, Set{Int}())
        new(state, county, tract, group, pop, nbh)
    end
end
census_id(obj::BlockGroup) =
    convert(Int, obj.state * 1e10 + obj.county * 1e7 + obj.tract * 1e1 + obj.group)
show(io::IO, obj::BlockGroup) = print(io, census_id(obj))

data = BlockGroup.(values(nodes), Ref(pop), Ref(edgelst))

struct SLDL
    blkgrps :: Vector{BlockGroup}
end
function getproperty(obj::SLDL, sym::Symbol)
    if sym == :pop
        sum(blkgrp.pop for blkgrp in obj.blkgrps)
    elseif sym == :nbh
        reduce(union, blkgrp.nbh for blkgrp in obj.blkgrps)
    else
        getfield(obj, sym)
    end
end
data[1].pop
data[2].pop
chk = SLDL([data[1]])



chk.nbh





nodes = Dict(eachrow(unique!(select(edgelist, [:src_FID, :GEOID20]))))
mapping = Dict(sort!(collect(keys(nodes))) .=> 1:length(nodes))
nodes_rev = Dict(values(nodes) .=> keys(nodes))
# Add island ferries
nodes_rev[720371604007]
push!(edgelst_clean, (x = nodes_rev[720371604007], y = nodes_rev[720499505002], LENGTH = 100))
push!(edgelst_clean, (x = nodes_rev[720371604007], y = nodes_rev[721479506003], LENGTH = 100))
sort!(unique!(edgelst_clean), order(:LENGTH))
select!(edgelist, Not(:LENGTH))
rename!(edgelist, [:x, :y])


edgelist = CSV.read(joinpath(homedir(), "OneDrive", "Desktop", "edgelst.csv"), DataFrame,
                    select = [:src_FID, :nbr_FID, :LENGTH, :GEOID20])
nodes = Dict(eachrow(unique!(select(edgelist, [:src_FID, :GEOID20]))))
mapping = Dict(sort!(collect(keys(nodes))) .=> 1:length(nodes))
nodes_rev = Dict(values(nodes) .=> keys(nodes))
# Add island ferries
nodes_rev[720371604007]
push!(edgelst_clean, (x = nodes_rev[720371604007], y = nodes_rev[720499505002], LENGTH = 100))
push!(edgelst_clean, (x = nodes_rev[720371604007], y = nodes_rev[721479506003], LENGTH = 100))
sort!(unique!(edgelst_clean), order(:LENGTH))

transform!(edgelist,
           :src_FID => ByRow(x -> mapping[x]),
           :nbr_FID => ByRow(x -> mapping[x]),
           renamecols = false)
sort!(edgelist)
subset!(edgelist, :LENGTH => ByRow(>(0)))
select!(edgelist, :src_FID, :nbr_FID)
rename!(edgelist, [:x, :y])

g = SimpleGraph(length(mapping))
for (x, y) in eachrow(edgelist)
    add_edge!(g, (x, y))
end

districts = Vector{Vector{Int}}()
for v in vertices(g)
    nbr = neighbors(g, v)
    if length(nbr) == 1
        push!(districts, [v, only(nbr)])
    end
end
g′ = SimpleGraph(length(mapping))
for district in districts
    add_edge!(g′, district...)
end



nodes[findfirst(isequal(mapping[1]), nodes[:src_FID]),:]

sort!(nodes, :GEOID20)
nodes[!,:id] .= 1:size(nodes, 1)
mapping = Dict(nodes[!,:src_FID] .=> eachindex(nodes[!,:src_FID]))
transform!(edgelist, :src_FID => ByRow(x -> )))
edgelist

nodes = sort!(unique!(vcat(edgelist[!,:GEOID20], edgelist[!,:GEOID20])))
edgelst = unique(select(edgelist, :src_FID, :nbr_FID, :LENGTH))
subset!(edgelst, [:src_FID, :nbr_FID] => ByRow(<))

mapping = unique(select(edgelist, :src_FID, :GEOID20), )
edgelst_clean = select(edgelst, [:src_FID, :nbr_FID, :LENGTH])
edgelst_clean = innerjoin(edgelst_clean, mapping, on = :src_FID)
rename!(edgelst_clean, :GEOID20 => :x)
edgelst_clean = innerjoin(edgelst_clean, mapping, on = :nbr_FID => :src_FID)
rename!(edgelst_clean, :GEOID20 => :y)
select!(edgelst_clean, [:x, :y, :LENGTH])
subset!(edgelst_clean, :LENGTH => ByRow(>(0)))
# Add island ferries
push!(edgelst_clean, (x = 720371604007, y = 720499505002, LENGTH = 100))
push!(edgelst_clean, (x = 720371604007, y = 721479506003, LENGTH = 100))
sort!(unique!(edgelst_clean), order(:LENGTH))

g = SimpleGraph(size(mapping, 1))

"""
    BlockGroup
"""
struct BlockGroup
    state     :: Int
    county    :: Int
    tract     :: Int
    group     :: Int
    pop       :: Int
    function BlockGroup(id::Integer,
                        pop::Dict{Int,Int})
        x = digits(id)
        group = x[1]
        tract = sum(x[k] * 10^(k - 2) for k in 2:7)
        county = sum(x[k] * 10^(k - 8) for k in 8:10)
        state = sum(x[k] * 10^(k - 11) for k in 11:12)
        pop = get(pop, id, 0)
        new(state, county, tract, group, pop)
    end
end
census_id(obj::BlockGroup) =
    convert(Int, obj.state * 1e10 + obj.county * 1e7 + obj.tract * 1e1 + obj.group)
show(io::IO, obj::BlockGroup) = print(io, census_id(obj))

struct SLDL
    blkgrps :: Vector{BlockGroup}
end
function getproperty(obj::SLDL, sym::Symbol)
    if sym == :pop
        sum(blkgrp.pop for blkgrp in obj.blkgrps)
    else
        getfield(obj, sym)
    end
end

function neighborhood(obj::SLDL, edgelst::AbstractVector{Int})
    union()
end

pop = CSV.read(joinpath("data", "pop.csv"), DataFrame)
transform!(pop,
           :BLOCK => ByRow(x -> x ÷ 1000) => :bg,
           :BLOCK => ByRow(x -> rem(x, 1_000)) => :blk)
rename!(pop, :COUNTY => :cty, :TRACT => :ct,
             :P0010001 => :total,
             :P0030001 => :adult,
             :P0050001 => :grp_quarters)
transform!(pop,
           [:cty, :ct, :bg] =>
             ByRow((cty, ct, bg) -> parse(Int, string("72", lpad(cty, 3, '0'), rpad(ct, 6, '0'), bg))) => :id)
# quantile(edgelst[!,:LENGTH], 0:0.01:1)
pop = Dict(pop[!,:id] .=> pop[!,:total])

edgelst = CSV.read(joinpath(homedir(), "OneDrive", "Desktop", "edgelst.csv"), DataFrame)

edgelst = unique(select(edgelst, :src_FID, :nbr_FID, :LENGTH))
subset!(edgelst, [:src_FID, :nbr_FID] => ByRow(<))
mapping = unique(select(edgelst, :src_FID, :GEOID20))
edgelst_clean = select(edgelst, [:src_FID, :nbr_FID, :LENGTH])
edgelst_clean = innerjoin(edgelst_clean, mapping, on = :src_FID)
rename!(edgelst_clean, :GEOID20 => :x)
edgelst_clean = innerjoin(edgelst_clean, mapping, on = :nbr_FID => :src_FID)
rename!(edgelst_clean, :GEOID20 => :y)
select!(edgelst_clean, [:x, :y, :LENGTH])
subset!(edgelst_clean, :LENGTH => ByRow(>(0)))
# Add island ferries
push!(edgelst_clean, (x = 720371604007, y = 720499505002, LENGTH = 100))
push!(edgelst_clean, (x = 720371604007, y = 721479506003, LENGTH = 100))
sort!(unique!(edgelst_clean), order(:LENGTH))

transform!(edgelst_clean,
           :x => ByRow(x -> BlockGroup(x, pop)),
           :y => ByRow(x -> BlockGroup(x, pop)),
           renamecols = false)

all_blkgrp = sort!(unique(vcat(edgelst_clean[!,:x], edgelst_clean[!,:y])))
edges_dict = Dict(blkgrp => Set(Int[]) for blkgrp in all_blkgrp)
for row in eachrow(edgelst_clean)
    x = row.x
    y = row.y
    push!(edges_dict[x], y)
    push!(edges_dict[y], x)
end

maximum(edgelst_clean[!,:LENGTH])

unique(vcat(edgelst_clean[!,1], edgelst_clean[!,2]))

src = combine(nrow, groupby(edgelst_clean, :x))
nbr = combine(nrow, groupby(edgelst_clean, :y))
rename!(nbr, :y => :x)

chk = sort!(combine(row -> sum(row.nrow), groupby(vcat(src, nbr), :x)), :x1)
count(isone, chk[!,2])




convert(Int, 72 * 1e10 + 21 * 1e7 + 030101 * 1e1 + 1) == (720210301011)

blkgrps = BlockGroup.(all_blkgrp, Ref(pop), Ref(edges_dict))
id = all_blkgrp[1]
neighbors = edges_dict
chk = BlockGroup(all_blkgrp[1], pop, edges_dict)
typeof(all_blkgrp[1])
typeof(pop)
typeof(edges_dict)
edges_dict[720210301011]
pop[720210301011]
extrema(length, values(edges_dict))
