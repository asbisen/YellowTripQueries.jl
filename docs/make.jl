push!(LOAD_PATH,"../src/")

using Documenter
using YellowTripQueries
using VegaLite
using UUIDs


function Base.show(io::IO, m::MIME"text/html", v::VegaLite.VLSpec)
    divid = string("vl", replace(string(uuid4()), "-" => ""))
    print(io, "<div id='$divid' style=\"width:100%;height:100%;\"></div>")
    print(io, "<script type='text/javascript'>requirejs.config({paths:{'vg-embed': 'https://cdn.jsdelivr.net/npm/vega-embed@6?noext','vega-lib': 'https://cdn.jsdelivr.net/npm/vega-lib?noext','vega-lite': 'https://cdn.jsdelivr.net/npm/vega-lite@4?noext','vega': 'https://cdn.jsdelivr.net/npm/vega@5?noext'}}); require(['vg-embed'],function(vegaEmbed){vegaEmbed('#$divid',")    
    VegaLite.our_json_print(io, v)
    print(io, ",{mode:'vega-lite'}).catch(console.warn);})</script>")
end


makedocs(
  modules=[YellowTripQueries],
  sitename="Yellow Trip Analysis",
  pages=[
    "Home" => "index.md",
    "Queries" => Any[
        "Queries" => "queries/queries.md",
    ]
  ]
)

