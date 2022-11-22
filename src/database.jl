

"""
    YellowDB(dbname; [access_mode, memory_limit, temp_directory])

`access_mode`: can be either "READ_ONLY", "READ_WRITE", "AUTOMATIC".
`memory_limit`: By default 75% of available memory. 
`temp_directory`: Use this location for spilling to disk for out of core computation.

Stucture to hold parameters to establish a connection with 
a DuckDB database.


## Example

```julia
ydb = YellowDB("/tmp/db.duckdb"; [access_mode, memory_limit, temp_directory])
```
"""
struct YellowDB
    db::AbstractString
    access_mode::Union{AbstractString, Nothing}
    memory_limit::Union{AbstractString, Nothing}
    temp_directory::Union{AbstractString, Nothing}
end

function YellowDB(db::AbstractString; 
                    access_mode::AbstractString="READ_WRITE",
                    memory_limit::Union{Nothing, AbstractString}=nothing,
                    temp_directory::Union{Nothing, AbstractString}=nothing
                    )

    (access_mode in ["READ_WRITE", "READ_ONLY", "AUTOMATIC"]) || error("access mode can be either READ_WRITE, READ_ONLY or AUTOMATIC")
    YellowDB(db, access_mode, memory_limit, temp_directory)
end


function connect(db::YellowDB) 
    dbconfig = DuckDB.Config()

    (db.memory_limit === nothing)   || DuckDB.set_config(dbconfig, "memory_limit", db.memory_limit)
    (db.temp_directory === nothing) || DuckDB.set_config(dbconfig, "temp_directory", db.temp_directory)
    (db.access_mode === nothing)    || DuckDB.set_config(dbconfig, "access_mode", db.access_mode)    
    
    DBInterface.connect(DuckDB.DB, db.db, dbconfig)
end


close!(conn::DuckDB.DB) = DBInterface.close!(conn)






"""
    YellowQueryResult

Structure to hold results from a query to NYC Yellow Taxi Dataset. Each query
defined in this package is an analysis which may be complemented by a visualization. 

This structure hold the original query that was executed `query` along with the time
it took to execute the query `runtime`
"""
struct YellowQueryResult
    db::YellowDB
    df::Union{Nothing, DataFrame}
    plot::Union{Nothing, VegaLite.VLSpec}
    runtime::Union{Nothing, Float64}
    sql::AbstractString
end



function _execute(db::YellowDB, sql::AbstractString)
    conn = connect(db)
    res, runtime, _ = @timed DBInterface.execute(conn, sql)
    close!(conn)
    return(res=res, runtime=runtime)
end


"""
    execute(db, sql)

A simple wrapper to DuckDB's `DBInterface.execute(DuckDB.DB, query)` interface. The 
biggest difference is that this version opens up a new connection to the database 
and closes it after fetching the results.

Returns: YellowQueryResult
"""
function execute(db::YellowDB, sql::AbstractString)
    (res, runtime) = _execute(db, sql)
    return YellowQueryResult(db, res.df, nothing, runtime, sql)
end


function Base.show(io::IO, m::MIME"text/html", r::YellowTripQueries.YellowQueryResult)
    show(r.df)
    println("\n")
    println("Execution Time: $(r.runtime)")
end