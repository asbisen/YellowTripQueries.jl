module YellowTripQueries

using DuckDB
using DataFrames
using VegaLite


include("database.jl")
include("utils.jl")
include("queries/filters.jl")
include("queries/passenger_count.jl")
include("queries/payments_and_tips.jl")
include("queries/ridership.jl")

export  
        # database.jl
        YellowDB,
        execute,
        close!,
        connect,  

        # passenger_count.jl
        query_passenger_count_frequency,
        query_passenger_count_outlier,

        # 
        query_payment_type,
        query_tip_pct_yoy,
        query_ridership_yearly,
        query_ridership_week_and_month,
        query_ridership_day_and_hour,
        query_tip_ratio_payment_type
        ;
    




end # module YellowTripQueries