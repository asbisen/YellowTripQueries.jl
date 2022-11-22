"""
    query_ridership_yearly(db, table; [plot=false])

Rollup of yearly ridership of NYC YellowCab

```sql
    SELECT 
        datepart('year', tpep_pickup_datetime) AS Year, 
        count(*) AS nRides 
    FROM 
        <table> 
    WHERE 
        Year >= 2011 AND 
        Year <= 2021 
    GROUP BY 
        Year 
    ORDER BY Year
    ;
```

"""
function query_ridership_yearly(db::YellowDB, table; plot=true)
    sql = """
        SELECT 
            datepart('year', tpep_pickup_datetime) AS Year, 
            count(*) AS nRides 
        FROM 
            $(table) 
        WHERE 
            Year >= 2011 AND 
            Year <= 2021 
        GROUP BY 
            Year 
        ORDER BY Year
        ;
    """

    res = execute(db, sql)

    plt = ifelse(plot==true, plot_ridership_yearly(res.df), nothing)
    YellowQueryResult(res.db, res.df, plt, res.runtime, res.sql)
end



function plot_ridership_yearly(df; width=600, height=200)
    p = df |> @vlplot(
                    mark = {:bar, tooltip=true},
                    x="year:o", 
                    y={:nRides},
                    width = width, height = height,
                    title= "Ridership by Year"
                )
    p
end





"""
    query_ridership_week_and_month(db, table; plot=true)

Aggregate number of rides by day of week and month of year across
the entire dataset. If plot=true then also generate a Heatmap to 
show the pattern of ridership demand across the groups.

```sql
    SELECT
        CASE 
            WHEN datepart('dow', tpep_pickup_datetime) = 0 THEN 'Sun'
            WHEN datepart('dow', tpep_pickup_datetime) = 1 THEN 'Mon'
            WHEN datepart('dow', tpep_pickup_datetime) = 2 THEN 'Tue'
            WHEN datepart('dow', tpep_pickup_datetime) = 3 THEN 'Wed'
            WHEN datepart('dow', tpep_pickup_datetime) = 4 THEN 'Thu'
            WHEN datepart('dow', tpep_pickup_datetime) = 5 THEN 'Fri'
            WHEN datepart('dow', tpep_pickup_datetime) = 6 THEN 'Sat'
        END AS DOW,

        CASE
            WHEN datepart('month', tpep_pickup_datetime) = 1  THEN 'Jan'
            WHEN datepart('month', tpep_pickup_datetime) = 2  THEN 'Feb'
            WHEN datepart('month', tpep_pickup_datetime) = 3  THEN 'Mar'
            WHEN datepart('month', tpep_pickup_datetime) = 4  THEN 'Apr'
            WHEN datepart('month', tpep_pickup_datetime) = 5  THEN 'May'
            WHEN datepart('month', tpep_pickup_datetime) = 6  THEN 'Jun'
            WHEN datepart('month', tpep_pickup_datetime) = 7  THEN 'Jul'
            WHEN datepart('month', tpep_pickup_datetime) = 8  THEN 'Aug'
            WHEN datepart('month', tpep_pickup_datetime) = 9  THEN 'Sep'
            WHEN datepart('month', tpep_pickup_datetime) = 10 THEN 'Oct'
            WHEN datepart('month', tpep_pickup_datetime) = 11 THEN 'Nov'
            WHEN datepart('month', tpep_pickup_datetime) = 12 THEN 'Dec'
        END AS Month,
        count(*) as cnt
    FROM <table>
    WHERE
        date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 0     AND
        date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) < 43200 AND
        passenger_count <= 6         AND
        passenger_count >  0         AND
        passenger_count IS NOT NULL  AND
        -- assumption: trip distances greater than 100 miles 
        -- must be for out of town hire
        trip_distance > 0            AND 
        trip_distance < 100          AND
        -- filter out non card / cash transactions
        payment_type <= 2            AND             
        -- filter out some records with incorrect date time 
        -- that falls outside the dataset range
        datepart('year', tpep_pickup_datetime) > 2010 AND
        datepart('year', tpep_pickup_datetime) < 2022
    GROUP BY DOW, Month;
```

"""
function query_ridership_week_and_month(db::YellowDB, table; plot=false)
    sql = """
        SELECT
            CASE 
                WHEN datepart('dow', tpep_pickup_datetime) = 0 THEN 'Sun'
                WHEN datepart('dow', tpep_pickup_datetime) = 1 THEN 'Mon'
                WHEN datepart('dow', tpep_pickup_datetime) = 2 THEN 'Tue'
                WHEN datepart('dow', tpep_pickup_datetime) = 3 THEN 'Wed'
                WHEN datepart('dow', tpep_pickup_datetime) = 4 THEN 'Thu'
                WHEN datepart('dow', tpep_pickup_datetime) = 5 THEN 'Fri'
                WHEN datepart('dow', tpep_pickup_datetime) = 6 THEN 'Sat'
            END AS DOW,

            CASE
                WHEN datepart('month', tpep_pickup_datetime) = 1  THEN 'Jan'
                WHEN datepart('month', tpep_pickup_datetime) = 2  THEN 'Feb'
                WHEN datepart('month', tpep_pickup_datetime) = 3  THEN 'Mar'
                WHEN datepart('month', tpep_pickup_datetime) = 4  THEN 'Apr'
                WHEN datepart('month', tpep_pickup_datetime) = 5  THEN 'May'
                WHEN datepart('month', tpep_pickup_datetime) = 6  THEN 'Jun'
                WHEN datepart('month', tpep_pickup_datetime) = 7  THEN 'Jul'
                WHEN datepart('month', tpep_pickup_datetime) = 8  THEN 'Aug'
                WHEN datepart('month', tpep_pickup_datetime) = 9  THEN 'Sep'
                WHEN datepart('month', tpep_pickup_datetime) = 10 THEN 'Oct'
                WHEN datepart('month', tpep_pickup_datetime) = 11 THEN 'Nov'
                WHEN datepart('month', tpep_pickup_datetime) = 12 THEN 'Dec'
            END AS Month,
            count(*) as cnt
        FROM $(table)
        WHERE
            date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 0     AND
            date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) < 43200 AND
            passenger_count <= 6         AND
            passenger_count >  0         AND
            passenger_count IS NOT NULL  AND
            -- assumption: trip distances greater than 100 miles 
            -- must be for out of town hire
            trip_distance > 0            AND 
            trip_distance < 100          AND
            -- filter out non card / cash transactions
            payment_type <= 2            AND             
            -- filter out some records with incorrect date time 
            -- that falls outside the dataset range
            datepart('year', tpep_pickup_datetime) > 2010 AND
            datepart('year', tpep_pickup_datetime) < 2022
        GROUP BY DOW, Month
    ;
    """

    res = execute(db, sql)
    plt = ifelse(plot==true, plot_ridership_week_and_month(res.df), nothing)
    YellowQueryResult(res.db, res.df, plt, res.runtime, res.sql)
end



function plot_ridership_week_and_month(df; width=600, height=400)
    p = df |> @vlplot(
                    mark={:rect, tooltip=true}, 
                    x={:month, type="ordinal",
                    sort = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                    }, 
                    y={:DOW, type="ordinal",
                        sort = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                    },  
                    color={:cnt, title="Pickups"}, 
                    width=width, height=height,
                    title="Pickup Demand by Month & Weekday"
    )
    p
end




"""
    query_ridership_day_and_hour(db, table; plot=false)

Aggregate the data by day of ween and hour of day across the 
entire dataset. If plot=true then also generate a heatmap displaying
the pattern of ridership in a week. 

```sql
    SELECT
        CASE 
            WHEN datepart('dow', tpep_pickup_datetime) = 0 THEN 'Sun'
            WHEN datepart('dow', tpep_pickup_datetime) = 1 THEN 'Mon'
            WHEN datepart('dow', tpep_pickup_datetime) = 2 THEN 'Tue'
            WHEN datepart('dow', tpep_pickup_datetime) = 3 THEN 'Wed'
            WHEN datepart('dow', tpep_pickup_datetime) = 4 THEN 'Thu'
            WHEN datepart('dow', tpep_pickup_datetime) = 5 THEN 'Fri'
            WHEN datepart('dow', tpep_pickup_datetime) = 6 THEN 'Sat'
        END AS DOW,
        datepart('hour', tpep_pickup_datetime) AS PickupHour,
        count(*) as cnt
    FROM <table>
    WHERE
        date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 0     AND
        date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) < 43200 AND
        passenger_count <= 6         AND
        passenger_count >  0         AND
        passenger_count IS NOT NULL  AND
        -- assumption: trip distances greater than 100 miles 
        -- must be for out of town hire
        trip_distance > 0            AND 
        trip_distance < 100          AND
        -- filter out non card / cash transactions
        payment_type <= 2            AND             
        -- filter out some records with incorrect date time 
        -- that falls outside the dataset range
        datepart('year', tpep_pickup_datetime) > 2010 AND
        datepart('year', tpep_pickup_datetime) < 2022
    GROUP BY DOW, PickupHour
    ;
```
"""
function query_ridership_day_and_hour(db::YellowDB, table; plot=false)
    sql = """
        SELECT
            CASE 
                WHEN datepart('dow', tpep_pickup_datetime) = 0 THEN 'Sun'
                WHEN datepart('dow', tpep_pickup_datetime) = 1 THEN 'Mon'
                WHEN datepart('dow', tpep_pickup_datetime) = 2 THEN 'Tue'
                WHEN datepart('dow', tpep_pickup_datetime) = 3 THEN 'Wed'
                WHEN datepart('dow', tpep_pickup_datetime) = 4 THEN 'Thu'
                WHEN datepart('dow', tpep_pickup_datetime) = 5 THEN 'Fri'
                WHEN datepart('dow', tpep_pickup_datetime) = 6 THEN 'Sat'
            END AS DOW,
            datepart('hour', tpep_pickup_datetime) AS PickupHour,
            count(*) as cnt
        FROM $(table)
        WHERE
            date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 0     AND
            date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) < 43200 AND
            passenger_count <= 6         AND
            passenger_count >  0         AND
            passenger_count IS NOT NULL  AND
            -- assumption: trip distances greater than 100 miles 
            -- must be for out of town hire
            trip_distance > 0            AND 
            trip_distance < 100          AND
            -- filter out non card / cash transactions
            payment_type <= 2            AND             
            -- filter out some records with incorrect date time 
            -- that falls outside the dataset range
            datepart('year', tpep_pickup_datetime) > 2010 AND
            datepart('year', tpep_pickup_datetime) < 2022
        GROUP BY DOW, PickupHour
    ;
    """

    res = execute(db, sql)
    plt = ifelse(plot==true, plot_ridership_day_and_hour(res.df), nothing)
    YellowQueryResult(res.db, res.df, plt, res.runtime, res.sql)
end


function plot_ridership_day_and_hour(df; width=600, height=400)
    p = df |> @vlplot(
                    mark={:rect, tooltip=true}, 
                    x={:DOW, type="ordinal",
                        sort = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                    },  
                    y={:PickupHour, type="ordinal"}, 
                    color={:cnt, title="Pickups"}, 
                    width=width, height=height,
                    title="Pickup Demand by Day & Hour"
    )
    p
end
