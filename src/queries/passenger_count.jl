

"""
    query_passenger_count_frequency(db, table; [plot=false])

Aggregate passenger counts across all trip records. Grouping 
all rides with 7+ passenger in a single group.

```sql
    WITH t1 AS (
        SELECT
            CASE 
                WHEN passenger_count >= 7 THEN '7+'
                WHEN passenger_count IS NULL THEN 'missing'
                ELSE passenger_count
            END AS passengers
        FROM <table>
    )

    SELECT 
        passengers,
        count(*) as cnt
    FROM t1
    GROUP BY
        passengers
    ORDER BY passengers;
```

"""
function query_passenger_count_frequency(db::YellowDB, table; plot=false)
    sql = """
        WITH t1 AS (
            SELECT
                CASE 
                    WHEN passenger_count >= 7 THEN '7+'
                    WHEN passenger_count IS NULL THEN 'missing'
                    ELSE passenger_count
                END AS passengers
            FROM $(table)
        )

        SELECT 
            passengers,
            count(*) as cnt
        FROM t1
        GROUP BY
            passengers
        ORDER BY passengers
        ;
        """

    res = execute(db, sql)
    plt = ifelse(plot==true, plot_passenger_count_frequency(res.df), nothing)
    YellowQueryResult(res.db, res.df, plt, res.runtime, res.sql)
end



"""
    plot_passenger_count_frequency(df)

Plots a barchart for the output from `passenger_count_frequency`
"""
function plot_passenger_count_frequency(df)
    p = df |> @vlplot(mark={:bar, tooltip=true}, 
                x = {:passengers, title="Num Passenger"}, 
                y = {:cnt, title="Num Rides"}, 
                width = 600, height = 200,
                title = "Passenger Counts Per Ride" 
    )
end




"""
    query_passenger_count_outlier(db, table)

Calculate the number of trips with invalid passenger counts.

```sql
    SELECT 
        count(*) as cnt
    FROM <table>
    WHERE 
        passenger_count > 6  OR
        passenger_count <= 0 OR
        passenger_count IS NULL;
```

"""
function query_passenger_count_outlier(db::YellowDB, table)
    sql = """
        SELECT 
            count(*) as cnt
        FROM $(table)
        WHERE 
            passenger_count > 6  OR
            passenger_count <= 0 OR
            passenger_count IS NULL;
    """

    res = execute(db, sql)
    YellowQueryResult(res.db, res.df, nothing, res.runtime, res.sql)
end

