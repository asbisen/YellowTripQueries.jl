


"""
    query_payment_type(db, table)

What percentage of trips were paid with Cash vs. Credit

```sql
    WITH t1 AS (
    SELECT 
        payment_type,
        CASE 
            WHEN payment_type = 1 THEN 'Card'
            WHEN payment_type = 2 THEN 'Cash'
            WHEN payment_type = 3 THEN 'No Charge'
            WHEN payment_type = 4 THEN 'Dispute'
            WHEN payment_type = 5 THEN 'Unknown'
            WHEN payment_type = 6 THEN 'Voided Trip'
            ELSE 'Unknown'
        END AS ptype,
        count(*) AS cnt
    FROM 
        <table>
    WHERE   
        $(def_filter)
    GROUP BY payment_type
    ),

    t2 AS (
        SELECT sum(cnt) AS total
        FROM t1
    )

    SELECT ptype, (CAST(cnt AS FLOAT)/total) * 100 AS pct
    FROM t1, t2
    ;
```

"""
function query_payment_type(db::YellowDB, table; 
                            where_filter::Union{Nothing, AbstractString}=def_filter
                            )

    where_filter = ifelse(def_filter===nothing, "", "WHERE $(where_filter)")
    
    sql = """
        WITH t1 AS (
            SELECT 
                payment_type,
                CASE 
                    WHEN payment_type = 1 THEN 'Card'
                    WHEN payment_type = 2 THEN 'Cash'
                    WHEN payment_type = 3 THEN 'No Charge'
                    WHEN payment_type = 4 THEN 'Dispute'
                    WHEN payment_type = 5 THEN 'Unknown'
                    WHEN payment_type = 6 THEN 'Voided Trip'
                    ELSE 'Unknown'
                END AS ptype,
                count(*) AS cnt
            FROM $(table)
            
            $(where_filter)

            GROUP BY payment_type
        ),

        t2 AS (
            SELECT sum(cnt) AS total
            FROM t1
        )

        SELECT ptype, (CAST(cnt AS FLOAT)/total) * 100 AS pct
        FROM t1, t2
        ;
    """

    res = execute(db, sql)
    YellowQueryResult(res.db, res.df, nothing, res.runtime, res.sql)
end





"""
    query_tip_pct_yoy(db, table)

Calculate ratio of rides with/without tips over the years.

```sql
    WITH t1 AS (
        SELECT 
            CASE 
                WHEN tip_amount > 0 THEN 1
                ELSE 0
            END AS tipped,
            datepart('year', tpep_pickup_datetime) as PickupYear,
            count(*) AS cnt,
        FROM 
            <table>
        WHERE date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime)     > 0     AND
            date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) < 43200 AND
            passenger_count <= 5         AND
            passenger_count > 0          AND
            passenger_count IS NOT NULL  AND
            trip_distance > 0            AND 
            trip_distance < 200          AND
            datepart('year', tpep_pickup_datetime) > 2010 AND
            datepart('year', tpep_pickup_datetime) < 2022
        GROUP BY PickupYear, tipped
    )

    SELECT PickupYear, tipped, 
        cnt, CAST(cnt AS FLOAT)/(sum(cnt) OVER (PARTITION BY PickupYear)) * 100 AS pct
    FROM t1;
```

"""
function query_tip_pct_yoy(db::YellowDB, table)
    sql = """
        WITH t1 AS (
            SELECT 
                CASE 
                    WHEN tip_amount > 0 THEN 1
                    ELSE 0
                END AS tipped,
                datepart('year', tpep_pickup_datetime) as PickupYear,
                count(*) AS cnt,
            FROM 
                $(table)
            WHERE date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime)     > 0     AND
                date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) < 43200 AND
                passenger_count <= 5         AND
                passenger_count > 0          AND
                passenger_count IS NOT NULL  AND
                trip_distance > 0            AND 
                trip_distance < 200          AND
                datepart('year', tpep_pickup_datetime) > 2010 AND
                datepart('year', tpep_pickup_datetime) < 2022
            GROUP BY PickupYear, tipped
        )

        SELECT PickupYear, tipped, 
            cnt, CAST(cnt AS FLOAT)/(sum(cnt) OVER (PARTITION BY PickupYear)) * 100 AS pct
        FROM t1;
    """
    res = execute(db, sql)
    YellowQueryResult(res.db, res.df, nothing, res.runtime, res.sql)
end




"""
    query_tip_ratio_payment_type(dbname, table)
"""
function query_tip_ratio_payment_type(db::YellowDB, table; plot=true)

    sql = """
        WITH t1 AS (
            SELECT 
                CASE 
                    WHEN tip_amount > 0 THEN 1
                    ELSE 0
                END AS tipped,
                datepart('year', tpep_pickup_datetime) as PickupYear,
                CASE 
                    WHEN payment_type = 1 THEN 'Card'
                    WHEN payment_type = 2 THEN 'Cash'
                    WHEN payment_type = 3 THEN 'No Charge'
                    WHEN payment_type = 4 THEN 'Dispute'
                    WHEN payment_type = 5 THEN 'Unknown'
                    WHEN payment_type = 6 THEN 'Voided Trip'
                    ELSE 'Unknown'
                END AS ptype,
                count(*) AS cnt,
            FROM 
                $(table)
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
            GROUP BY PickupYear, ptype, tipped 
            )

        SELECT PickupYear, tipped, ptype, cnt,
               CAST(cnt AS FLOAT)/(sum(cnt) OVER (PARTITION BY PickupYear, ptype)) * 100 AS pct
        FROM t1
        ;
    """

    res = execute(db, sql)
    plt = ifelse(plot==true, plot_tip_ratio_payment_type(res.df), nothing)

    YellowQueryResult(res.db, res.df, plt, res.runtime, res.sql)
end



function plot_tip_ratio_payment_type(df; width=300, height=400)
    p = df |> @vlplot(
            mark = {:bar, tooltip=true},
            x = {:PickupYear, type="ordinal", title = "Year"},
            y = {:pct},
            color = {:tipped, type="nominal"},
            column={:ptype, type="ordinal", title="Payment Type"},
            title="Tiping Percentage for Payment Type",
            width=width, height=height
    )
end