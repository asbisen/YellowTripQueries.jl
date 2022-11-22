
A simple analysis of trip [record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) released 
by Taxi and Limousine Commission of New York City. 


```@setup init
using ..YellowTripQueries
using VegaLite

table = "parquet_scan('/Users/abisen/Desktop/data/tlc/*/*.parquet')"
ydb = YellowDB("/tmp/ydb.db")
```

## About the data

Eleven years of data 2011-2021 was used for this analysis, which consists of ~1.3 Billions 
individual trip records. Each trip record is defined by the following 19 features

```@example init
q = "DESCRIBE SELECT * FROM $(table) LIMIT 1"
result = execute(ydb, q)
result.df[:, 1:3] 
```

As we can observe that the ridership has been in steady decline YoY from 2011. And 
we can also notice the sudden drop in ridership during the COVID-19 pandemic.

```@example init
res1 = query_ridership_yearly(ydb, table; plot=true)
res1.plot
```

```@example init
println("Query Execution Time: $(res1.runtime)")
```

Exploring further the pattern of trips broken by hour and day of week we start
to see a pattern emerge as to the time/days when there is higher than 
average ridership.

```@example init
res2 = query_ridership_week_and_month(ydb, table; plot=true)
res2.plot
```

```@example init
println("Query Execution Time: $(res2.runtime)")
```


Exploring further

```@example init
res2 = query_ridership_day_and_hour(ydb, table; plot=true)
res2.plot
```

```@example init
println("Query Execution Time: $(res2.runtime)")
```



Passenger count frequency

```@example init
res2 = query_passenger_count_frequency(ydb, table; plot=true)
res2.plot
```

```@example init
println("Query Execution Time: $(res2.runtime)")
```


```@example init
res = query_payment_type(ydb, table)
res.df
```
