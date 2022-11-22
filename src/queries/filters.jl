

# Default Filter 
def_filter = 
"""        
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
"""