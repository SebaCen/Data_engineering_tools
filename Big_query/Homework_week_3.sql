#1
SELECT
  COUNT(1)
FROM
  `green_data_taxi.green_native_date` ;

#2
SELECT
  COUNT(DISTINCT(PULocationID))
FROM
  `green_data_taxi.green_native_date` ;
SELECT
  COUNT(DISTINCT(PULocationID))
FROM
  `green_data_taxi.green_external_date` ; 

#3
SELECT
  COUNT(1)
FROM
  `green_data_taxi.green_native_date`
WHERE
  fare_amount=0 ; 

#4
CREATE OR REPLACE TABLE
  `green_data_taxi.green_native_date_partitioned_and_clustered`
PARTITION BY
  DATE(lpep_pickup_datetime)
CLUSTER BY
  PUlocationID AS
SELECT
  *
FROM
  `green_data_taxi.green_native_date` ; 
  
#5
SELECT
  DISTINCT(PULocationID)
FROM
  `green_data_taxi.green_native_date`
WHERE
  lpep_pickup_datetime BETWEEN "2022-06-01"
  AND "2011-06-30" ;
SELECT
  DISTINCT(PULocationID)
FROM
  `green_data_taxi.green_native_date_partitioned_and_clustered`
WHERE
  lpep_pickup_datetime BETWEEN "2022-06-01"
  AND "2011-06-30" ; 


#6: GCP bucket

#7: TRUE

#8
SELECT
  COUNT(*)
FROM
  `green_data_taxi.green_native_date` ;