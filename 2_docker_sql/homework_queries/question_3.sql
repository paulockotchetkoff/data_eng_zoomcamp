/*
During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

Up to 1 mile
In between 1 (exclusive) and 3 miles (inclusive),
In between 3 (exclusive) and 7 miles (inclusive),
In between 7 (exclusive) and 10 miles (inclusive),
Over 10 miles
*/

SELECT
	CASE
		WHEN trip_distance <= 1 THEN 'Up to 1 mile'
		WHEN trip_distance <= 3 THEN 'In between 1 (exclusive) and 3 miles (inclusive)'
		WHEN trip_distance <= 7 THEN 'In between 3 (exclusive) and 7 miles (inclusive)'
		WHEN trip_distance <= 10 THEN 'In between 7 (exclusive) and 10 miles (inclusive)'
		WHEN trip_distance > 10 THEN 'Over 10 miles'
	END AS trip_distance_bin
	,COUNT(1) AS trips
FROM
	green_taxi_trips AS t
WHERE
	DATE_TRUNC('MONTH', lpep_pickup_datetime) = DATE '2019-10-01'
AND
	DATE_TRUNC('MONTH', lpep_dropoff_datetime) = DATE '2019-10-01'
GROUP BY 1