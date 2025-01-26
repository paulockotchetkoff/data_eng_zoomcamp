/*
Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.
*/

SELECT
	*
FROM
	green_taxi_trips AS t
WHERE
	trip_distance = (SELECT MAX(trip_distance) FROM green_taxi_trips)