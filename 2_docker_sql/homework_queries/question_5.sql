/*
Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?

Consider only lpep_pickup_datetime when filtering by date.
*/

SELECT
	z."Zone"
	,COUNT(1)
FROM
	green_taxi_trips AS t
JOIN
	green_taxi_zones AS z ON t."PULocationID" = z."LocationID"
WHERE
	DATE(t.lpep_pickup_datetime) = DATE '2019-10-18'
GROUP BY 1
HAVING SUM(t.total_amount) > 13000
ORDER BY 2 DESC