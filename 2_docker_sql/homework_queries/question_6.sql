/*
For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?

Note: it's tip , not trip
*/

SELECT
	doz."Zone"
	,COUNT(1)
FROM
	green_taxi_trips AS t
JOIN
	green_taxi_zones AS puz ON t."PULocationID" = puz."LocationID"
JOIN
	green_taxi_zones AS doz ON t."PULocationID" = doz."LocationID"
WHERE
	DATE_TRUNC('MONTH', t.lpep_pickup_datetime) = DATE '2019-10-01'
AND
	puz."Zone" = 'East Harlem North'
GROUP BY 1
ORDER BY 2 DESC