-- CONNECTION: name=postgres
CREATE TABLE information_mart.trips_statistics_month_most_trips_per_year AS 
	SELECT 
		puzone.zonename AS "Pickup Zone Name", 
		dozone.zonename AS "Dropoff Zone Name", 
		TO_CHAR(TO_DATE (CAST(EXTRACT(MONTH FROM td.timestring) AS TEXT), 'MM'), 'Month') AS "Month Name",
	    EXTRACT(YEAR FROM td.timestring) AS "Year",

		avg(total_amount) AS "Average Total Amount",
		PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_amount) AS "3rd Quartile Total Amount", 
		PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount) AS "Median Total Amount",
		PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_amount) AS "1st Quartile Total Amount",
		min(total_amount) AS "Minimum Total Amount", 
		max(total_amount) AS "Maximum Total Amount", 
		sum(total_amount) AS "Sum Total Amount",

		count(*) AS "Count", 

		avg(passengercount) AS "Average Passsenger Count", 
		sum(passengercount) AS "Total Number of Passenger",

		sum(trip_distance) AS "Total Trip Distance",
		avg(trip_distance) AS "Average Trip Distance",

		PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance) AS "3rd Quartile Trip Distance",
		PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_distance) AS "Median Trip Distance",
		PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance) AS "1st Quartile Trip Distance",
		min(trip_distance) AS "Minimum Trip Distance", 
		max(trip_distance) AS "Maximum Trip Distance",

		(SELECT 
			rcd.ratecodename
		FROM business_vault.tripfacts tf 
			JOIN business_vault.ratecodedim rcd ON tf.ratecodeid = rcd.ratecodeid
			JOIN business_vault.timedim td ON td.timeid = tf.pickuptimeid
		GROUP BY 
			rcd.ratecodename, 
			EXTRACT(MONTH FROM td.timestring), 
			EXTRACT(YEAR FROM td.timestring) 
		ORDER BY 
			count(*) DESC 
		LIMIT 1) 
		AS "Most Common Rate",
		
		(SELECT 
			ptd.payment_type_name 
		FROM business_vault.tripfacts tf 
			JOIN business_vault.payment_typedim ptd ON tf.ratecodeid = ptd.payment_typeid 
			JOIN business_vault.timedim td ON td.timeid = tf.pickuptimeid
		GROUP BY 
			ptd.payment_type_name, 
			EXTRACT(MONTH FROM td.timestring), 
			EXTRACT(YEAR FROM td.timestring) 
		ORDER BY 
			count(*) DESC 
		LIMIT 1) 
		AS "Most Common Payment Method"
		
	FROM business_vault.tripfacts JOIN business_vault.zonedim puzone ON puzoneid = puzone.zoneid
		JOIN business_vault.zonedim dozone ON dozoneid = dozone.zoneid
		JOIN business_vault.timedim td ON td.timeid = pickuptimeid
	WHERE 
		EXTRACT(MONTH FROM td.timestring) = (
			SELECT 
				EXTRACT(MONTH FROM td2.timestring) AS "Month"
			FROM business_vault.tripfacts tf2 
				JOIN business_vault.timedim td2 ON td2.timeid = tf2.pickuptimeid
			GROUP BY 
				EXTRACT(MONTH FROM td2.timestring) 
			ORDER BY 
				count(*) DESC 
			LIMIT 1)
	GROUP BY 
		puzone.zonename,
		dozone.zonename,
		"Month Name",
		"Year"
	ORDER BY 
		count(*) DESC;