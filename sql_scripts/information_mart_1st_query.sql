-- CONNECTION: name=postgres
CREATE TABLE information_mart.trips_statistics_month_most_trips_per_year AS 
	SELECT 
		puzone.zonename AS "Pickup Zone Name", 
		dozone.zonename AS "Dropoff Zone Name", 
		(SELECT TO_CHAR(
	    	TO_DATE (CAST(EXTRACT(MONTH FROM td.timestring) AS TEXT), 'MM'), 'Month'
	    	)
		) 
		AS "Month Name",
		(SELECT 
	    	EXTRACT(YEAR FROM td.timestring)
		)
		AS "Year",
		avg(total_amount) AS "Average Total Amount",
		
		(SELECT 
			PERCENTILE_CONT(0.75) 
		WITHIN GROUP 
			(ORDER BY total_amount)
		FROM business_vault.tripfacts) 
		AS "3rd Quartile Total Amount",
		(SELECT 
			PERCENTILE_CONT(0.5) 
		WITHIN GROUP 
			(ORDER BY total_amount)
		FROM business_vault.tripfacts) 
		AS "Median Total Amount",
		
		(SELECT 
			PERCENTILE_CONT(0.25) 
		WITHIN GROUP 
			(ORDER BY total_amount)
		FROM business_vault.tripfacts)
		AS "1st Quartile Total Amount",
		
		min(total_amount) AS "Minimum Total Amount", 
		max(total_amount) AS "Maximum Total Amount", 
		sum(total_amount) AS "Sum Total Amount",
		count(*) AS "Count", 
		avg(passengercount) AS "Average Passsenger Count", 
		sum(passengercount) AS "Total Number of Passenger",
		sum(trip_distance) AS "Total Trip Distance",
		avg(trip_distance) AS "Average Trip Distance",
		
		(SELECT 
			PERCENTILE_CONT(0.75) 
		WITHIN GROUP 
			(ORDER BY total_amount)
		FROM business_vault.tripfacts) 
		AS "3rd Quartile Trip Distance",
		
		(SELECT 
			PERCENTILE_CONT(0.5) 
		WITHIN GROUP 
			(ORDER BY total_amount)
		FROM business_vault.tripfacts) 
		AS "Median Trip Distance",
		
		(SELECT 
			PERCENTILE_CONT(0.25) 
		WITHIN GROUP 
			(ORDER BY total_amount)
		FROM business_vault.tripfacts) 
		AS "1st Quartile Trip Distance",
		
		min(trip_distance) AS "Minimum Trip Distance", 
		max(trip_distance) AS "Maximum Trip Distance",
		avg(trip_time) AS "Average Trip Time",
		
		(SELECT 
			PERCENTILE_CONT(0.75) 
		WITHIN GROUP 
			(ORDER BY trip_time)
		FROM business_vault.tripfacts) 
		AS "3rd Quartile Trip Time",
		(SELECT 
			PERCENTILE_CONT(0.5) 
		WITHIN GROUP 
			(ORDER BY trip_time)
		FROM business_vault.tripfacts) 
		AS "Median Trip Time",
		
		(SELECT 
			PERCENTILE_CONT(0.25) 
		WITHIN GROUP 
			(ORDER BY trip_time)
		FROM business_vault.tripfacts)
		AS "1st Quartile Trip Time",
		
		min(trip_time) AS "Minimum Trip Time", 
		max(trip_time) AS "Maximum Trip Time", 
		sum(trip_time) AS "Sum Trip Time",
		
		(SELECT 
			rcd.ratecodename
		FROM business_vault.tripfacts tf 
			JOIN business_vault.ratecodedim rcd ON tf.ratecodeid = rcd.ratecodeid
			JOIN business_vault.timedim td ON td.timeid = tf.pickuptimeid
		GROUP BY 
			rcd.ratecodeid, 
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
			ptd.payment_typeid, 
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