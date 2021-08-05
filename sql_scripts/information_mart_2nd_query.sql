-- CONNECTION: name=postgres
CREATE TABLE information_mart.trips_frequency_hour_of_day AS 
	SELECT 
		zdpu.zonename AS "Pickup Zone Name", 
		zddo.zonename AS "Dropoff Zone Name",
		q."tot_am_sum" AS "Total Amount Sum", 
		td2."hour" AS "Hour",
		td2."Year",
		q."ct" AS "Count"
		
	FROM (
		SELECT DISTINCT 
			"hour", 
			(SELECT 
	    		EXTRACT(YEAR FROM timestring) AS "Year"
		)
		FROM business_vault.timedim) AS td2 
		JOIN LATERAL (
			SELECT 
				tf.puzoneid, 
				tf.dozoneid, 
				sum(tf.total_amount) AS "tot_am_sum", 
				td1."hour", 
				count(*) AS "ct"
			FROM business_vault.tripfacts tf JOIN business_vault.timedim td1 ON tf.pickuptimeid = td1.timeid 
			WHERE  
				td1."hour" = td2."hour" AND
				EXTRACT(YEAR FROM td1.timestring) = td2."Year"

			GROUP BY 
				td1."hour", 
				tf.puzoneid, 
				tf.dozoneid 
			ORDER BY 
				count(*) DESC 
			LIMIT 10
		) AS q ON TRUE
		JOIN business_vault.zonedim zdpu ON zdpu.zoneid = q.puzoneid
		JOIN business_vault.zonedim zddo ON zddo.zoneid = q.dozoneid
	ORDER BY 
		td2."hour" ASC, 
		q."ct" DESC;










