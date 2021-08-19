-- CONNECTION: name=postgres
CREATE TABLE information_mart.trips_frequency_hour_of_day AS 
	SELECT DISTINCT
		zdpu.zonename AS "Pickup Zone Name", 
		zddo.zonename AS "Dropoff Zone Name",
		sum(tf.total_amount) AS "Total Amount Sum", 
		td2."hour" AS "Hour",
		EXTRACT(YEAR FROM td2.timestring) AS "Year",
		count(*) AS "Count"
		
	FROM business_vault.tripfacts tf 
		JOIN business_vault.timedim td2 ON tf.pickuptimeid = td2.timeid	
		JOIN business_vault.zonedim zdpu ON zdpu.zoneid = tf.puzoneid
		JOIN business_vault.zonedim zddo ON zddo.zoneid = tf.dozoneid
	GROUP BY 
		"Pickup Zone Name",
		"Dropoff Zone Name",
		"Hour",
		"Year"
	ORDER BY  
		"Count" DESC,
		"Hour" ASC;










