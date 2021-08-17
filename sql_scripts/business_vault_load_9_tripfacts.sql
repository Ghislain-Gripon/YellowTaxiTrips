INSERT INTO business_vault.tripfacts(
	triphashkey,
	pickuptimeid,
	dropofftimeid,
	puzoneid,
	dozoneid,
	vendorid,
	is_payment,
	payment_typeid,
	ratecodeid,
	passengercount,
	total_amount,
	tip_amount,
	trip_distance)
		
SELECT DISTINCT
	st.triphashkey,
	putime.timeid, 
	dotime.timeid, 
	st.puzoneid, 
	st.dozoneid, 
	st.vendorid,
	st.is_payment,
	st.payment_type, 
	st.ratecodeid, 
	st.passenger_count,
	st.total_amount, 
	st.tip_amount,
	st.trip_distance
		
FROM raw_vault.sattrips_csv st 
			JOIN business_vault.timedim putime ON 
				(st.tpep_pickup_datetime >= putime.timestring AND 
				st.tpep_pickup_datetime <= putime.timestring + INTERVAL '59 minutes 59 seconds')
			JOIN business_vault.timedim dotime ON 
				(st.tpep_dropoff_datetime >= dotime.timestring AND 
				st.tpep_dropoff_datetime <= dotime.timestring + INTERVAL '59 minutes 59 seconds')
	
WHERE st.loadenddate > CAST('{now}' AS TIMESTAMP);
