DO $$
DECLARE

	time_now timestamp := CAST('{now}' AS TIMESTAMP);

BEGIN

	CREATE EXTENSION IF NOT EXISTS pgcrypto;

	DROP INDEX IF EXISTS puzoneid_tripfacts_index;
	DROP INDEX IF EXISTS dozoneid_tripfacts_index;
	DROP INDEX IF EXISTS pickuptimeid_tripfacts_index;
	DROP INDEX IF EXISTS dropofftimeid_tripfacts_index;
	DROP INDEX IF EXISTS vendorid_tripfacts_index;
	DROP INDEX IF EXISTS is_payment_tripfacts_index;
	DROP INDEX IF EXISTS payment_typeid_tripfacts_index;
	DROP INDEX IF EXISTS ratecodeid_tripfacts_index;
		
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
		trip_distance,
		trip_time)
			
	SELECT 
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
		st.trip_distance,
		st.tpep_dropoff_datetime - st.tpep_pickup_datetime
			
	FROM raw_vault.sattrips_csv st 
				JOIN business_vault.timedim putime ON 
					(st.tpep_pickup_datetime >= putime.timestring AND 
					st.tpep_pickup_datetime <= putime.timestring + INTERVAL '59 minutes 59 seconds')
				JOIN business_vault.timedim dotime ON 
					(st.tpep_dropoff_datetime >= dotime.timestring AND 
					st.tpep_dropoff_datetime <= dotime.timestring + INTERVAL '59 minutes 59 seconds')
		
	WHERE st.loadenddate > time_now;

	COMMIT;

	CREATE INDEX IF NOT EXISTS puzoneid_tripfacts_index ON business_vault.tripfacts(puzoneid);
	CREATE INDEX IF NOT EXISTS dozoneid_tripfacts_index ON business_vault.tripfacts(dozoneid);
	CREATE INDEX IF NOT EXISTS pickuptimeid_tripfacts_index ON business_vault.tripfacts(pickuptimeid);
	CREATE INDEX IF NOT EXISTS dropofftimeid_tripfacts_index ON business_vault.tripfacts(dropofftimeid);
	CREATE INDEX IF NOT EXISTS vendorid_tripfacts_index ON business_vault.tripfacts(vendorid);
	CREATE INDEX IF NOT EXISTS is_payment_tripfacts_index ON business_vault.tripfacts(is_payment);
	CREATE INDEX IF NOT EXISTS payment_typeid_tripfacts_index ON business_vault.tripfacts(payment_typeid);
	CREATE INDEX IF NOT EXISTS ratecodeid_tripfacts_index ON business_vault.tripfacts(ratecodeid);

END $$;