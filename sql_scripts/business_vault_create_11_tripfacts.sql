CREATE TABLE IF NOT EXISTS business_vault.tripfacts(
		triphashkey CHAR(64),
		pickuptimeid INTEGER, 
		dropofftimeid INTEGER, 
		puzoneid INTEGER, 
		dozoneid INTEGER, 
		vendorid INTEGER, 
		payment_typeid INTEGER, 
		ratecodeid INTEGER,
		passengercount INTEGER,
		total_amount DOUBLE PRECISION,
		tip_amount DOUBLE PRECISION,
		trip_distance DOUBLE PRECISION,
		is_payment INTEGER,
		trip_time INTERVAL,
		PRIMARY KEY (
			triphashkey,
			pickuptimeid, 
			dropofftimeid, 
			puzoneid, 
			dozoneid, 
			vendorid,
			ratecodeid,
			payment_typeid
			)
);