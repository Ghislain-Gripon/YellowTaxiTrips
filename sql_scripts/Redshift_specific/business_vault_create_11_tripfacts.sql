CREATE TABLE IF NOT EXISTS business_vault.tripfacts(
	triphashkey CHAR(64),
	pickuptimeid INTEGER REFERENCES business_vault.timedim (timeid), 
	dropofftimeid INTEGER REFERENCES business_vault.timedim (timeid), 
	puzoneid INTEGER REFERENCES business_vault.zonedim (zoneid), 
	dozoneid INTEGER REFERENCES business_vault.zonedim (zoneid), 
	vendorid INTEGER REFERENCES business_vault.vendordim (vendorid), 
	payment_typeid INTEGER REFERENCES business_vault.payment_typedim (payment_typeid), 
	ratecodeid INTEGER REFERENCES business_vault.ratecodedim (ratecodeid),
	passengercount INTEGER,
	total_amount DOUBLE PRECISION,
	tip_amount DOUBLE PRECISION,
	trip_distance DOUBLE PRECISION,
	is_payment INTEGER REFERENCES business_vault.payment_naturedim (payment_natureid),
	PRIMARY KEY (
		triphashkey,
		pickuptimeid, 
		dropofftimeid, 
		puzoneid, 
		dozoneid, 
		vendorid,
		ratecodeid,
		payment_typeid
		))
	DISTSTYLE AUTO
	DISTKEY(triphashkey)
	SORTKEY(triphashkey);
