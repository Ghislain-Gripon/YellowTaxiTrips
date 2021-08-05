-- CONNECTION: name=postgres
DO $$
DECLARE

	loaddaterecord TIMESTAMP := CAST('{now}' AS TIMESTAMP);
	recordsourceorigin VARCHAR := TRIM(BOTH FROM LOWER('{origin}'));
	loadenddaterecord TIMESTAMP := CAST('9999-12-30 00:00:00.000' AS TIMESTAMP);

BEGIN
	
	CREATE EXTENSION IF NOT EXISTS pgcrypto;

	IF (SELECT count(*) FROM staging_area.rawtripsfile_csv) > 2e6 THEN
	
		DROP INDEX IF EXISTS tpep_pickup_sattrips_csv_index;
		DROP INDEX IF EXISTS tpep_dropoff_sattrips_csv_index;
	
	END IF;

	INSERT INTO raw_vault.sattrips_csv (
		triphashkey, 
		loaddate, 
		recordsource, 
		passenger_count, 
		loadenddate, 
		trip_distance, 
		ratecodeid, 
		store_and_fwd_flag, 
		payment_type, 
		fare_amount, 
		extra, 
		mta_tax, 
		improvement_surcharge, 
		tip_amount, 
		tolls_amount, 
		total_amount, 
		vendorid, 
		tpep_pickup_datetime, 
		tpep_dropoff_datetime, 
		puzoneid, 
		dozoneid, 
		is_payment)

	SELECT DISTINCT ON (triphashkey)
		CAST(
			DIGEST(CONCAT(CAST(TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_pickup_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS VARCHAR),
					CAST(TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_dropoff_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS VARCHAR), 
					TRIM(BOTH FROM LOWER(rtf.puzoneid)), TRIM(BOTH FROM LOWER(rtf.dozoneid)),
					TRIM(BOTH FROM LOWER(rtf.vendorid)), 
					CAST(CASE WHEN CAST(TRIM(BOTH FROM LOWER(rtf.total_amount)) AS DOUBLE PRECISION) >= 0 THEN 1 
					ELSE 0 END AS VARCHAR)),
			'{hashfunc}') 
		AS CHAR(64)) AS triphashkey,
		loaddaterecord, TRIM(BOTH FROM LOWER(recordsourceorigin)) AS recordsourceorigin,
		CAST(TRIM(BOTH FROM LOWER(rtf.passenger_count)) AS INTEGER) AS passenger_count, 
		loadenddaterecord,
		CAST(TRIM(BOTH FROM LOWER(rtf.trip_distance)) AS DOUBLE PRECISION) AS trip_distance, 
		CAST(TRIM(BOTH FROM LOWER(rtf.ratecodeid)) AS INTEGER) AS ratecodeid, 
		TRIM(BOTH FROM LOWER(rtf.store_and_fwd_flag)) AS store_and_fwd_flag, 
		CAST(TRIM(BOTH FROM LOWER(rtf.payment_type)) AS INTEGER) AS payment_type,
		CAST(TRIM(BOTH FROM LOWER(rtf.fare_amount)) AS DOUBLE PRECISION) AS fare_amount,
		CAST(TRIM(BOTH FROM LOWER(rtf.extra)) AS DOUBLE PRECISION) AS extra,
		CAST(TRIM(BOTH FROM LOWER(rtf.mta_tax)) AS DOUBLE PRECISION) AS mta_tax, 
		CAST(TRIM(BOTH FROM LOWER(rtf.improvement_surcharge)) AS DOUBLE PRECISION) AS improvement_surcharge,
		CAST(TRIM(BOTH FROM LOWER(rtf.tip_amount)) AS DOUBLE PRECISION) AS tip_amount,
		CAST(TRIM(BOTH FROM LOWER(rtf.tolls_amount)) AS DOUBLE PRECISION) AS tolls_amount, 
		CAST(TRIM(BOTH FROM LOWER(rtf.total_amount)) AS DOUBLE PRECISION) AS total_amount,
		CAST(TRIM(BOTH FROM LOWER(rtf.vendorid)) AS INTEGER) AS vendorid, 
		TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_pickup_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS tpep_pickup_datetime,
		TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_dropoff_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS tpep_dropoff_datetime, 
		CAST(TRIM(BOTH FROM LOWER(rtf.puzoneid)) AS INTEGER) AS puzoneid, 
		CAST(TRIM(BOTH FROM LOWER(rtf.dozoneid)) AS INTEGER) AS dozoneid,
		(CASE WHEN CAST(TRIM(BOTH FROM LOWER(rtf.total_amount)) AS DOUBLE PRECISION) >= 0 THEN 1 
			ELSE 0 END) AS is_payment
	
	FROM (SELECT DISTINCT * FROM staging_area.rawtripsfile_csv) AS rtf

	WHERE 
		rtf.tpep_pickup_datetime IS NOT NULL AND 
		rtf.tpep_dropoff_datetime IS NOT NULL AND 
		rtf.puzoneid IS NOT NULL AND 
		rtf.dozoneid IS NOT NULL AND 
		rtf.vendorid IS NOT NULL AND 
		rtf.total_amount IS NOT NULL AND 
		NOT EXISTS (SELECT * FROM raw_vault.sattrips_csv r WHERE r.triphashkey = CAST(DIGEST(CONCAT(
			CAST(TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_pickup_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS VARCHAR),
			CAST(TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_dropoff_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS VARCHAR), 
			TRIM(BOTH FROM rtf.puzoneid), TRIM(BOTH FROM rtf.dozoneid),
			TRIM(BOTH FROM rtf.vendorid), 
			CASE WHEN CAST(rtf.total_amount AS DOUBLE PRECISION) >= 0 THEN '1' ELSE '0' END), '{hashfunc}') AS CHAR(64))
			AND r.loaddate = loaddaterecord);

	CREATE INDEX IF NOT EXISTS tpep_pickup_sattrips_csv_index ON raw_vault.sattrips_csv(tpep_pickup_datetime);
	CREATE INDEX IF NOT EXISTS tpep_dropoff_sattrips_csv_index ON raw_vault.sattrips_csv(tpep_dropoff_datetime);
	
END $$;