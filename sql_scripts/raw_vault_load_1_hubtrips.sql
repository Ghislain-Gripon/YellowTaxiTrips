INSERT INTO raw_vault.hubtrips(
	triphashkey, 
	loaddate, 
	recordsource, 
	tpep_pickup_datetime, 
	tpep_dropoff_datetime, 
	puzoneid, 
	dozoneid, 
	vendorid, 
	is_payment)
		
SELECT DISTINCT
	CAST(
		SHA2(
			CONCAT(CAST(TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_pickup_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS VARCHAR),
				CAST(TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_dropoff_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS VARCHAR), 
				TRIM(BOTH FROM rtf.puzoneid), TRIM(BOTH FROM rtf.dozoneid),
				TRIM(BOTH FROM rtf.vendorid), 
				CASE WHEN CAST(rtf.total_amount AS DOUBLE PRECISION) >= 0 THEN '1' ELSE '0' END), 
			{hash_size}) 
		AS CHAR(64)) AS triphashkey,
	CAST('{now}' AS TIMESTAMP), 
	TRIM(BOTH FROM LOWER('{origin}')), 
	TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_pickup_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS tpep_pickup_datetime, 
	TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_dropoff_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS tpep_dropoff_datetime, 
	CAST(rtf.puzoneid AS INTEGER),
	CAST(rtf.dozoneid AS INTEGER),
	CAST(rtf.vendorid AS INTEGER),
	CASE WHEN CAST(rtf.total_amount AS DOUBLE PRECISION) >= 0 THEN 1 ELSE 0 END
		
FROM (SELECT DISTINCT * FROM staging_area.rawtripsfile_csv) AS rtf
	
WHERE 
	rtf.tpep_pickup_datetime IS NOT NULL AND 
	rtf.tpep_dropoff_datetime IS NOT NULL AND 
	rtf.puzoneid IS NOT NULL AND 
	rtf.dozoneid IS NOT NULL AND 
	rtf.vendorid IS NOT NULL AND 
	rtf.total_amount IS NOT NULL AND 
	NOT EXISTS (SELECT * FROM raw_vault.hubtrips r WHERE r.triphashkey = CAST(SHA2(CONCAT(
		CAST(TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_pickup_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS VARCHAR),
		CAST(TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_dropoff_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS VARCHAR), 
		TRIM(BOTH FROM rtf.puzoneid), TRIM(BOTH FROM rtf.dozoneid),
		TRIM(BOTH FROM rtf.vendorid),
		CASE WHEN CAST(rtf.total_amount AS DOUBLE PRECISION) >= 0 THEN '1' ELSE '0' END), {hash_size}) AS CHAR(64)));