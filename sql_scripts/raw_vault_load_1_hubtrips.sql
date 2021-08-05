-- CONNECTION: name=postgres
DO $$
DECLARE

	loaddaterecord TIMESTAMP := CAST('{now}' AS TIMESTAMP);
	recordsourceorigin VARCHAR := TRIM(BOTH FROM LOWER('{origin}'));
	loadenddaterecord TIMESTAMP := CAST('9999-12-30 00:00:00.000' AS TIMESTAMP);

BEGIN
	
	CREATE EXTENSION IF NOT EXISTS pgcrypto;

	IF (SELECT count(*) FROM staging_area.rawtripsfile_csv) > 2e6 THEN
	
		DROP INDEX IF EXISTS tpep_pickup_hubtrips_index;
		DROP INDEX IF EXISTS tpep_dropoff_hubtrips_index;
		
	END IF;
	
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
			
	SELECT DISTINCT ON (triphashkey)
		CAST(
			DIGEST(
				CONCAT(CAST(TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_pickup_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS VARCHAR),
					CAST(TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_dropoff_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS VARCHAR), 
					TRIM(BOTH FROM rtf.puzoneid), TRIM(BOTH FROM rtf.dozoneid),
					TRIM(BOTH FROM rtf.vendorid), 
					CASE WHEN CAST(rtf.total_amount AS DOUBLE PRECISION) >= 0 THEN '1' ELSE '0' END), 
				'{hashfunc}') 
			AS CHAR(64)) AS triphashkey,
		loaddaterecord, 
		recordsourceorigin, 
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
		NOT EXISTS (SELECT * FROM raw_vault.hubtrips r WHERE r.triphashkey = CAST(DIGEST(CONCAT(
			CAST(TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_pickup_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS VARCHAR),
			CAST(TO_TIMESTAMP(TRIM(BOTH FROM LOWER(rtf.tpep_dropoff_datetime)), 'MM/DD/YYYY HH:MI:SS AM') AS VARCHAR), 
			TRIM(BOTH FROM rtf.puzoneid), TRIM(BOTH FROM rtf.dozoneid),
			TRIM(BOTH FROM rtf.vendorid), 
			CASE WHEN CAST(rtf.total_amount AS DOUBLE PRECISION) >= 0 THEN '1' ELSE '0' END), '{hashfunc}') AS CHAR(64)));
		
	CREATE INDEX IF NOT EXISTS tpep_pickup_hubtrips_index ON raw_vault.hubtrips(tpep_pickup_datetime);
	CREATE INDEX IF NOT EXISTS tpep_dropoff_hubtrips_index ON raw_vault.hubtrips(tpep_dropoff_datetime);
	
END $$;