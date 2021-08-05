-- CONNECTION: name=postgres
DO $$
DECLARE

	loaddaterecord TIMESTAMP := CAST('{now}' AS TIMESTAMP);
	recordsourceorigin VARCHAR := TRIM(BOTH FROM LOWER('{origin}'));
	loadenddaterecord TIMESTAMP := CAST('9999-12-30 00:00:00.000' AS TIMESTAMP);

BEGIN

	INSERT INTO raw_vault.satratecodeids_csv(
		ratecodenamehashkey,
		ratecodeid,
		ratecodename,
		loaddate,
		loadenddate,
		recordsource)
		
	SELECT DISTINCT ON (ratecodenamehashkey)
		CAST(DIGEST(TRIM(BOTH FROM LOWER(rrcf.ratecodename)),'{hashfunc}') AS CHAR(64)) AS ratecodenamehashkey,
		CAST(rrcf.ratecodeid AS INTEGER),
		TRIM(BOTH FROM LOWER(rrcf.ratecodename)),
		loaddaterecord,
		loadenddaterecord,
		recordsourceorigin
		
	FROM (SELECT DISTINCT * FROM staging_area.rawratecodeidsfile_csv) AS rrcf
	
	WHERE 
		rrcf.ratecodeid IS NOT NULL AND 
		rrcf.ratecodename IS NOT NULL AND 
		NOT EXISTS (SELECT * 
						FROM raw_vault.satratecodeids_csv src 
						WHERE 
							src.ratecodenamehashkey = CAST(DIGEST(TRIM(BOTH FROM LOWER(rrcf.ratecodename)),'{hashfunc}')
							AS CHAR(64))
							AND src.loaddate = loaddaterecord);
							
END $$;