INSERT INTO raw_vault.satratecodeids_csv(
	ratecodenamehashkey,
	ratecodeid,
	ratecodename,
	loaddate,
	loadenddate,
	recordsource)
	
SELECT DISTINCT
	CAST(SHA2(TRIM(BOTH FROM LOWER(rrcf.ratecodename)), {hash_size}) AS CHAR(64)) AS ratecodenamehashkey,
	CAST(rrcf.ratecodeid AS INTEGER),
	TRIM(BOTH FROM LOWER(rrcf.ratecodename)),
	CAST('{now}' AS TIMESTAMP),
	CAST('9999-12-30 00:00:00.000' AS TIMESTAMP),
	TRIM(BOTH FROM LOWER('{origin}'))
	
FROM (SELECT DISTINCT * FROM staging_area.rawratecodeidsfile_csv) AS rrcf

WHERE 
	rrcf.ratecodeid IS NOT NULL AND 
	rrcf.ratecodename IS NOT NULL AND 
	NOT EXISTS (SELECT * 
					FROM raw_vault.satratecodeids_csv src 
					WHERE 
						src.ratecodenamehashkey = CAST(SHA2(TRIM(BOTH FROM LOWER(rrcf.ratecodename)), {hash_size})
						AS CHAR(64))
						AND src.loaddate = CAST('{now}' AS TIMESTAMP));
