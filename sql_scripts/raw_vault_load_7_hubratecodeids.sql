INSERT INTO raw_vault.hubratecodeids(
	ratecodenamehashkey,
	ratecodename,
	loaddate,
	recordsource)
	
SELECT DISTINCT
	CAST(SHA2(TRIM(BOTH FROM LOWER(rrcf.ratecodename)), {hash_size}) AS CHAR(64)) AS ratecodenamehashkey,
	TRIM(BOTH FROM LOWER(rrcf.ratecodename)),
	CAST('{now}' AS TIMESTAMP),
	TRIM(BOTH FROM LOWER('{origin}'))
	
FROM (SELECT DISTINCT * FROM staging_area.rawratecodeidsfile_csv) AS rrcf

WHERE 
	rrcf.ratecodeid IS NOT NULL AND 
	rrcf.ratecodename IS NOT NULL AND 
	NOT EXISTS (SELECT * 
				FROM raw_vault.hubratecodeids hrc 
				WHERE 
					hrc.ratecodenamehashkey = CAST(SHA2(TRIM(BOTH FROM LOWER(rrcf.ratecodename)), {hash_size}) AS CHAR(64)));
