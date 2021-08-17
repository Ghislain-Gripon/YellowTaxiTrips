UPDATE raw_vault.satratecodeids_csv src 
SET loadenddate = CAST('{now}' AS TIMESTAMP) - INTERVAL '1s'
WHERE 
	src.loadenddate > CAST('{now}' AS TIMESTAMP) AND 
	src.loaddate < CAST('{now}' AS TIMESTAMP) AND 
	src.ratecodenamehashkey IN (SELECT ratecodenamehashkey 
							FROM raw_vault.satratecodeids_csv 
							WHERE
								ratecodenamehashkey = src.ratecodenamehashkey AND 
								loaddate = CAST('{now}' AS TIMESTAMP));
							
TRUNCATE staging_area.rawratecodeidsfile_csv;
