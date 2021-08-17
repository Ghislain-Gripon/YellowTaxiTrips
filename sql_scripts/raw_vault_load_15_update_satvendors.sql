UPDATE raw_vault.satvendors_csv sv 
SET loadenddate = CAST('{now}' AS TIMESTAMP) - INTERVAL '1s'
WHERE 
	sv.loadenddate > CAST('{now}' AS TIMESTAMP) AND 
	sv.loaddate < CAST('{now}' AS TIMESTAMP) AND 
	sv.vendorhashkey IN (SELECT vendorhashkey 
							FROM raw_vault.satvendors_csv 
							WHERE
								vendorhashkey = sv.vendorhashkey AND 
								loaddate = CAST('{now}' AS TIMESTAMP));
								
TRUNCATE staging_area.rawvendorsfile_csv;