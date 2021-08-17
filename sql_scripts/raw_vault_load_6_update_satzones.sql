UPDATE raw_vault.satzones_csv sl
	SET loadenddate = CAST('{now}' AS TIMESTAMP) - INTERVAL '1s'
	WHERE 
		sl.loadenddate > CAST('{now}' AS TIMESTAMP) AND 
		sl.loaddate < CAST('{now}' AS TIMESTAMP) AND 
		sl.zonenamehashkey IN (SELECT zonenamehashkey 
									FROM raw_vault.satzones_csv 
									WHERE 
										zonenamehashkey = sl.zonenamehashkey AND 
										loaddate = CAST('{now}' AS TIMESTAMP));
								
TRUNCATE staging_area.rawzonesfile_csv;
