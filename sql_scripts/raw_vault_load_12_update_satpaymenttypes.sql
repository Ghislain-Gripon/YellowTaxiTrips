UPDATE raw_vault.satpaymenttypes_csv spt 
SET loadenddate = CAST('{now}' AS TIMESTAMP) - INTERVAL '1s'   
WHERE 
	spt.loadenddate > CAST('{now}' AS TIMESTAMP) AND 
	spt.loaddate < CAST('{now}' AS TIMESTAMP) AND 
	spt.paymenttypehashkey IN (SELECT paymenttypehashkey 
							FROM raw_vault.satpaymenttypes_csv 
							WHERE
								paymenttypehashkey = spt.paymenttypehashkey AND 
								loaddate = CAST('{now}' AS TIMESTAMP));
								
TRUNCATE staging_area.rawpaymenttypesfile_csv;
