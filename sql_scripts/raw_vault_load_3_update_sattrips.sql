UPDATE raw_vault.sattrips_csv st 
SET loadenddate = CAST('{now}' AS TIMESTAMP) - INTERVAL '1s' 
WHERE 
	st.loadenddate > CAST('{now}' AS TIMESTAMP) AND 
	st.loaddate < CAST('{now}' AS TIMESTAMP) AND 
	st.triphashkey IN (SELECT triphashkey 
							FROM raw_vault.sattrips_csv 
							WHERE
								triphashkey = st.triphashkey AND 
								loaddate = CAST('{now}' AS TIMESTAMP));

TRUNCATE staging_area.rawtripsfile_csv;

