-- CONNECTION: name=postgres
DO $$
DECLARE

	loaddaterecord TIMESTAMP := CAST('{now}' AS TIMESTAMP);
	recordsourceorigin VARCHAR := TRIM(BOTH FROM LOWER('{origin}'));
	loadenddaterecord TIMESTAMP := CAST('9999-12-30 00:00:00.000' AS TIMESTAMP);

BEGIN					
									
	UPDATE raw_vault.sattrips_csv st 
	SET loadenddate = loaddaterecord - INTERVAL '1s' 
	WHERE 
		st.loadenddate > loaddaterecord AND 
		st.loaddate < loaddaterecord AND 
		st.triphashkey IN (SELECT triphashkey 
								FROM raw_vault.sattrips_csv 
								WHERE
									triphashkey = st.triphashkey AND 
									loaddate = loaddaterecord);

	TRUNCATE staging_area.rawtripsfile_csv;
								
END $$;
