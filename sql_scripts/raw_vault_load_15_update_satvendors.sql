-- CONNECTION: name=postgres
DO $$
DECLARE

	loaddaterecord TIMESTAMP := CAST('{now}' AS TIMESTAMP);
	recordsourceorigin VARCHAR := TRIM(BOTH FROM LOWER('{origin}'));
	loadenddaterecord TIMESTAMP := CAST('9999-12-30 00:00:00.000' AS TIMESTAMP);

BEGIN					

								
	UPDATE raw_vault.satvendors_csv sv 
	SET loadenddate = loaddaterecord - INTERVAL '1s'
	WHERE 
		sv.loadenddate > loaddaterecord AND 
		sv.loaddate < loaddaterecord AND 
		sv.vendorhashkey IN (SELECT vendorhashkey 
								FROM raw_vault.satvendors_csv 
								WHERE
									vendorhashkey = sv.vendorhashkey AND 
									loaddate = loaddaterecord);
									
	TRUNCATE staging_area.rawvendorsfile_csv;
								
END $$;