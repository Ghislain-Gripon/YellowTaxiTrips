-- CONNECTION: name=postgres
DO $$
DECLARE

	loaddaterecord TIMESTAMP := CAST('{now}' AS TIMESTAMP);
	recordsourceorigin VARCHAR := TRIM(BOTH FROM LOWER('{origin}'));
	loadenddaterecord TIMESTAMP := CAST('9999-12-30 00:00:00.000' AS TIMESTAMP);

BEGIN					

	UPDATE raw_vault.satratecodeids_csv src 
	SET loadenddate = loaddaterecord - INTERVAL '1s'
	WHERE 
		src.loadenddate > loaddaterecord AND 
		src.loaddate < loaddaterecord AND 
		src.ratecodenamehashkey IN (SELECT ratecodenamehashkey 
								FROM raw_vault.satratecodeids_csv 
								WHERE
									ratecodenamehashkey = src.ratecodenamehashkey AND 
									loaddate = loaddaterecord);
								

	
	TRUNCATE staging_area.rawratecodeidsfile_csv;

END $$;