-- CONNECTION: name=postgres
DO $$
DECLARE

	loaddaterecord TIMESTAMP := CAST('{now}' AS TIMESTAMP);
	recordsourceorigin VARCHAR := TRIM(BOTH FROM LOWER('{origin}'));
	loadenddaterecord TIMESTAMP := CAST('9999-12-30 00:00:00.000' AS TIMESTAMP);

BEGIN					
								
	UPDATE raw_vault.satpaymenttypes_csv spt 
	SET loadenddate = loaddaterecord - INTERVAL '1s'   
	WHERE 
		spt.loadenddate > loaddaterecord AND 
		spt.loaddate < loaddaterecord AND 
		spt.paymenttypehashkey IN (SELECT paymenttypehashkey 
								FROM raw_vault.satpaymenttypes_csv 
								WHERE
									paymenttypehashkey = spt.paymenttypehashkey AND 
									loaddate = loaddaterecord);
									
	TRUNCATE staging_area.rawpaymenttypesfile_csv;

END $$;