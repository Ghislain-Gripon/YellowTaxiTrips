-- CONNECTION: name=postgres
DO $$
DECLARE

	loaddaterecord TIMESTAMP := CAST('{now}' AS TIMESTAMP);
	recordsourceorigin VARCHAR := TRIM(BOTH FROM LOWER('{origin}'));
	loadenddaterecord TIMESTAMP := CAST('9999-12-30 00:00:00.000' AS TIMESTAMP);

BEGIN

	UPDATE raw_vault.satzones_csv sl
		SET loadenddate = loaddaterecord - INTERVAL '1s'
		WHERE 
			sl.loadenddate > loaddaterecord AND 
			sl.loaddate < loaddaterecord AND 
			sl.zonenamehashkey IN (SELECT zonenamehashkey 
										FROM raw_vault.satzones_csv 
										WHERE 
											zonenamehashkey = sl.zonenamehashkey AND 
											loaddate = loaddaterecord);
									
	TRUNCATE staging_area.rawzonesfile_csv;
									
END $$;