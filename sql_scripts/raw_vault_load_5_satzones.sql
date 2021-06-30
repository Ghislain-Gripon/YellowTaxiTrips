-- CONNECTION: name=postgres
DO $$
DECLARE

	loaddaterecord TIMESTAMP := CAST('{now}' AS TIMESTAMP);
	recordsourceorigin VARCHAR := TRIM(BOTH FROM LOWER('{origin}'));
	loadenddaterecord TIMESTAMP := CAST('9999-12-30 00:00:00.000' AS TIMESTAMP);

BEGIN
	
	INSERT INTO raw_vault.satzones_csv(
		zonenamehashkey, 
		loaddate, 
		borough, 
		"zone", 
		service_zone, 
		loadenddate, 
		recordsource, 
		zoneid)
		
	SELECT DISTINCT ON (zonenamehashkey)
		CAST(DIGEST(CONCAT(TRIM(BOTH FROM LOWER(rtf."zone")), TRIM(BOTH FROM LOWER(rtf.borough)), 
			TRIM(BOTH FROM LOWER(rtf.service_zone))),'{hashfunc}') as CHAR(64)) AS zonenamehashkey, 
		loaddaterecord, 
		TRIM(BOTH FROM TRIM(both '"' FROM LOWER(rtf.borough))) AS borough, 
		TRIM(BOTH FROM TRIM(both '"' FROM LOWER(rtf."zone"))) AS "zone", 
		TRIM(BOTH FROM TRIM(both '"' FROM LOWER(rtf.service_zone))) AS service_zone, 
		loadenddaterecord, 
		TRIM(BOTH FROM LOWER(recordsourceorigin)) AS recordsource, 
		CAST(TRIM(BOTH FROM LOWER(rtf.zoneid)) AS INTEGER) AS zoneid
	
	FROM (SELECT DISTINCT * FROM staging_area.rawzonesfile_csv) AS rtf
	
	WHERE 
		rtf.zoneid IS NOT NULL AND 
		rtf.borough IS NOT NULL AND 
		rtf."zone" IS NOT NULL AND 
		rtf.service_zone IS NOT NULL AND 
		NOT EXISTS (SELECT * FROM raw_vault.satzones_csv s WHERE s.zonenamehashkey = CAST(DIGEST(CONCAT(TRIM(BOTH FROM LOWER(rtf."zone")), 
			TRIM(BOTH FROM LOWER(rtf.borough)), TRIM(BOTH FROM LOWER(rtf.service_zone))),'{hashfunc}')
			AS CHAR(64))
			AND s.loaddate = loaddaterecord);
	
END $$;