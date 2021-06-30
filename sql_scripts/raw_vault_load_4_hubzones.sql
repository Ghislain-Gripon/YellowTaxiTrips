-- CONNECTION: name=postgres
DO $$
DECLARE

	loaddaterecord TIMESTAMP := CAST('{now}' AS TIMESTAMP);
	recordsourceorigin VARCHAR := TRIM(BOTH FROM LOWER('{origin}'));
	loadenddaterecord TIMESTAMP := CAST('9999-12-30 00:00:00.000' AS TIMESTAMP);

BEGIN
	
	INSERT INTO raw_vault.hubzones (
		zonenamehashkey, 
		"zone", 
		borough, 
		service_zone,
		loaddate, 
		recordsource
		)

	SELECT DISTINCT ON (zonenamehashkey)
		CAST(DIGEST(CONCAT(TRIM(BOTH FROM LOWER(rtf."zone")), TRIM(BOTH FROM LOWER(rtf.borough)), 
			TRIM(BOTH FROM LOWER(rtf.service_zone))),'{hashfunc}') 
			AS CHAR(64)) AS zonenamehashkey, 
		TRIM(BOTH FROM TRIM(both '"' FROM LOWER(rtf."zone"))) AS "zone",
		TRIM(BOTH FROM TRIM(both '"' FROM LOWER(rtf.borough))) AS borough,
		TRIM(BOTH FROM TRIM(both '"' FROM LOWER(rtf.service_zone))) AS service_zone,
		loaddaterecord,
		TRIM(BOTH FROM LOWER(recordsourceorigin)) AS recordsource 
	
	FROM (SELECT DISTINCT * FROM staging_area.rawzonesfile_csv) AS rtf

	WHERE 
		rtf."zone" IS NOT NULL AND
		rtf.borough IS NOT NULL AND
		rtf.service_zone IS NOT NULL AND
		NOT EXISTS (SELECT * FROM raw_vault.hubzones h WHERE h.zonenamehashkey = CAST(DIGEST(CONCAT(TRIM(BOTH FROM LOWER(rtf."zone")),
			TRIM(BOTH FROM LOWER(rtf.borough)), TRIM(BOTH FROM LOWER(rtf.service_zone))),'{hashfunc}') AS CHAR(64)));

END $$;