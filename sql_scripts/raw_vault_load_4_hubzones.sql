INSERT INTO raw_vault.hubzones (
	zonenamehashkey, 
	"zone", 
	borough, 
	service_zone,
	loaddate, 
	recordsource
	)

SELECT DISTINCT
	CAST({hash_func}(CONCAT(TRIM(BOTH FROM LOWER(rtf."zone")), TRIM(BOTH FROM LOWER(rtf.borough)), 
		TRIM(BOTH FROM LOWER(rtf.service_zone))), {hash_param}) 
		AS CHAR(64)) AS zonenamehashkey, 
	TRIM(BOTH FROM TRIM(both '"' FROM LOWER(rtf."zone"))) AS "zone",
	TRIM(BOTH FROM TRIM(both '"' FROM LOWER(rtf.borough))) AS borough,
	TRIM(BOTH FROM TRIM(both '"' FROM LOWER(rtf.service_zone))) AS service_zone,
	CAST('{now}' AS TIMESTAMP),
	TRIM(BOTH FROM LOWER('{origin}')) AS recordsource 

FROM (SELECT DISTINCT * FROM staging_area.rawzonesfile_csv) AS rtf

WHERE 
	rtf."zone" IS NOT NULL AND
	rtf.borough IS NOT NULL AND
	rtf.service_zone IS NOT NULL AND
	NOT EXISTS (SELECT * FROM raw_vault.hubzones h WHERE h.zonenamehashkey = CAST({hash_func}(CONCAT(TRIM(BOTH FROM LOWER(rtf."zone")),
		TRIM(BOTH FROM LOWER(rtf.borough)), TRIM(BOTH FROM LOWER(rtf.service_zone))), {hash_param}) AS CHAR(64)));
