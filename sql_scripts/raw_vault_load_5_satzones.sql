INSERT INTO raw_vault.satzones_csv(
	zonenamehashkey, 
	loaddate, 
	borough, 
	"zone", 
	service_zone, 
	loadenddate, 
	recordsource, 
	zoneid)
	
SELECT DISTINCT
	CAST({hash_func}(CONCAT(TRIM(BOTH FROM LOWER(rtf."zone")), TRIM(BOTH FROM LOWER(rtf.borough)), 
		TRIM(BOTH FROM LOWER(rtf.service_zone))), {hash_param}) as CHAR(64)) AS zonenamehashkey, 
	CAST('{now}' AS TIMESTAMP), 
	TRIM(BOTH FROM TRIM(both '"' FROM LOWER(rtf.borough))) AS borough, 
	TRIM(BOTH FROM TRIM(both '"' FROM LOWER(rtf."zone"))) AS "zone", 
	TRIM(BOTH FROM TRIM(both '"' FROM LOWER(rtf.service_zone))) AS service_zone, 
	CAST('9999-12-30 00:00:00.000' AS TIMESTAMP), 
	TRIM(BOTH FROM LOWER('{origin}')) AS recordsource, 
	CAST(TRIM(BOTH FROM LOWER(rtf.zoneid)) AS INTEGER) AS zoneid

FROM (SELECT DISTINCT * FROM staging_area.rawzonesfile_csv) AS rtf

WHERE 
	rtf.zoneid IS NOT NULL AND 
	rtf.borough IS NOT NULL AND 
	rtf."zone" IS NOT NULL AND 
	rtf.service_zone IS NOT NULL AND 
	NOT EXISTS (SELECT * FROM raw_vault.satzones_csv s WHERE s.zonenamehashkey = CAST({hash_func}(CONCAT(TRIM(BOTH FROM LOWER(rtf."zone")), 
		TRIM(BOTH FROM LOWER(rtf.borough)), TRIM(BOTH FROM LOWER(rtf.service_zone))), {hash_param})
		AS CHAR(64))
		AND s.loaddate = CAST('{now}' AS TIMESTAMP));
