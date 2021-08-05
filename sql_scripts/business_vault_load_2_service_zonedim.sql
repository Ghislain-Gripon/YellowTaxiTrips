INSERT INTO business_vault.service_zonedim(
	service_zonename) 
SELECT DISTINCT 
	TRIM(BOTH FROM LOWER(service_zone)) AS "service_zone"
FROM raw_vault.satzones_csv
WHERE loadenddate > CAST('{now}' AS TIMESTAMP)
ORDER BY
	service_zone ASC;
