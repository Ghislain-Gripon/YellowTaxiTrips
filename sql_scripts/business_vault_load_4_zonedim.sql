INSERT INTO business_vault.zonedim(
	zoneid, 
	boroughid, 
	zonename)
SELECT DISTINCT 
	zoneid, 
	boroughid,
	TRIM(BOTH FROM LOWER("zone")) AS "zone" 
FROM raw_vault.satzones_csv
	INNER JOIN business_vault.boroughdim ON borough = boroughname
WHERE loadenddate > CAST('{now}' AS TIMESTAMP)
ORDER BY 
	zoneid ASC;