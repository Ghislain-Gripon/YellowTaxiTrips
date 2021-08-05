INSERT INTO business_vault.boroughdim(
	boroughname)
SELECT
	DISTINCT TRIM(BOTH FROM LOWER(borough)) AS "borough"
FROM raw_vault.satzones_csv
WHERE loadenddate > CAST('{now}' AS TIMESTAMP)
ORDER BY 
	borough ASC;