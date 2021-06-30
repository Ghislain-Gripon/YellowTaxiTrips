-- CONNECTION: name=postgres
DO $$
DECLARE

	time_now timestamp := CAST('{now}' AS TIMESTAMP);

BEGIN

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
	WHERE loadenddate > time_now
	ORDER BY 
		zoneid ASC;
		
END $$;