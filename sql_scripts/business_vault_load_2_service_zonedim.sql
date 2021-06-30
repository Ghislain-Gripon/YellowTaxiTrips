-- CONNECTION: name=postgres
DO $$
DECLARE

	time_now timestamp := CAST('{now}' AS TIMESTAMP);

BEGIN

	INSERT INTO business_vault.service_zonedim(
		service_zonename) 
	SELECT DISTINCT 
		TRIM(BOTH FROM LOWER(service_zone)) AS "service_zone"
	FROM raw_vault.satzones_csv
	WHERE loadenddate > time_now
	ORDER BY
		service_zone ASC;
	
END $$;