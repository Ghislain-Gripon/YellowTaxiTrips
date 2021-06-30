-- CONNECTION: name=postgres
DO $$
DECLARE

	time_now timestamp := CAST('{now}' AS TIMESTAMP);

BEGIN

	INSERT INTO business_vault.service_zone_borough_correspondance(
		service_zoneid, 
		boroughid)
	SELECT DISTINCT 
		service_zoneid,
		boroughid
	FROM raw_vault.satzones_csv 
		INNER JOIN business_vault.boroughdim ON borough = boroughname
		INNER JOIN business_vault.service_zonedim ON service_zonename = service_zone
	WHERE loadenddate > time_now
	ORDER BY 
		service_zoneid ASC, 
		boroughid ASC;

END $$;