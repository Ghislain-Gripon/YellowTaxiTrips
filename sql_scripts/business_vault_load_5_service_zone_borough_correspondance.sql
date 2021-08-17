INSERT INTO business_vault.service_zone_borough_correspondance(
	service_zoneid, 
	boroughid)

SELECT DISTINCT 
	service_zoneid,
	boroughid

FROM raw_vault.satzones_csv 
	INNER JOIN business_vault.boroughdim ON borough = boroughname
	INNER JOIN business_vault.service_zonedim ON service_zonename = service_zone

WHERE loadenddate > CAST('{now}' AS TIMESTAMP)

ORDER BY 
	service_zoneid ASC, 
	boroughid ASC;