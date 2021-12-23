CREATE TABLE IF NOT EXISTS business_vault.service_zone_borough_correspondance(
	correspondanceid INT IDENTITY(1,1) PRIMARY KEY, 
	service_zoneid SMALLINT, 
	boroughid SMALLINT)
	DISTSTYLE AUTO
	DISTKEY(service_zoneid)
	SORTKEY(service_zoneid);
