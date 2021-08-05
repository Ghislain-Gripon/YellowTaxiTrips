CREATE TABLE IF NOT EXISTS business_vault.service_zonedim(
	service_zoneid IDENTITY(0,1) PRIMARY KEY, 
	service_zonename CHAR(100))
	DISTSTYLE AUTO
	DISTKEY(service_zoneid)
	SORTKEY(service_zoneid);