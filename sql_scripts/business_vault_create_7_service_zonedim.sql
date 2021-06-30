-- CONNECTION: name=postgres
CREATE TABLE IF NOT EXISTS business_vault.service_zonedim(
	service_zoneid SERIAL PRIMARY KEY, 
	service_zonename VARCHAR);

CREATE INDEX IF NOT EXISTS service_zonedim_index ON business_vault.service_zonedim(service_zoneid);