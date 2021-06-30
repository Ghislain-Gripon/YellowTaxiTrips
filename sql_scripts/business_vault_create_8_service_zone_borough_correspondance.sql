-- CONNECTION: name=postgres
CREATE TABLE IF NOT EXISTS business_vault.service_zone_borough_correspondance(
	correspondanceid SERIAL PRIMARY KEY, 
	service_zoneid INTEGER, 
	boroughid INTEGER
	);

CREATE INDEX IF NOT EXISTS correspondanceid_dim_index ON business_vault.service_zone_borough_correspondance(correspondanceid);
CREATE INDEX IF NOT EXISTS service_zoneid_dim_index ON business_vault.service_zone_borough_correspondance(service_zoneid);
CREATE INDEX IF NOT EXISTS boroughid_dim_index ON business_vault.service_zone_borough_correspondance(boroughid);
