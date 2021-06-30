-- CONNECTION: name=postgres
CREATE TABLE IF NOT EXISTS business_vault.zonedim(
	zoneid INTEGER PRIMARY KEY, 
	boroughid INTEGER, 
	zonename VARCHAR
	);

CREATE INDEX IF NOT EXISTS zoneid_dim_index ON business_vault.zonedim(zoneid);
CREATE INDEX IF NOT EXISTS boroughid_dim_index ON business_vault.zonedim(boroughid);
