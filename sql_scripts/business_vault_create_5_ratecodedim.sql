-- CONNECTION: name=postgres
CREATE TABLE IF NOT EXISTS business_vault.ratecodedim(
	ratecodeid INTEGER PRIMARY KEY, 
	ratecodename VARCHAR);
	
CREATE INDEX IF NOT EXISTS ratecode_dim_index ON business_vault.ratecodedim(ratecodeid);
