-- CONNECTION: name=postgres
CREATE TABLE IF NOT EXISTS business_vault.boroughdim(
	boroughid SERIAL PRIMARY KEY, 
	boroughname VARCHAR);

CREATE INDEX IF NOT EXISTS boroughdim_index ON business_vault.boroughdim(boroughid);
