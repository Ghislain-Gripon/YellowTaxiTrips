-- CONNECTION: name=postgres
CREATE TABLE IF NOT EXISTS business_vault.vendordim(
		vendorid INTEGER PRIMARY KEY, 
		vendorname VARCHAR);
	
CREATE INDEX IF NOT EXISTS vendorid_dim_index ON business_vault.vendordim(vendorid);