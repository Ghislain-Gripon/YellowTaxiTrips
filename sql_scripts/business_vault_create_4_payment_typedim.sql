-- CONNECTION: name=postgres
CREATE TABLE IF NOT EXISTS business_vault.payment_typedim(
	payment_typeid INTEGER PRIMARY KEY, 
	payment_type_name VARCHAR);
		
CREATE INDEX IF NOT EXISTS payment_type_dim_index ON business_vault.payment_typedim(payment_typeid);
