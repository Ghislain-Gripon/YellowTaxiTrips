CREATE TABLE IF NOT EXISTS business_vault.payment_typedim(
	payment_typeid SMALLINT PRIMARY KEY, 
	payment_type_name CHAR(150))
	DISTSTYLE AUTO
	DISTKEY(payment_typeid)
	SORTKEY(payment_typeid);