CREATE TABLE IF NOT EXISTS business_vault.vendordim(
	vendorid SMALLINT PRIMARY KEY, 
	vendorname CHAR(250))
	DISTSTYLE AUTO
	DISTKEY(vendorid)
	SORTKEY(vendorid);
