CREATE TABLE IF NOT EXISTS business_vault.zonedim(
	zoneid SMALLINT PRIMARY KEY, 
	boroughid SMALLINT, 
	zonename VARCHAR(150));
