CREATE TABLE IF NOT EXISTS business_vault.boroughdim(
	boroughid IDENTITY(0,1) PRIMARY KEY, 
	boroughname CHAR(250))
	DISTSTYLE AUTO
	DISTKEY(boroughid)
	SORTKEY(boroughid);