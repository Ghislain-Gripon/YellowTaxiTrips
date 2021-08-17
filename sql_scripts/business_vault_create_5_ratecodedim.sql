CREATE TABLE IF NOT EXISTS business_vault.ratecodedim(
	ratecodeid SMALLINT PRIMARY KEY, 
	ratecodename CHAR(150))
	DISTSTYLE AUTO
	DISTKEY(ratecodeid)
	SORTKEY(ratecodeid);
