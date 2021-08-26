CREATE TABLE IF NOT EXISTS business_vault.payment_naturedim(
	payment_natureid SMALLINT PRIMARY KEY,
	payment_nature_name CHAR(150))
	DISTSTYLE AUTO
	DISTKEY(payment_natureid)
	SORTKEY(payment_natureid);

