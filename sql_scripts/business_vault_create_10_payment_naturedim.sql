CREATE TABLE IF NOT EXISTS business_vault.payment_naturedim(
	payment_natureid SMALLINT PRIMARY KEY,
	payment_nature_name VARCHAR)
DISTSTYLE AUTO
DISTKEY (payment_natureid)
SORTKEY(payment_natureid)
;

INSERT INTO business_vault.payment_naturedim(payment_natureid, payment_nature_name)
VALUES
	(0, 'Refund'),
	(1, 'Payment')
ON CONFLICT (payment_natureid)
DO NOTHING;