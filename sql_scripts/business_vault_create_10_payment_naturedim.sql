-- CONNECTION: name=postgres
CREATE TABLE IF NOT EXISTS business_vault.payment_naturedim(
	payment_natureid INTEGER PRIMARY KEY,
	payment_nature_name VARCHAR);

INSERT INTO business_vault.payment_naturedim(payment_natureid, payment_nature_name)
VALUES
(0, 'Refund'),
(1, 'Payment')
ON CONFLICT (payment_natureid)
DO NOTHING;

CREATE INDEX IF NOT EXISTS payment_nature_dim_index ON business_vault.payment_naturedim(payment_natureid);
