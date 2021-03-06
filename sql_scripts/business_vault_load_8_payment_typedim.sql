INSERT INTO business_vault.payment_typedim(
	payment_typeid, 
	payment_type_name)

SELECT DISTINCT 
	paymenttypeid,
	TRIM(BOTH FROM LOWER(paymenttypename)) AS "paymenttypename"

FROM raw_vault.satpaymenttypes_csv

WHERE loadenddate > CAST('{now}' AS TIMESTAMP)

ORDER BY 
	paymenttypeid ASC;