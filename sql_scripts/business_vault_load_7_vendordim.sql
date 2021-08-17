INSERT INTO business_vault.vendordim(
	vendorid, 
	vendorname)

SELECT DISTINCT 
	vendorid, 
	TRIM(BOTH FROM LOWER(vendorname)) AS "vendorname"

FROM raw_vault.satvendors_csv

WHERE loadenddate > CAST('{now}' AS TIMESTAMP)

ORDER BY 
	vendorid ASC;