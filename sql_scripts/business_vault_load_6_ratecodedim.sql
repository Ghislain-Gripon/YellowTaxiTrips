INSERT INTO business_vault.ratecodedim(
	ratecodeid, 
	ratecodename)

SELECT DISTINCT 
	ratecodeid, 
	TRIM(BOTH FROM LOWER(ratecodename)) AS "ratecodename"

FROM raw_vault.satratecodeids_csv

WHERE loadenddate > CAST('{now}' AS TIMESTAMP)

ORDER BY 
	ratecodeid ASC;