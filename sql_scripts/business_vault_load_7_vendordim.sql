-- CONNECTION: name=postgres
DO $$
DECLARE

	time_now timestamp := CAST('{now}' AS TIMESTAMP);

BEGIN
	
	INSERT INTO business_vault.vendordim(
		vendorid, 
		vendorname)
	SELECT DISTINCT 
		vendorid, 
		TRIM(BOTH FROM LOWER(vendorname)) AS "vendorname"
	FROM raw_vault.satvendors_csv
	WHERE loadenddate > time_now
	ORDER BY 
		vendorid ASC;
		
END $$;