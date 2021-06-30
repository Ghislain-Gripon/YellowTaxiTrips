-- CONNECTION: name=postgres
DO $$
DECLARE

	time_now timestamp := CAST('{now}' AS TIMESTAMP);

BEGIN

	INSERT INTO business_vault.ratecodedim(
		ratecodeid, 
		ratecodename)
	SELECT DISTINCT 
		ratecodeid, 
		TRIM(BOTH FROM LOWER(ratecodename)) AS "ratecodename"
	FROM raw_vault.satratecodeids_csv
	WHERE loadenddate > time_now
	ORDER BY 
		ratecodeid ASC;
		
END $$;