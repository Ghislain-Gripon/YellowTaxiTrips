-- CONNECTION: name=postgres
DO $$
DECLARE

	time_now timestamp := CAST('{now}' AS TIMESTAMP);

BEGIN

	INSERT INTO business_vault.boroughdim(
		boroughname)
	SELECT
		DISTINCT TRIM(BOTH FROM LOWER(borough)) AS "borough"
	FROM raw_vault.satzones_csv
	WHERE loadenddate > time_now
	ORDER BY 
		borough ASC;
		
END $$;