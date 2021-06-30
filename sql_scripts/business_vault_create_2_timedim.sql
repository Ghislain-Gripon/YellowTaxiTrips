DO $$
DECLARE

	time_begin TIMESTAMP := CAST('{time_start}' AS TIMESTAMP);
	time_end TIMESTAMP := CAST('{time_end}' AS TIMESTAMP);

BEGIN

	IF NOT EXISTS (SELECT FROM pg_catalog.pg_tables WHERE schemaname = 'business_vault' AND tablename = 'timedim') THEN 

	CREATE TABLE IF NOT EXISTS business_vault.timedim(
		timeid SERIAL PRIMARY KEY,
		"hour" INTEGER,
		"day" INTEGER,
		timestring TIMESTAMP);

	COMMIT;

	INSERT INTO business_vault.timedim(
		"hour", 
		"day", 
		timestring)
		
	SELECT
		EXTRACT(HOUR FROM time_series),
		EXTRACT(DOY FROM time_series),
		time_series
		
	FROM generate_series(
		time_begin,
		time_end,
		CAST('1 hour' AS INTERVAL)
		) AS time_series;
		
	DROP INDEX IF EXISTS timestring_dim_index;
	DROP INDEX IF EXISTS timeid_dim_index;
	DROP INDEX IF EXISTS hour_dim_index;
	DROP INDEX IF EXISTS day_dim_index;
	
	CREATE INDEX IF NOT EXISTS timestring_dim_index ON business_vault.timedim(timestring);
	CREATE INDEX IF NOT EXISTS timeid_dim_index ON business_vault.timedim(timeid);
	CREATE INDEX IF NOT EXISTS hour_dim_index ON business_vault.timedim("hour");
	CREATE INDEX IF NOT EXISTS day_dim_index ON business_vault.timedim("day");

	END IF;
	
END $$;