DO $$
DECLARE

	time_now timestamp := CAST('{now}' AS TIMESTAMP);

BEGIN

    IF (SELECT count(*) FROM raw_vault.sattrips_csv st WHERE st.loadenddate > time_now) >= {bv_drop_trigger} THEN

        DROP SCHEMA IF EXISTS business_vault CASCADE;
        
    END IF;

    CREATE SCHEMA IF NOT EXISTS business_vault;
    CREATE EXTENSION IF NOT EXISTS pgcrypto;

END $$;