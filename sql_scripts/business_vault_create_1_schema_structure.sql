IF (SELECT count(*) FROM raw_vault.sattrips_csv st WHERE st.loadenddate > CAST('{now}' AS TIMESTAMP)) >= {bv_drop_trigger} THEN

    DROP SCHEMA IF EXISTS business_vault CASCADE;
    
END IF;

CREATE SCHEMA IF NOT EXISTS business_vault;
