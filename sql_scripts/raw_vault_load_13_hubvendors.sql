-- CONNECTION: name=postgres
DO $$
DECLARE

	loaddaterecord TIMESTAMP := CAST('{now}' AS TIMESTAMP);
	recordsourceorigin VARCHAR := TRIM(BOTH FROM LOWER('{origin}'));
	loadenddaterecord TIMESTAMP := CAST('9999-12-30 00:00:00.000' AS TIMESTAMP);

BEGIN

	INSERT INTO raw_vault.hubvendors(
		vendorhashkey,
		vendorname,
		loaddate,
		recordsource)
	
	SELECT DISTINCT ON (vendorhashkey)
		CAST(DIGEST(TRIM(BOTH FROM LOWER(rvf.vendorname)),'{hashfunc}') AS CHAR(64)) AS vendorhashkey,
		TRIM(BOTH FROM LOWER(rvf.vendorname)),
		loaddaterecord,
		recordsourceorigin
		
	FROM (SELECT DISTINCT * FROM staging_area.rawvendorsfile_csv) AS rvf
	
	WHERE 
		rvf.vendorname IS NOT NULL AND 
		rvf.vendorid IS NOT NULL AND 
		NOT EXISTS (SELECT * 
						FROM raw_vault.hubvendors hv 
						WHERE 
							hv.vendorhashkey = CAST(DIGEST(TRIM(BOTH FROM LOWER(rvf.vendorname)),'{hashfunc}') AS CHAR(64)));
							
END $$;