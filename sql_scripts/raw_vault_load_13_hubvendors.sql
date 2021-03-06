INSERT INTO raw_vault.hubvendors(
	vendorhashkey,
	vendorname,
	loaddate,
	recordsource)

SELECT DISTINCT
	CAST({hash_func}(TRIM(BOTH FROM LOWER(rvf.vendorname)), {hash_param}) AS CHAR(64)) AS vendorhashkey,
	TRIM(BOTH FROM LOWER(rvf.vendorname)),
	CAST('{now}' AS TIMESTAMP),
	TRIM(BOTH FROM LOWER('{origin}'))
	
FROM (SELECT DISTINCT * FROM staging_area.rawvendorsfile_csv) AS rvf

WHERE 
	rvf.vendorname IS NOT NULL AND 
	rvf.vendorid IS NOT NULL AND 
	NOT EXISTS (SELECT * 
					FROM raw_vault.hubvendors hv 
					WHERE 
						hv.vendorhashkey = CAST({hash_func}(TRIM(BOTH FROM LOWER(rvf.vendorname)), {hash_param}) AS CHAR(64)));
