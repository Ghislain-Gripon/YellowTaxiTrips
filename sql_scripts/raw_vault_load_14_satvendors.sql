INSERT INTO raw_vault.satvendors_csv(
	vendorhashkey,
	vendorname,
	loaddate,
	recordsource,
	vendorid,
	loadenddate)

SELECT DISTINCT
	CAST({hash_func}(TRIM(BOTH FROM LOWER(rvf.vendorname)), {hash_param}) AS CHAR(64)) AS vendorhashkey,
	TRIM(BOTH FROM LOWER(rvf.vendorname)),
	CAST('{now}' AS TIMESTAMP),
	TRIM(BOTH FROM LOWER('{origin}')),
	CAST(rvf.vendorid AS INTEGER),
	CAST('9999-12-30 00:00:00.000' AS TIMESTAMP)
	
FROM (SELECT DISTINCT * FROM staging_area.rawvendorsfile_csv) AS rvf

WHERE 
	rvf.vendorname IS NOT NULL AND 
	rvf.vendorid IS NOT NULL AND 
	NOT EXISTS (SELECT * 
					FROM raw_vault.satvendors_csv sv 
					WHERE 
						sv.vendorhashkey = CAST({hash_func}(TRIM(BOTH FROM LOWER(rvf.vendorname)), {hash_param}) AS CHAR(64))
						AND sv.loaddate = CAST('{now}' AS TIMESTAMP));
