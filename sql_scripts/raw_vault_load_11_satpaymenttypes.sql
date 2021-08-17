INSERT INTO raw_vault.satpaymenttypes_csv(
	paymenttypehashkey,
	paymenttypeid,
	paymenttypename,
	loaddate,
	loadenddate,
	recordsource)

SELECT DISTINCT
	CAST(SHA2(TRIM(BOTH FROM LOWER(rptf.paymenttype_name)), {hash_size}) AS CHAR(64)) AS paymenttypehashkey,
	CAST(rptf.paymenttype_id AS INTEGER),
	TRIM(BOTH FROM LOWER(rptf.paymenttype_name)),
	CAST('{now}' AS TIMESTAMP),
	CAST('9999-12-30 00:00:00.000' AS TIMESTAMP),
	TRIM(BOTH FROM LOWER('{origin}'))

FROM (SELECT DISTINCT * FROM staging_area.rawpaymenttypesfile_csv) AS rptf

WHERE 
	rptf.paymenttype_id IS NOT NULL AND 
	rptf.paymenttype_name IS NOT NULL AND 
	NOT EXISTS (SELECT * FROM raw_vault.satpaymenttypes_csv spt 
	WHERE spt.paymenttypehashkey = CAST(SHA2(TRIM(BOTH FROM LOWER(rptf.paymenttype_name)), {hash_size}) AS CHAR(64))
		AND spt.loaddate = CAST('{now}' AS TIMESTAMP));
