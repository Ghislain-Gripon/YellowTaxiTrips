INSERT INTO raw_vault.hubpaymenttypes(
	paymenttypehashkey,
	paymenttypename,
	loaddate,
	recordsource)

SELECT DISTINCT
	CAST(SHA2(TRIM(BOTH FROM LOWER(rptf.paymenttype_name)), {hash_size}) AS CHAR(64)) AS paymenttypehashkey,
	TRIM(BOTH FROM LOWER(rptf.paymenttype_name)),
	CAST('{now}' AS TIMESTAMP),
	TRIM(BOTH FROM LOWER('{origin}'))

FROM (SELECT DISTINCT * FROM staging_area.rawpaymenttypesfile_csv) AS rptf

WHERE
	rptf.paymenttype_id IS NOT NULL AND 
	rptf.paymenttype_name IS NOT NULL AND 
	NOT EXISTS (SELECT * FROM raw_vault.hubpaymenttypes hpt 
					WHERE hpt.paymenttypehashkey = CAST(SHA2(TRIM(BOTH FROM LOWER(rptf.paymenttype_name)),
						{hash_size}) AS CHAR(64)));