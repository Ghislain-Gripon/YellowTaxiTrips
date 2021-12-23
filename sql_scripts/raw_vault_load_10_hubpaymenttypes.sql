INSERT INTO raw_vault.hubpaymenttypes(
	paymenttypehashkey,
	paymenttypename,
	loaddate,
	recordsource)

SELECT DISTINCT
	CAST({hash_func}(TRIM(BOTH FROM LOWER(rptf.paymenttype_name)), {hash_param}) AS CHAR(64)) AS paymenttypehashkey,
	TRIM(BOTH FROM LOWER(rptf.paymenttype_name)),
	CAST('{now}' AS TIMESTAMP),
	TRIM(BOTH FROM LOWER('{origin}'))

FROM (SELECT DISTINCT * FROM staging_area.rawpaymenttypesfile_csv) AS rptf

WHERE
	rptf.paymenttype_id IS NOT NULL AND 
	rptf.paymenttype_name IS NOT NULL AND 
	NOT EXISTS (SELECT * FROM raw_vault.hubpaymenttypes hpt 
					WHERE hpt.paymenttypehashkey = CAST({hash_func}(TRIM(BOTH FROM LOWER(rptf.paymenttype_name)),
						{hash_param}) AS CHAR(64)));