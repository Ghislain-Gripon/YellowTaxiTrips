-- CONNECTION: name=postgres
DO $$
DECLARE

	loaddaterecord TIMESTAMP := CAST('{now}' AS TIMESTAMP);
	recordsourceorigin VARCHAR := TRIM(BOTH FROM LOWER('{origin}'));
	loadenddaterecord TIMESTAMP := CAST('9999-12-30 00:00:00.000' AS TIMESTAMP);

BEGIN
	
	INSERT INTO raw_vault.satpaymenttypes_csv(
		paymenttypehashkey,
		paymenttypeid,
		paymenttypename,
		loaddate,
		loadenddate,
		recordsource)
	
	SELECT DISTINCT ON (paymenttypehashkey)
		CAST(DIGEST(TRIM(BOTH FROM LOWER(rptf.paymenttype_name)),'{hashfunc}') AS CHAR(64)) AS paymenttypehashkey,
		CAST(rptf.paymenttype_id AS INTEGER),
		TRIM(BOTH FROM LOWER(rptf.paymenttype_name)),
		loaddaterecord,
		loadenddaterecord,
		recordsourceorigin
	
	FROM (SELECT DISTINCT * FROM staging_area.rawpaymenttypesfile_csv) AS rptf
	
	WHERE 
		rptf.paymenttype_id IS NOT NULL AND 
		rptf.paymenttype_name IS NOT NULL AND 
		NOT EXISTS (SELECT * FROM raw_vault.satpaymenttypes_csv spt 
		WHERE spt.paymenttypehashkey = CAST(DIGEST(TRIM(BOTH FROM LOWER(rptf.paymenttype_name)),'{hashfunc}') AS CHAR(64))
			AND spt.loaddate = loaddaterecord);
			
END $$;