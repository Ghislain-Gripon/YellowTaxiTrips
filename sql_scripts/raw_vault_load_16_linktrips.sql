-- CONNECTION: name=postgres
DO $$
DECLARE

	loaddaterecord TIMESTAMP := CAST('{now}' AS TIMESTAMP);
	recordsourceorigin VARCHAR := TRIM(BOTH FROM LOWER('system'));
	loadenddaterecord TIMESTAMP := CAST('9999-12-30 00:00:00.000' AS TIMESTAMP);

BEGIN
	
	INSERT INTO raw_vault.linktrips(
		linktriphashkey,
		triphashkey,
		puzonenamehashkey,
		dozonenamehashkey,
		vendorhashkey,
		ratecodenamehashkey,
		paymenttypehashkey,
		loaddate,
		recordsource)
		
	SELECT DISTINCT ON (linktriphashkey)
		CAST(DIGEST(CONCAT(st.triphashkey, pusz.zonenamehashkey, dosz.zonenamehashkey, 
			pusz.zonenamehashkey, dosz.zonenamehashkey, sv.vendorhashkey, 
			src.ratecodenamehashkey, spt.paymenttypehashkey), '{hashfunc}') AS CHAR(64)) AS linktriphashkey,
		st.triphashkey,
		pusz.zonenamehashkey,
		dosz.zonenamehashkey,
		sv.vendorhashkey,
		src.ratecodenamehashkey,
		spt.paymenttypehashkey,
		loaddaterecord, 
		recordsourceorigin
		
	FROM raw_vault.sattrips_csv st
		JOIN raw_vault.satzones_csv pusz ON st.puzoneid = pusz.zoneid AND loaddaterecord < pusz.loadenddate AND loaddaterecord >= pusz.loaddate
		JOIN raw_vault.satzones_csv dosz ON st.dozoneid = dosz.zoneid AND loaddaterecord < dosz.loadenddate AND loaddaterecord >= dosz.loaddate
		JOIN raw_vault.satvendors_csv sv ON st.vendorid = sv.vendorid AND loaddaterecord < sv.loadenddate AND loaddaterecord >= sv.loaddate
		JOIN raw_vault.satratecodeids_csv src ON st.ratecodeid = src.ratecodeid AND loaddaterecord < src.loadenddate AND loaddaterecord >= src.loaddate
		JOIN raw_vault.satpaymenttypes_csv spt ON st.payment_type = spt.paymenttypeid AND loaddaterecord < spt.loadenddate AND loaddaterecord >= spt.loaddate

	WHERE 
		st.triphashkey IS NOT NULL AND
		pusz.zonenamehashkey IS NOT NULL AND 
		dosz.zonenamehashkey IS NOT NULL AND
		sv.vendorhashkey IS NOT NULL AND
		src.ratecodenamehashkey IS NOT NULL AND
		spt.paymenttypehashkey IS NOT NULL AND
		NOT EXISTS (SELECT * 
						FROM raw_vault.linktrips l 
						WHERE 
							l.linktriphashkey = CAST(DIGEST(CONCAT(st.triphashkey, pusz.zonenamehashkey, dosz.zonenamehashkey, 
													pusz.zonenamehashkey, dosz.zonenamehashkey, sv.vendorhashkey, 
													src.ratecodenamehashkey, spt.paymenttypehashkey), '{hashfunc}') AS CHAR(64)));
	
END $$;