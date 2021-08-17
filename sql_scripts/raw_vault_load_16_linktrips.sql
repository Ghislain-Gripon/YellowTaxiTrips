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
	
SELECT DISTINCT
	CAST(SHA2(CONCAT(st.triphashkey, pusz.zonenamehashkey, dosz.zonenamehashkey, 
		pusz.zonenamehashkey, dosz.zonenamehashkey, sv.vendorhashkey, 
		src.ratecodenamehashkey, spt.paymenttypehashkey), {hash_size}) AS CHAR(64)) AS linktriphashkey,
	st.triphashkey,
	pusz.zonenamehashkey,
	dosz.zonenamehashkey,
	sv.vendorhashkey,
	src.ratecodenamehashkey,
	spt.paymenttypehashkey,
	CAST('{now}' AS TIMESTAMP), 
	TRIM(BOTH FROM LOWER('origin'))
	
FROM (SELECT DISTINCT * FROM raw_vault.sattrips_csv) AS st
	JOIN raw_vault.satzones_csv pusz ON st.puzoneid = pusz.zoneid AND CAST('{now}' AS TIMESTAMP) < pusz.loadenddate AND CAST('{now}' AS TIMESTAMP) >= pusz.loaddate
	JOIN raw_vault.satzones_csv dosz ON st.dozoneid = dosz.zoneid AND CAST('{now}' AS TIMESTAMP) < dosz.loadenddate AND CAST('{now}' AS TIMESTAMP) >= dosz.loaddate
	JOIN raw_vault.satvendors_csv sv ON st.vendorid = sv.vendorid AND CAST('{now}' AS TIMESTAMP) < sv.loadenddate AND CAST('{now}' AS TIMESTAMP) >= sv.loaddate
	JOIN raw_vault.satratecodeids_csv src ON st.ratecodeid = src.ratecodeid AND CAST('{now}' AS TIMESTAMP) < src.loadenddate AND CAST('{now}' AS TIMESTAMP) >= src.loaddate
	JOIN raw_vault.satpaymenttypes_csv spt ON st.payment_type = spt.paymenttypeid AND CAST('{now}' AS TIMESTAMP) < spt.loadenddate AND CAST('{now}' AS TIMESTAMP) >= spt.loaddate

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
						l.linktriphashkey = CAST(SHA2(CONCAT(st.triphashkey, pusz.zonenamehashkey, dosz.zonenamehashkey, 
												pusz.zonenamehashkey, dosz.zonenamehashkey, sv.vendorhashkey, 
												src.ratecodenamehashkey, spt.paymenttypehashkey), {hash_size}) AS CHAR(64)));
