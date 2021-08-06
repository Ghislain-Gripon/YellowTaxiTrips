INSERT INTO business_vault.timedim(
	timeid,
	"hour",
	"day",
	timestring)
SELECT DISTINCT 
	timeid,
    "hour",
    "day",
    timestring
FROM raw_vault.timedim;