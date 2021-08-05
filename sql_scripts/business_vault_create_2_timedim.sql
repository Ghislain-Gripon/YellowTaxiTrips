CREATE TABLE IF NOT EXISTS business_vault.timedim(
	timeid IDENTITY(0,1) PRIMARY KEY,
	"hour" SMALLINT,
	"day" SMALLINT,
	timestring TIMESTAMP)
	DISTSTYLE AUTO
	DISTKEY(timeid)
	SORTKEY(timeid);