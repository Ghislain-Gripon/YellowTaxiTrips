CREATE TABLE IF NOT EXISTS business_vault.timedim(
	timeid INT PRIMARY KEY,
	"hour" SMALLINT,
	"day" SMALLINT,
	timestring TIMESTAMP);
