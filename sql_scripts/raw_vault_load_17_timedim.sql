INSERT INTO raw_vault.timedim(
    timeid,
    "hour",
	"day",
	timestring)

SELECT
    CAST(timeid as INTEGER),
    CAST("hour" as SMALLINT),
    CAST("day" as SMALLINT),
    CAST(timestring as TIMESTAMP)
    
FROM staging_area.rawtimedimfile_csv;