CREATE SCHEMA IF NOT EXISTS staging_area;

CREATE TABLE IF NOT EXISTS staging_area.rawpaymenttypesfile_csv (
	paymenttype_id varchar NULL,
	paymenttype_name varchar NULL
);


-- staging_area.rawratecodeidsfile_csv definition

-- Drop table

-- DROP TABLE staging_area.rawratecodeidsfile_csv;

CREATE TABLE IF NOT EXISTS staging_area.rawratecodeidsfile_csv (
	ratecodeid varchar NULL,
	ratecodename varchar NULL
);


-- staging_area.rawtripsfile_csv definition

-- Drop table

-- DROP TABLE staging_area.rawtripsfile_csv;

CREATE TABLE IF NOT EXISTS staging_area.rawtripsfile_csv (
	vendorid varchar NULL,
	tpep_pickup_datetime varchar NULL,
	tpep_dropoff_datetime varchar NULL,
	passenger_count varchar NULL,
	trip_distance varchar NULL,
	ratecodeid varchar NULL,
	store_and_fwd_flag varchar NULL,
	puzoneid varchar NULL,
	dozoneid varchar NULL,
	payment_type varchar NULL,
	fare_amount varchar NULL,
	extra varchar NULL,
	mta_tax varchar NULL,
	tip_amount varchar NULL,
	tolls_amount varchar NULL,
	improvement_surcharge varchar NULL,
	total_amount varchar NULL
);


-- staging_area.rawvendorsfile_csv definition

-- Drop table

-- DROP TABLE staging_area.rawvendorsfile_csv;

CREATE TABLE IF NOT EXISTS staging_area.rawvendorsfile_csv (
	vendorid varchar NULL,
	vendorname varchar NULL
);


-- staging_area.rawzonesfile_csv definition

-- Drop table

-- DROP TABLE staging_area.rawzonesfile_csv;

CREATE TABLE IF NOT EXISTS staging_area.rawzonesfile_csv (
	zoneid varchar NULL,
	borough varchar NULL,
	"zone" varchar NULL,
	service_zone varchar NULL
);