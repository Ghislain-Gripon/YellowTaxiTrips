CREATE SCHEMA IF NOT EXISTS raw_vault;
CREATE TABLE IF NOT EXISTS raw_vault.hubpaymenttypes (
	paymenttypehashkey CHAR(64),
	paymenttypename varchar,
	loaddate timestamp,
	recordsource varchar
);
CREATE TABLE IF NOT EXISTS raw_vault.hubratecodeids (
	ratecodenamehashkey CHAR(64),
	ratecodename varchar,
	loaddate timestamp,
	recordsource varchar
);
CREATE TABLE IF NOT EXISTS raw_vault.hubtrips (
	triphashkey CHAR(64),
	loaddate timestamp,
	recordsource varchar,
	tpep_pickup_datetime timestamp,
	tpep_dropoff_datetime timestamp,
	puzoneid int4,
	dozoneid int4,
	vendorid int4,
	is_payment int4
);
CREATE TABLE IF NOT EXISTS raw_vault.hubvendors (
	vendorhashkey CHAR(64) NULL,
	vendorname varchar NULL,
	loaddate timestamp NULL,
	recordsource varchar NULL
);
CREATE TABLE IF NOT EXISTS raw_vault.hubzones (
	zonenamehashkey CHAR(64) NOT NULL,
	"zone" varchar NOT NULL,
	loaddate timestamp NOT NULL,
	recordsource varchar NOT NULL,
	borough varchar NULL,
	service_zone varchar NULL
);
CREATE TABLE IF NOT EXISTS raw_vault.satpaymenttypes_csv (
	paymenttypehashkey CHAR(64) NULL,
	paymenttypeid int4 NULL,
	paymenttypename varchar NULL,
	loaddate timestamp NULL,
	loadenddate timestamp NULL,
	recordsource varchar NULL
);
CREATE TABLE IF NOT EXISTS raw_vault.satratecodeids_csv (
	ratecodenamehashkey CHAR(64) NULL,
	ratecodeid int4 NULL,
	ratecodename varchar NULL,
	loaddate timestamp NULL,
	loadenddate timestamp NULL,
	recordsource varchar NULL
);
CREATE TABLE IF NOT EXISTS raw_vault.sattrips_csv (
	triphashkey CHAR(64) NOT NULL,
	loaddate timestamp NOT NULL,
	recordsource varchar NOT NULL,
	passenger_count int4 NULL,
	loadenddate timestamp NOT NULL,
	trip_distance float8 NULL,
	ratecodeid int4 NULL,
	store_and_fwd_flag varchar NULL,
	payment_type int4 NULL,
	fare_amount float8 NULL,
	extra float8 NULL,
	mta_tax float8 NULL,
	improvement_surcharge float8 NULL,
	tip_amount float8 NULL,
	tolls_amount float8 NULL,
	total_amount float8 NOT NULL,
	vendorid int4 NOT NULL,
	tpep_pickup_datetime timestamp NOT NULL,
	tpep_dropoff_datetime timestamp NOT NULL,
	puzoneid int4 NOT NULL,
	dozoneid int4 NOT NULL,
	is_payment int4 NOT NULL
);
CREATE TABLE IF NOT EXISTS raw_vault.satvendors_csv (
	vendorhashkey CHAR(64) NULL,
	vendorid int4 NULL,
	vendorname varchar NULL,
	loaddate timestamp NULL,
	recordsource varchar NULL,
	loadenddate timestamp NULL
);
CREATE TABLE IF NOT EXISTS raw_vault.satzones_csv (
	zonenamehashkey CHAR(64) NOT NULL,
	borough varchar NOT NULL,
	"zone" varchar NOT NULL,
	service_zone varchar NOT NULL,
	loaddate timestamp NOT NULL,
	loadenddate timestamp NOT NULL,
	recordsource varchar NOT NULL,
	zoneid int4 NOT NULL
);
CREATE TABLE IF NOT EXISTS raw_vault.linktrips(
	linktriphashkey CHAR(64) NOT NULL,
	triphashkey CHAR(64) NOT NULL,
	puzonenamehashkey CHAR(64) NOT NULL,
	dozonenamehashkey CHAR(64) NOT NULL,
	vendorhashkey CHAR(64) NOT NULL,
	ratecodenamehashkey CHAR(64) NOT NULL,
	paymenttypehashkey CHAR(64) NOT NULL,
	loaddate timestamp NOT NULL,
	recordsource varchar NOT NULL
);
