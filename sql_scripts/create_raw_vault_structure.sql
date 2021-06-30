-- CONNECTION: name=postgres
-- DROP SCHEMA raw_vault;

CREATE SCHEMA IF NOT EXISTS raw_vault;
-- Drop table

-- DROP TABLE raw_vault.hubpaymenttypes;

CREATE TABLE IF NOT EXISTS raw_vault.hubpaymenttypes (
	paymenttypehashkey CHAR(64) NULL,
	paymenttypename varchar NULL,
	loaddate timestamp(0) NULL,
	recordsource varchar NULL,
	CONSTRAINT hubpaymenttypes_un UNIQUE (paymenttypehashkey)
);


-- raw_vault.hubratecodeids definition

-- Drop table

-- DROP TABLE raw_vault.hubratecodeids;

CREATE TABLE IF NOT EXISTS raw_vault.hubratecodeids (
	ratecodenamehashkey CHAR(64) NULL,
	ratecodename varchar NULL,
	loaddate timestamp(0) NULL,
	recordsource varchar NULL,
	CONSTRAINT hubratecodeids_un UNIQUE (ratecodenamehashkey)
);


-- raw_vault.hubtrips definition

-- Drop table

-- DROP TABLE raw_vault.hubtrips;

CREATE TABLE IF NOT EXISTS raw_vault.hubtrips (
	triphashkey CHAR(64) NOT NULL,
	loaddate timestamp(0) NOT NULL,
	recordsource varchar NOT NULL,
	tpep_pickup_datetime timestamp(0) NOT NULL,
	tpep_dropoff_datetime timestamp(0) NOT NULL,
	puzoneid int4 NOT NULL,
	dozoneid int4 NOT NULL,
	vendorid int4 NOT NULL,
	is_payment int4 NOT NULL,
	CONSTRAINT hubtrips_un UNIQUE (triphashkey)
);
CREATE INDEX IF NOT EXISTS tpep_dropoff_hubtrips_index ON raw_vault.hubtrips USING btree (tpep_dropoff_datetime);
CREATE INDEX IF NOT EXISTS tpep_pickup_hubtrips_index ON raw_vault.hubtrips USING btree (tpep_pickup_datetime);


-- raw_vault.hubvendors definition

-- Drop table

-- DROP TABLE raw_vault.hubvendors;

CREATE TABLE IF NOT EXISTS raw_vault.hubvendors (
	vendorhashkey CHAR(64) NULL,
	vendorname varchar NULL,
	loaddate timestamp(0) NULL,
	recordsource varchar NULL,
	CONSTRAINT hubvendors_csv_un UNIQUE (vendorhashkey)
);


-- raw_vault.hubzones definition

-- Drop table

-- DROP TABLE raw_vault.hubzones;

CREATE TABLE IF NOT EXISTS raw_vault.hubzones (
	zonenamehashkey CHAR(64) NOT NULL,
	"zone" varchar NOT NULL,
	loaddate timestamp(0) NOT NULL,
	recordsource varchar NOT NULL,
	borough varchar NULL,
	service_zone varchar NULL,
	CONSTRAINT hublocation_un UNIQUE (zonenamehashkey)
);


-- raw_vault.rawpaymenttypesfile definition

-- Drop table

-- DROP TABLE raw_vault.rawpaymenttypesfile;




-- raw_vault.satpaymenttypes definition

-- Drop table

-- DROP TABLE raw_vault.satpaymenttypes;

CREATE TABLE IF NOT EXISTS raw_vault.satpaymenttypes_csv (
	paymenttypehashkey CHAR(64) NULL,
	paymenttypeid int4 NULL,
	paymenttypename varchar NULL,
	loaddate timestamp(0) NULL,
	loadenddate timestamp(0) NULL,
	recordsource varchar NULL,
	CONSTRAINT satpaymenttypes_csv_un UNIQUE (paymenttypehashkey, loaddate)
);


-- raw_vault.satratecodeids definition

-- Drop table

-- DROP TABLE raw_vault.satratecodeids;

CREATE TABLE IF NOT EXISTS raw_vault.satratecodeids_csv (
	ratecodenamehashkey CHAR(64) NULL,
	ratecodeid int4 NULL,
	ratecodename varchar NULL,
	loaddate timestamp(0) NULL,
	loadenddate timestamp(0) NULL,
	recordsource varchar NULL,
	CONSTRAINT satratecodeids_csv_un UNIQUE (ratecodenamehashkey, loaddate)
);


-- raw_vault.sattrips definition

-- Drop table

-- DROP TABLE raw_vault.sattrips;

CREATE TABLE IF NOT EXISTS raw_vault.sattrips_csv (
	triphashkey CHAR(64) NOT NULL,
	loaddate timestamp(0) NOT NULL,
	recordsource varchar NOT NULL,
	passenger_count int4 NULL,
	loadenddate timestamp(0) NOT NULL,
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
	tpep_pickup_datetime timestamp(0) NOT NULL,
	tpep_dropoff_datetime timestamp(0) NOT NULL,
	puzoneid int4 NOT NULL,
	dozoneid int4 NOT NULL,
	is_payment int4 NOT NULL,
	CONSTRAINT sattrips_csv_un UNIQUE (triphashkey, loaddate)
);
CREATE INDEX IF NOT EXISTS tpep_dropoff_sattrips_csv_index ON raw_vault.sattrips_csv USING btree (tpep_dropoff_datetime);
CREATE INDEX IF NOT EXISTS tpep_pickup_sattrips_csv_index ON raw_vault.sattrips_csv USING btree (tpep_pickup_datetime);


-- raw_vault.satvendors definition

-- Drop table

-- DROP TABLE raw_vault.satvendors;

CREATE TABLE IF NOT EXISTS raw_vault.satvendors_csv (
	vendorhashkey CHAR(64) NULL,
	vendorid int4 NULL,
	vendorname varchar NULL,
	loaddate timestamp(0) NULL,
	recordsource varchar NULL,
	loadenddate timestamp(0) NULL,
	CONSTRAINT satvendors_csv_un UNIQUE (vendorhashkey, loaddate)
);


-- raw_vault.satzones definition

-- Drop table

-- DROP TABLE raw_vault.satzones;

CREATE TABLE IF NOT EXISTS raw_vault.satzones_csv (
	zonenamehashkey CHAR(64) NOT NULL,
	borough varchar NOT NULL,
	"zone" varchar NOT NULL,
	service_zone varchar NOT NULL,
	loaddate timestamp(0) NOT NULL,
	loadenddate timestamp(0) NOT NULL,
	recordsource varchar NOT NULL,
	zoneid int4 NOT NULL,
	CONSTRAINT satzones_csv_un UNIQUE (zonenamehashkey, loaddate)
);


-- raw_vault.linktrips definition

-- Drop table

-- DROP TABLE raw_vault.linktrips

CREATE TABLE IF NOT EXISTS raw_vault.linktrips(
	linktriphashkey CHAR(64) NOT NULL,
	triphashkey CHAR(64) NOT NULL,
	puzonenamehashkey CHAR(64) NOT NULL,
	dozonenamehashkey CHAR(64) NOT NULL,
	vendorhashkey CHAR(64) NOT NULL,
	ratecodenamehashkey CHAR(64) NOT NULL,
	paymenttypehashkey CHAR(64) NOT NULL,
	loaddate timestamp(0) NOT NULL,
	recordsource varchar NOT NULL,
	CONSTRAINT linktrip_un UNIQUE (linktriphashkey)
);
