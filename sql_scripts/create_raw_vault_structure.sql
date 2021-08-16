CREATE SCHEMA IF NOT EXISTS raw_vault;
CREATE TABLE IF NOT EXISTS raw_vault.hubpaymenttypes (
	paymenttypehashkey CHAR(64) NOT NULL PRIMARY KEY,
	paymenttypename varchar,
	loaddate timestamp,
	recordsource varchar
	DISTSTYLE AUTO
	DISTKEY(paymenttypehashkey)
	SORTKEY(paymenttypehashkey));

CREATE TABLE IF NOT EXISTS raw_vault.hubratecodeids (
	ratecodenamehashkey CHAR(64) NOT NULL PRIMARY KEY,
	ratecodename varchar,
	loaddate timestamp,
	recordsource varchar
	DISTSTYLE AUTO
	DISTKEY(ratecodenamehashkey)
	SORTKEY(ratecodenamehashkey));

CREATE TABLE IF NOT EXISTS raw_vault.hubtrips (
	triphashkey CHAR(64) NOT NULL PRIMARY KEY,
	loaddate timestamp,
	recordsource varchar,
	tpep_pickup_datetime timestamp,
	tpep_dropoff_datetime timestamp,
	puzoneid int4,
	dozoneid int4,
	vendorid int4,
	is_payment int4
	DISTSTYLE AUTO
	DISTKEY(triphashkey)
	SORTKEY(triphashkey));

CREATE TABLE IF NOT EXISTS raw_vault.hubvendors (
	vendorhashkey CHAR(64) NOT NULL PRIMARY KEY,
	vendorname varchar NOT NULL,
	loaddate timestamp NOT NULL,
	recordsource varchar NOT NULL
	DISTSTYLE AUTO
	DISTKEY(vendorhashkey)
	SORTKEY(vendorhashkey));

CREATE TABLE IF NOT EXISTS raw_vault.hubzones (
	zonenamehashkey CHAR(64) NOT NULL PRIMARY KEY,
	"zone" varchar NOT NULL,
	loaddate timestamp NOT NULL,
	recordsource varchar NOT NULL,
	borough varchar NOT NULL,
	service_zone varchar NOT NULL
	DISTSTYLE AUTO
	DISTKEY(zonenamehashkey)
	SORTKEY(zonenamehashkey));

CREATE TABLE IF NOT EXISTS raw_vault.satpaymenttypes_csv (
	paymenttypehashkey CHAR(64) NOT NULL PRIMARY KEY,
	paymenttypeid int4 NOT NULL,
	paymenttypename varchar NOT NULL,
	loaddate timestamp NOT NULL,
	loadenddate timestamp NOT NULL,
	recordsource varchar NOT NULL
	DISTSTYLE AUTO
	DISTKEY(paymenttypehashkey)
	SORTKEY(paymenttypehashkey));

CREATE TABLE IF NOT EXISTS raw_vault.satratecodeids_csv (
	ratecodenamehashkey CHAR(64) NOT NULL PRIMARY KEY,
	ratecodeid int4 NOT NULL,
	ratecodename varchar NOT NULL,
	loaddate timestamp NOT NULL,
	loadenddate timestamp NOT NULL,
	recordsource varchar NOT NULL
	DISTSTYLE AUTO
	DISTKEY(ratecodenamehashkey)
	SORTKEY(ratecodenamehashkey));

CREATE TABLE IF NOT EXISTS raw_vault.sattrips_csv (
	triphashkey CHAR(64) NOT NULL PRIMARY KEY,
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
	tpep_pickup_datetime timestamp NULL,
	tpep_dropoff_datetime timestamp NULL,
	puzoneid int4 NOT NULL,
	dozoneid int4 NOT NULL,
	is_payment int4 NOT NULL
	DISTSTYLE AUTO
	DISTKEY(triphashkey)
	SORTKEY(triphashkey));

CREATE TABLE IF NOT EXISTS raw_vault.satvendors_csv (
	vendorhashkey CHAR(64) NOT NULL PRIMARY KEY,
	vendorid int4 NOT NULL,
	vendorname varchar NOT NULL,
	loaddate timestamp NOT NULL,
	recordsource varchar NOT NULL,
	loadenddate timestamp NOT NULL
	DISTSTYLE AUTO
	DISTKEY(vendorhashkey)
	SORTKEY(vendorhashkey));

CREATE TABLE IF NOT EXISTS raw_vault.satzones_csv (
	zonenamehashkey CHAR(64) NOT NULL PRIMARY KEY,
	borough varchar NOT NULL,
	"zone" varchar NOT NULL,
	service_zone varchar NOT NULL,
	loaddate timestamp NOT NULL,
	loadenddate timestamp NOT NULL,
	recordsource varchar NOT NULL,
	zoneid int4 NOT NULL
	DISTSTYLE AUTO
	DISTKEY(zonenamehashkey)
	SORTKEY(zonenamehashkey));

CREATE TABLE IF NOT EXISTS raw_vault.linktrips(
	linktriphashkey CHAR(64) NOT NULL PRIMARY KEY,
	triphashkey CHAR(64) NOT NULL,
	puzonenamehashkey CHAR(64) NOT NULL,
	dozonenamehashkey CHAR(64) NOT NULL,
	vendorhashkey CHAR(64) NOT NULL,
	ratecodenamehashkey CHAR(64) NOT NULL,
	paymenttypehashkey CHAR(64) NOT NULL,
	loaddate timestamp NOT NULL,
	recordsource varchar NOT NULL
	DISTSTYLE AUTO
	DISTKEY(triphashkey)
	SORTKEY(triphashkey));
