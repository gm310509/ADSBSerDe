--*******************************************************************************
-- Create ACTREF table in Hive.
-- G. McCall
-- 
-- Assumes hive is run as follows:
--   hive -d DB=skynet
--
-- Since this table definition uses my ADS-B SerDe, there is
-- no need to preprocess the adsb file before loading.
--
-- copy converted adsb files into HDFS
--   hdfs dfs -mkdir /skynet/transponder/adsbSerDe
--   hdfs dfs -put skynet/adsb/site01/2014/* /skynet/transponder/adsbSerDe
--   hdfs dfs -put skynet/adsb/site02/2014/* /skynet/transponder/adsbSerDe
-- 
--*******************************************************************************/

create database if not exists ${DB};

drop table if exists ${DB}.adsbSerDe;

ADD JAR ADSBSerDe/target/ADSBSerDe-1.0.jar;

create external table if not exists ${DB}.adsbSerDe (
    clock       Timestamp,
    hexid       String,
    ident       String,
    squawk      Int,
    alt         Int,
    speed       Int,
    heading     Int,
    airGround   String,
    lat         float,
    lon         float
)
ROW FORMAT SERDE 'com.teradata.adsbserde.AdsbSerDe'
location '/skynet/transponder/adsbSerDe'
tblproperties("skip.header.line.count"="1")
;

describe formatted ${DB}.adsbSerDe;

--load data inpath '/data/skynet/aircraft/ACFTREF.txt' into table ${DB}.actref ;

select * from ${DB}.adsbSerDe limit 20;

