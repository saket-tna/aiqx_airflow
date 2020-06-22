SET hive.exec.compress.output=true;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
set hive.execution.engine=tez;
set hive.merge.mapfiles=false;
SET hive.default.fileformat=Orc;
set hive.tez.auto.reducer.parallelism=true;
set hive.optimize.ppd=true;
SET hive.optimize.ppd.storage=true;
set hive.optimize.index.filter=true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.fetch.task.conversion=more;
SET hive.vectorized.execution.enabled = TRUE;
SET hive.vectorized.execution.reduce.enabled = true;
SET mapred.job.reduce.input.buffer.percent = 1;
set mapred.reduce.tasks = -1;
SET mapred.inmem.merge.threshold = 0;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec = org.apache.hadoop.io.compress.SnappyCodec;
SET hive.tez.container.size=10240;
SET hive.tez.java.opts=-Xmx8192m;
SET mapreduce.map.memory.mb = 10240;
SET mapreduce.reduce.memory.mb = 10240;
SET mapreduce.map.java.opts = -Xmx8192m;
SET mapreduce.reduce.java.opts = -Xmx8192m;


Use uns_daily;


DROP TABLE IF EXISTS segment_feed;
CREATE EXTERNAL TABLE IF NOT EXISTS segment_feed(
  DT string,
  USER_ID_64 string,
  MEMBER_ID string,
  SEGMENT_ID string,
  IS_DAILY_UNIQUE string,
  IS_MONTHLY_UNIQUE string,
  EXTERNAL_VALUE string)
PARTITIONED BY (dayserial_numeric string) STORED AS RCFILE
LOCATION 's3://aiqdatabucket/aiq-inputfiles/segment_feed_rc/';



ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY30}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY29}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY28}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY27}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY26}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY25}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY24}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY23}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY22}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY21}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY20}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY19}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY18}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY17}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY16}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY15}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY14}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY13}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY12}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY11}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY10}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY9}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY8}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY7}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY6}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY5}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY4}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY3}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY2}');
ALTER TABLE segment_feed add if not exists PARTITION(dayserial_numeric='${DAY1}');


DROP TABLE IF EXISTS crm_stored_data;
create external table if not exists crm_stored_data
(
  user_id_64 string,
  segment_id string,
  dt date
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://aiqx/uns_modularize/crm_segments_data/';


DROP TABLE IF EXISTS dpi_dump_ids;
create external table if not exists dpi_dump_ids (
  userid                  bigint,
  degeocountry            string,
  hours_of_the_day        array<int>,
  days_of_the_week        array<int>,
  sitedomains             array<int>,
  adv_segments            array<int>,
  aud_segments            array<bigint>,
  demog_segments          array<bigint>,
  word array<int>,
  url_words               array<int>,
  brands                  array<int>,
  devices                 array<int>,
  cities                  array<bigint>,
  regions                 array<bigint>,
  titles                  array<string>,
  networks                array<string>,
  day_week                array<string>,
  cast_brands                  array<int>,
  domains_category        array<int>
) PARTITIONED BY (geo_country string)
stored as orc
LOCATION 's3://aiqx/uns_modularize/master_data/dayserial_numeric=${datedpi}/';



ALTER TABLE dpi_dump_ids add if not exists PARTITION(geo_country='GB');
ALTER TABLE dpi_dump_ids add if not exists PARTITION(geo_country='US');
ALTER TABLE dpi_dump_ids add if not exists PARTITION(geo_country='AU');
ALTER TABLE dpi_dump_ids add if not exists PARTITION(geo_country='DE');
ALTER TABLE dpi_dump_ids add if not exists PARTITION(geo_country='CA');
ALTER TABLE dpi_dump_ids add if not exists PARTITION(geo_country='SG');
ALTER TABLE dpi_dump_ids add if not exists PARTITION(geo_country='TH');
ALTER TABLE dpi_dump_ids add if not exists PARTITION(geo_country='MY');


DROP TABLE IF EXISTS dpi_dump_ids_daily;
create external table if not exists dpi_dump_ids_daily (
userid                  bigint,
degeocountry            string,
hours_of_the_day        array<int>,
days_of_the_week        array<int>,
sitedomains             array<int>,
adv_segments            array<int>,
aud_segments            array<bigint>,
demog_segments          array<bigint>,
word array<int>,
url_words               array<int>,
brands                  array<int>,
devices                 array<int>,
cities                  array<bigint>,
regions                 array<bigint>,
titles                  array<string>,
networks                array<string>,
day_week                array<string>,
cast_brands                  array<int>,
domains_category          array<int>
)
PARTITIONED BY ( geo_country string)
stored as orc
LOCATION 's3://aiqx/uns_modularize/automated-daily/master_data/dayserial_numeric=${DAYSERIAL_NUMERIC}/';


DROP TABLE IF EXISTS advertiser_segment_lu;
Create External Table IF NOT EXISTS advertiser_segment_lu(
  segment_id       BIGINT,
  segment_name     string,
  pixel_type       string,
  advertiser_id    string,
  miq_category     string,
  requiredForSrs   string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION 's3://mediaiq-apps-jarvis/connect/production/lookups/advertiser-segment/';


drop table if exists groupm_advertiser_segment_lu;
Create External Table IF NOT EXISTS groupm_advertiser_segment_lu(
  segment_id       BIGINT,
  segment_name     string,
  pixel_type       string,
  advertiser_id    string,
  miq_category     string,
  requiredForSrs   string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION 's3://aiqx/lookup_data/vidya_groupm_adv_segments_lu/'
tblproperties ("skip.header.line.count" = "1");


set hive.exec.compress.output = false;

DROP TABLE aiqx_univ_search_adv_segments_daily;
create external table aiqx_univ_search_adv_segments_daily(
  degeocountry            string,
  id        int,
  entity_type             string,
  users                   bigint,
  advertiser_id bigint,
  segment_name string)
PARTITIONED BY (dayserial_numeric bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION 's3://aiqx/uns_modularize/automated-daily/es_indexing/adv_segments/';
