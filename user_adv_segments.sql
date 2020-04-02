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


DROP TABLE user_adv_segments1;
create  table user_adv_segments1 as
select a.USER_ID_64, a.segment_id from
(select USER_ID_64,cast(SEGMENT_ID as int) as segment_id from  segment_feed GROUP BY USER_ID_64,SEGMENT_ID
union
select USER_ID_64,cast(SEGMENT_ID as int) as segment_id from crm_stored_data GROUP BY USER_ID_64,SEGMENT_ID) as a
group by a.USER_ID_64, a.segment_id;


DROP TABLE user_adv_segments2;
create  table user_adv_segments2 as
SELECT USER_ID_64, COLLECT_set(SEGMENT_ID) as segments from user_adv_segments1 GROUP BY USER_ID_64;
