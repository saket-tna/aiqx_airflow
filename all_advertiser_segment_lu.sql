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


Use saket;


drop table if exists all_advertiser_segment_lu;
create table all_advertiser_segment_lu as
select segment_id, segment_name, pixel_type, advertiser_id, miq_category, requiredForSrs
from advertiser_segment_lu
union all
select segment_id, segment_name, pixel_type, advertiser_id, miq_category, requiredForSrs
from groupm_advertiser_segment_lu;