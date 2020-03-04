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


DROP TABLE adv_segments_counts;
create table adv_segments_counts as
select a.degeocountry,a.adv_segments as id,'adv_segments' as entity_type,count(a.userid) as users from
(select userid, degeocountry,adv_segments2 as adv_segments
from dpi_dump_ids_new lateral view explode (adv_segments) dpi_dump_ids_new as adv_segments2) as a  group by  a.degeocountry,a.adv_segments;
