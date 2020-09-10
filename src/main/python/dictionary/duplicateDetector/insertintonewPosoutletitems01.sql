SET hive.execution.engine=tez; 
--SET hive.cbo.enable=true;
--SET yarn.nodemanager.resource.memory-mb=224GB; 
--SET yarn.scheduler.minimum-allocation-mb=14GB
--SET yarn.scheduler.maximum-allocation-mb=224GB
--SET mapreduce.map.memory.mb=4GB
--SET mapreduce.reduce.memory.mb=8GB
--SET mapreduce.map.java.opts=3.2GB
--SET mapreduce.reduce.java.opts=6.4GB
--SET yarn.app.mapreduce.am.resource.mb=28000;
--SET yarn.app.mapreduce.am.command-opts=22400;
SET yarn.nodemanager.vmem-check-enabled=false;
--SET tez.am.resource.memory.mb=20GB;
SET tez.am.resource.memory.mb=28000;
SET tez.task.resource.memory.mb=28000;
SET tez.am.java.opts=-Xmx22000m;
SET hive.tez.container.size=28000;
SET hive.tez.java.opts=-Xmx22000m;
Set hive.merge.cardinality.check=false;
SET mapred.map.memory.mb=12000; 
SET mapred.map.java.opts.max.heap=-Xmx10800m;
SET mapred.reduce.memory.mb=28000; 
SET mapred.reduce.java.opts.max.heap=-Xmx22400m;
--SET hive.tez.container.size=24192;
--SET hive.tez.java.opts=-Xmx8192m;
--SET hive.exec.dynamic.partition.mode=nonstrict;
--SET hive.exec.max.dynamic.partitions=100000;
--SET hive.exec.max.dynamic.partitions.pernode=100000;
--SET yarn.scheduler.capacity.root.default.user-limit-factor=1;
--SET yarn.scheduler.capacity.root.default.user-limit-factor=0.33;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.auto.convert.join=true;
--SET mapreduce.tasktracker.map.tasks.maximum=400;
--SET mapreduce.tasktracker.reduce.tasks.maximum=400;
--SET hive.exec.parallel=true;
--SET mapreduce.compress.map.output=true;
SET mapreduce.output.compress=true;
SET hive.exec.compress.output=true;
SET hive.exec.parallel=true;
--SET hive.cbo.enable=true;
--SET hive.compute.query.using.stats=true;
--SET hive.stats.fetch.column.stats=true;
--SET hive.stats.fetch.partition.stats=true;
--SET hive.auto.convert.join=false;
--SET mapreduce.job.maps=400;
--SET mapreduce.job.reduces=100;
--SET hive.exec.dynamic.partition.pruning=false;
--SET hive.exec.dynamic.partition=true;
SET mapreduce.job.queue.name=long_running;
SET hive.exec.scratchdir=/hdpdata/hivetemp;
--SET yarn.nodemanager.vmem-pmem-ratio=2.1;
--SET yarn.nodemanager.vmem-pmem-ratio=2.1;
ADD FILE /hdpdata/dictionaryValidation/hive/uniquePosoutlet/itemIdWithPartition02.txt;
ADD FILE /hdpdata/udf-development/src/main/java/net/diybigdata/udf/property/basicConfiguration.txt;
ADD JAR /hdpdata/dictionaryValidation/hive/dqmachine-udf-1.4-SNAPSHOT.jar;
--ADD JAR /hdpdata/dictionaryValidation/hive/dqmachine-udf-1.0-SNAPSHOT.jar;
use dqdictionaryhivedb;
--use unique_posoutlet;
CREATE TEMPORARY FUNCTION partitiondeterminerChoice as 'net.diybigdata.udf.partitionDeterminer';
--CREATE TEMPORARY FUNCTION partitiondeterminerChoice01 as 'net.diybigdata.udf.partitionDeterminer';
select partitiondeterminerChoice(160046153,'/hdpdata/dictionaryValidation/hive/uniquePosoutlet/itemIdWithPartition06.txt','','',0);
insert overwrite table uniqueodsposoutletwpacid_int partition (partitioner)  select poi_id,business_id, posoutlet , outletdivision, outletdepartment , outletclass , outletbrand , outletitemnumber, outletdescription , outletbrandmatch, manufacturercode, sku, itemid, itemtype, price, manufacturercodestatus, loadid, status, added, updated, matched_country_code, previous_poiid, parent_poiid, parent_poiid_status, partitiondeterminerChoice(itemid,'/hdpdata/dictionaryValidation/hive/uniquePosoutlet/itemIdWithPartition06.txt','','',0) as partitioner
--case when bussiness_id between 50 and 55 then 50
--when bussiness_id between 56 and 60 then 60
--when bussiness_id between 61 and 65 then 65
--when bussiness_id between 66 and 70 then 70
--when bussiness_id between 71 and 75 then 75
--when bussiness_id between 76 and 80 then 80
--when bussiness_id between 81 and 85 then 85
--when bussiness_id between 86 and 90 then 90
--when bussiness_id between 91 and 110 then 110
--when bussiness_id between 111 and 130 then 130
--when bussiness_id between 131 and 150 then 150
--when bussiness_id between 151 and 172 then 172
--else
--       180
--end as
--bussinessid_parts

from dqdictionaryhivedb.uniqueodsposoutletitemswpext where itemid is not NULL ;--and bussiness_id =55;


--LOAD DATA INPATH 'hdfs:/npd/test/s_test1/tempodsposoutletitems' INTO TABLE dqdictionaryhivedb.uniqueodsposoutletitemswithpartition partition (partitioner = partitiondeterminerChoice(itemid,'./itemIdWithPartition.txt','',0) , bussinessid_parts = case when bussiness_id between 50 and 55 then 50
--when bussiness_id between 56 and 60 then 60
--when bussiness_id between 61 and 65 then 65
--when bussiness_id between 66 and 70 then 70
--when bussiness_id between 71 and 75 then 75
--when bussiness_id between 76 and 80 then 80
--when bussiness_id between 81 and 85 then 85
--when bussiness_id between 86 and 90 then 90
--when bussiness_id between 91 and 110 then 110
--when bussiness_id between 111 and 130 then 130
--when bussiness_id between 131 and 150 then 150
--when bussiness_id between 151 and 172 then 172
--else
--        180
--end); 
