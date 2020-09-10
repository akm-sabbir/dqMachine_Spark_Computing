set mapreduce.map.memory.mb=6000;
set mapreduce.map.java.opts=-Xmx9000m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx7200m;
SET hive.tez.container.size=10240;
SET hive.tez.java.opts=-Xmx8192m;
set hive.exec.dynamic.partition.mode=nonstrict;
ADD FILE /home/abdul.sabbir/hive/itemIdWithPartition.txt;
ADD FILE /home/abdul.sabbir/udf-development/src/main/java/net/diybigdata/property/basicConfiguration.txt;

ADD JAR dqmachine-udf-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION partitiondeterminer as 'net.diybigdata.udf.partitionDeterminer';

insert overwrite table karun_test.odsposoutletitemswithpartition partition (partitioner,business_part)
select poi_id,business_id, posoutlet , outletdivision, outletdepartment , outletclass , outletbrand , outletitemnumber, outletdescription , outletbrandmatch, outletdescriptionmatch, manufacturercode, sku, itemid, itemtype, price, manufacturercodestatus, loadid, status, added, updated, matched_country_code, previous_poiid, parent_poiid, parent_poiid_status, partitiondeterminer(itemid,'./itemIdWithPartition.txt') as partitioner from karun_test.odsposoutletitems_test_u where itemid is not NULL;
