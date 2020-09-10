set hive.exec.dynamic.partition.mode=nonstrict;

set mapreduce.map.memory.mb=6000;
set mapreduce.map.java.opts=-Xmx9000m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx7200m;
set hive.tez.container.size=10240;
set hive.tez.java.opts=-Xmx8192m;
set hive.exec.dynamic.partition.mode=nonstrict

ADD FILE /home/abdul.sabbir/hive/itemIdWithPartition.text;
ADD FILE /home/abdul.sabbir/udf-development/src/main/java/net/diybigdata/udf/property/basicConfiguration.txt;
ADD JAR dqmachine-udf-1.1-SNAPSHOT.jar;

Create temporary FUNCTION partitiondeterminerChoice as 'net.diybigdata.udf.partitionDeterminer';

insert into table karun_test.odsposoutletitemswp partition (partitioner, business_part) 
select poi_id, business_id, posoutlet, outletdivision, outletdepartment, outletclass, outletbrand, outletitemnumber, outletdescription, outletbrandmatch, outletdescriptionmatch, manufacturercode, sku, itemid, itemtype, price, manufacturercodestatus, loadid, status, added, updated, matched_country_code, previous_poiid, parent_poiid, parent_poiid_status, partitiondeterminerChoice(itemid,'./itemIdWithPartition.txt','./basicConfiguration.txt',0) as partitioner,

case when business_id between 50 and 55 then 50
when business_id between 56 and 60 then 60
when business_id between 61 and 65 then 65
when business_id between 66 and 70 then 70
when business_id between 71 and 75 then 75
when business_id between 76 and 80 then 80
when business_id between 81 and 85 then 85
when business_id between 86 and 90 then 90
when business_id between 91 and 110 then 110
when business_id between 111 and 130 then 130
when business_id between 131 and 150 then 150
when business_id between 151 and 172 then 172
else
	180
end as 
business_part

from karun_test.odsposoutletitems_test_u where itemid is not NULL;
