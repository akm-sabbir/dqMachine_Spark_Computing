set hive.execution.engine=mr;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.index.compact.binary.search = true;
use karun_test;
create table if not exists ODSITEMSWP 
(ITEMID bigint,BUSINESSID int, SUBCATEGRYN int, ITEMNUMBER int, UNITSPERPACKAGE int, 
STATUS int, ADDED Date, UPDATED Date, COUNTRY_CODE int ) 
partitioned by (businessid_parts int)
STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY','orc.create.index'='true','orc.bloom.filter.columns'='ITEMID','orc.bloom.filter.fpp'='0.05','orc.stripe.size'='268435456','orc.row.index.stride'='100000');

create table if not exists ODSPOSOUTLETITEMSWP (poi_id bigint, business_id int, posoutlet int, outletdivision string, outletdepartment string,
outletclass string, outletbrand string, outletitemnumber string, outletdescription string, outletbrandmatch string, outletdescriptionmatch string, 
manufacturercode bigint, sku string, itemid bigint, itemtype string, price int, manufacturercodestatus int, loadid int, status int, added timestamp, 
updated timestamp, matched_country_code int, previous_poiid int,parent_poiid int ,parent_poiid_status string) 
partitioned by (partitioner string)
clustered by (business_id) into 10 buckets
STORED AS 
ORC TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY','ORC.CREATE.INDEX'='true','orc.bloom.filter.columns'='manufacturercode','orc.bloom.filter.fpp'='0.05','orc.stripe.size'='268435456','orc.row.index.stride'='100000');
