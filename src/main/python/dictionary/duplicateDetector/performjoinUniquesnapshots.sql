SET mapreduce.map.memory.mb=6000
SET mapreduce.map.java.opts=-Xmx9000m
SET mapreduce.reduce.memory.mb=9000
SET mapreduce.reduce.java.opts=-Xmx7200m
SET hive.tez.container.size=10240
SET hive.tez.java.opts=-Xmx8192m
SET hive.exec.dynamic.partition.mode=nonstrict
SET hive.exec.max.dynamic.partitions=100000
SET hive.exec.max.dynamic.partitions.pernode=100000
SET hive.exec.compress.output=false
select distinct dq.poi_id as poi_id,dq.business_id as business_id,dq.posoutlet as posoutlet,dq.outletdivision as outletdivision,dq.outletdepartment as outletdepartment,dq.outletclass as outletclass,dq.outletbrand as outletbrand,dq.outletitemnumber as outletitemnumber ,dq.outletdescription as outletdescription,dq.outletbrandmatch as outletbrandmatch,dq.manufacturercode as manufacturercode,dq.sku as sku,dq.itemid as itemid,dq.itemtype as itemtype ,dq.price as price ,dq.manufacturercodestatus as manufacturercodestatus ,dq.loadid as loadid,dq.status as status,dq.added as added, dq.updated as updated ,dq.matched_country_code as matched_country_code,dq.previous_poiid as previous_poiid,dq.parent_poiid as parent_poiid ,dq.parent_poiid_status as parent_poiid_status from dqdictionaryhivedb.maintempTableext as dq inner join dqdictionaryhivedb.uniquesnapshotstempTableext as temptab on (dq.poi_id = temptab.poi_id) where abs(unix_timestamp(cast(dq.updated as string)) - unix_timestamp(cast(temptab.updated as string) ))   = 0
