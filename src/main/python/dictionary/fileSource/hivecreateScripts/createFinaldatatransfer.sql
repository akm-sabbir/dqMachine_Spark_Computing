set hive.execution.engine=tez
SET odsposoutletitem_input = '/npd/s_test2/finalResults/'
drop table if exists dqdictionaryhivedb.finalDatatotransfer
create external table dqdictionaryhivedb.finalDatatotransfer(poi_id bigint, business_id int, posoutlet int, outletdivision string, outletdepartment string, outletclass string, outletbrand string,outletitemnumber string, outletdescription string, outletbrandmatch string, manufacturercode bigint, sku string, itemid bigint, itemtype string, price int,manufacturercodestatus int, loadid int, status int, added timestamp, updated timestamp, matched_country_code int, previous_poiid int ,parent_poiid int , parent_poiid_status string,partitioner string)row format delimited fields terminated by '|' lines terminated by '\n' location ${hiveconf:odsposoutletitem_input}
ALTER TABLE dqdictionaryhivedb.maintempTableext SET SERDEPROPERTIES ("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ")
