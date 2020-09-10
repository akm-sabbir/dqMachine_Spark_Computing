SET hive.execution.engine=tez;
SET odsposoutletitem_input ='/npd/s_test2/maps/upc_map/';
drop table if exists dqdictionaryhivedb.upc_maps_ext;
create external table dqdictionaryhivedb.upc_maps_ext(upc string, itemid bigint, count int, total int) row format delimited fields terminated by '|' lines terminated by '\n' location ${hiveconf:odsposoutletitem_input};
ALTER TABLE dqdictionaryhivedb.upc_maps_ext SET SERDEPROPERTIES ("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ");
