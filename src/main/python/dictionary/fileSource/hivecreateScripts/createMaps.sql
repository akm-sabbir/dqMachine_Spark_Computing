SET hive.execution.engine=tez;
SET odsposoutletitem_input ='/npd/test/maps/dictionary/itemid_maps2/upc_map';
--SET odsposoutletitem_input ='/npd/test/maps/dictionary/itemid_maps2/sku_map';
drop table if exists dqdictionaryhivedb.sku_maps_ext;
create external table dqdictionaryhivedb.sku_maps_ext(UPC string, ITEMID bigint, COUNT int, TOTAL int) row format delimited fields terminated by '|' lines terminated by '\n' location ${hiveconf:odsposoutletitem_input};
ALTER TABLE dqdictionaryhivedb.upc_maps_ext SET SERDEPROPERTIES ("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ");
