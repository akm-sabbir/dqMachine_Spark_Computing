set hive.exec.dynamic.partition.mode=nonstrict;
select 
case 
when false then 
	insert overwrite table karun_test.odsitems  partition (businessid_parts)
	select item_id,business_id, subcategoryn,itemnumber,unitsperpackage,status,added,updated,country_code,
	case when business_id between 50 and 55 then 50
	when business_id between 56 and 60 then 60
	when business_id between 61 and 65 then 65
	when business_id between 66 and 70 then 70	
	when business_id between 71 and 90 then 80
	when business_id between 91 and 110 then 100
	when business_id between 111 and 131 then 120
	when business_id between 132 and 151 then 140
	when business_id between 152 and 172 then 160
	else 
     		180
	end as businessid  
	from karun_test.ods_positems_k_test;
else
	insert overwrite table karun_test.odsposoutletitems 
	select poi_id,business_id, posoutlet , outletdivision, outletdepartment , outletclass , outletbrand , outletitemnumber, outletdescription , outletbrandmatch, outletdescriptionmatch, manufacturercode, sku, itemid, itemtype, price, manufacturercodestatus, loadid, status, added, updated, matched_country_code, previous_poiid, parent_poiid, parent_poiid_status from karun_test.ods_posoutletitems_k_test
end ;
