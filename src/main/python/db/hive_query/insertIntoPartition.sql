insert into table odsitems partition (businessid)
select item_id, subcategoryn,itemnumber,unitsperpackage,status,added,updated,country_code,
case when business_id between 50 and 70 then 60
when business_id between 71 and 90 then 80
when business_id between 91 and 110 then 100
when business_id between 111 and 131 then 120
when business_id between 132 and 151 then 140
when business_id between 152 and 172 then 160
else 
	180
end as businessid  
from ods_positems_k_test;

