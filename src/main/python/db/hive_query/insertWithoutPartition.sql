insert into table odsitems(itemid,businessid,subcategoryn,itemnumber,unitsperpackage,status,added,updated,country_code) 
select itemid,businessid,subcategoryn,itemnumber,unitsperpackage,status,added,updated,country_code from ods_positems_k_test;
