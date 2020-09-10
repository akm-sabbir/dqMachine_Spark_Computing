SET mapreduce.map.memory.mb=6000
SET mapreduce.map.java.opts=-Xmx9000m
SET mapreduce.reduce.memory.mb=9000
SET mapreduce.reduce.java.opts=-Xmx7200m
SET hive.tez.container.size=10240
SET hive.tez.java.opts=-Xmx8192m
select distinct poi_id,business_id, posoutlet, outletdivision, outletdepartment,  outletsubdepartment,  outletclass, outletsubclass, outletbrand, outletitemnumber , outletdescription, outletbrandmatch, outletitemnumbermatch ,outletdescriptionmatch,sku, manufacturercodetype,  manufacturercode, zzzppmonthfrom, zzzppmonthto, zzzppmonthlastused,  itemid,itemtype , price , manufacturercodestatus , loadid, status, added, updated , ppweekfrom, ppweekto, ppweeklastused, matched_country_code, previous_poiid, include_data_ppmonthfrom , include_data_ppweekfrom, manufacturercodematch , skumatch, unitofmeasure, packsize,  manufacturername, manufacturernamematch, privatelabel, outletdescriptionsupplement, total_confidence_score, parent_poiid , parent_poiid_status from dqdictionaryhivedb.maindictempTableext
