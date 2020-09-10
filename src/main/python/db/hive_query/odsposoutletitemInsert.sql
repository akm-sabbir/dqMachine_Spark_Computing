
insert overwrite table karun_test.odsposoutletitems
	select poi_id,business_id, posoutlet , outletdivision, outletdepartment , outletclass , outletbrand , outletitemnumber, outletdescription , outletbrandmatch, outletdescriptionmatch, manufacturercode, sku, itemid, itemtype, price, manufacturercodestatus, loadid, status, added, updated, matched_country_code, previous_poiid, parent_poiid, parent_poiid_status from karun_test.odsposoutletitemsDecompressed
