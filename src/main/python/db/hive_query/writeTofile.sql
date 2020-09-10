insert overwrite directory '/npd/processed/sabbirDir/manufacturedCode' select printf( "%d\t%d\t%d\t%d", manufacturercode , poi_id, business_id, itemid) from karun_test.ods_posoutletitems_k_test;
