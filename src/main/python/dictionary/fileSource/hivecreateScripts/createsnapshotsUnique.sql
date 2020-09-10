SET odsposoutletitem_input = '/npd/s_test2/snapshots/withoutdups/'
drop table if exists dqdictionaryhivedb.uniquesnapshotstempTableext
create external table dqdictionaryhivedb.uniquesnapshotstempTableext(poi_id bigint,updated string) row format delimited fields terminated by '|' lines terminated by '\n' location ${hiveconf:odsposoutletitem_input}
