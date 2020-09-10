SET odsposoutletitem_input = '/npd/test/s_test1/snapshots/withoutdups';
use dqdictionaryhivedb;
drop table temptableext;
create external table dqdictionaryhivedb.uniquesnapshotstempTableext(
poi_id bigint,
updated string)row format delimited fields terminated by '|' lines terminated by '\n' location ${hiveconf:odsposoutletitem_input};


