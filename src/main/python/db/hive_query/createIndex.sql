use karun_test;
set hive.execution.engine= mr;
set hive.index.compact.binary.search=true;
create index indexOnPosoutletitems on table odsposoutletitemswithpartition(itemid) as 'compact' with deferred rebuild;
show formatted index on odsposoutletitemswithpartition;
describe extended odsposoutletitemswithpartition;
show formatted index on odsitems;
show formatted index on odsposoutletitems;
show partitions odsposoutletitemswithpartition;
describe extended odsposoutletitemswithpartition;

