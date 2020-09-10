SET mapreduce.map.memory.mb=8000
SET mapreduce.map.java.opts=-Xmx6000m
SET mapreduce.reduce.memory.mb=8000
SET mapreduce.reduce.java.opts=-Xmx6200m
SET hive.tez.container.size=10240
SET hive.tez.java.opts=-Xmx8192m
SET hive.exec.dynamic.partition.mode=nonstrict
SET hive.exec.max.dynamic.partitions=100000
SET hive.exec.max.dynamic.partitions.pernode=100000
SET hive.warehouse.subdir.inherit.perms=False
select poi_id, cast(max(distinct updated) as string) as updated from dqdictionaryhivedb.maintempTableext group by poi_id 
