import pyspark.sql.functions as func

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
conf = SparkConf().setAppName('Electricity_Data_Exploration')
sc = SparkContext(conf=conf)
hive_context = HiveContext(sc)

table = hive_context.table("progetto_olanda_elettr.electricity_data_pre_transformed")
hive_context.registerDataFrameAsTable(table, "electricity_data_pre_transformed")
df1 = hive_context.sql("select year, AVG(smartmeter_perc) as avg_smartmeter_perc from electricity_data_pre_transformed group by year order by year")
df1.coalesce(1).write.save('/user/cloudera/target/exploration_out1', format='json', mode='overwrite')


df2 = hive_context.sql("select annual_consume, annual_consume_lowtarif, num_active_connections from electricity_data_pre_transformed where year >= 2018")
df2.coalesce(1).write.save('/user/cloudera/target/exploration_out2', format='json', mode='overwrite')

df3 = hive_context.sql("select deliviery_perc from electricity_data_pre_transformed where deliviery_perc<95")
df3.coalesce(1).write.save('/user/cloudera/target/exploration_out3', format='json', mode='overwrite')