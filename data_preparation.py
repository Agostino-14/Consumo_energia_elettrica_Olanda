#Run: spark-submit data_preparation.py
#Run Yarn: spark-submit --master yarn data_preparation.py
#Run Yarn: spark-submit --master yarn --deploy-mode cluster data_preparation.py
#Inizializzazione Spark

import pyspark.sql.functions as func

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
conf = SparkConf().setAppName('Electricity_Data_Preparation')
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

############################################

#Importiamo la tabella raw
sqlContext.sql('use progetto_olanda_elettr')
raw_df = sqlContext.table('electric_consumption_raw')

#Convertiamo le colonne numeriche in Float o Int in modo da avere null in valori non numerici
raw_df = raw_df.withColumn('num_connections', raw_df['num_connections'].cast('int'))
raw_df = raw_df.withColumn('deliviery_perc', raw_df['deliviery_perc'].cast('float'))
raw_df = raw_df.withColumn('perc_of_active_connections', raw_df['perc_of_active_connections'].cast('float'))
raw_df = raw_df.withColumn('type_conn_perc', raw_df['type_conn_perc'].cast('float'))
raw_df = raw_df.withColumn('annual_consume_lowtarif_perc', raw_df['annual_consume_lowtarif_perc'].cast('float'))
raw_df = raw_df.withColumn('annual_consume', raw_df['annual_consume'].cast('float'))
raw_df = raw_df.withColumn('smartmeter_perc', raw_df['smartmeter_perc'].cast('float'))
raw_df = raw_df.withColumn('year', raw_df['year'].cast('int'))

#Escludiamo i record con percentuale di connessioni attive null o zero
raw_df = raw_df.filter('perc_of_active_connections IS NOT NULL AND perc_of_active_connections!=0')

#Escludiamo i record con consumo annuale null o zero
raw_df = raw_df.filter('annual_consume IS NOT NULL AND annual_consume!=0')

#Contiamo i record nel dataframe e lo rendiamo persistente
raw_df.cache()
raw_df.count()
#count = 3982657

#Calcoliamo le colonne con valori numerici al posto di quelli percentuali
raw_df = raw_df.withColumn('delivered', func.round(((raw_df['annual_consume']/100)*raw_df['deliviery_perc']),2).cast('float'))
raw_df = raw_df.withColumn('annual_consume_lowtarif', func.round(((raw_df['annual_consume']/100)*raw_df['annual_consume_lowtarif_perc']),2).cast('float'))
raw_df = raw_df.withColumn('smartmeter', func.round(((raw_df['num_connections']/100)*raw_df['smartmeter_perc']),0).cast('int'))
raw_df = raw_df.withColumn('active_connections', func.round(((raw_df['num_connections']/100)*raw_df['perc_of_active_connections']),0).cast('int'))

#Eliminiamo le colonne non piu' necessarie
drop_list = ['deliviery_perc', 'perc_of_active_connections', 'annual_consume_lowtarif_perc', 'type_conn_perc', 'smartmeter_perc', 'net_manager', 'purchase_area', 'street', 'zipcode_from', 'zipcode_to', 'type_of_connection']
raw_df = raw_df.select([column for column in raw_df.columns if column not in drop_list])

#Puliama la colonna city
raw_df = raw_df.withColumn('city', func.upper(func.regexp_replace(func.trim(raw_df['city']), "[^A-Z0-9_]", "")))

#Aggreghiamo per City e Year, applicando la somma sulle colonne numeriche
group_df = raw_df.groupBy('city','year').sum()

#Contiamo i record
#group_df.count()
#count = 27445

#Rinominiamo le colonne e applichiamo la round sulle colonne float
group_df = group_df.drop('sum(year)').withColumnRenamed('sum(num_connections)','num_connections').withColumnRenamed('sum(smartmeter)','smartmeter').withColumnRenamed('sum(active_connections)','active_connections')
group_df = group_df.withColumn('sum(annual_consume)', func.round(group_df['sum(annual_consume)'],2).cast('float')).withColumnRenamed('sum(annual_consume)','annual_consume')
group_df = group_df.withColumn('sum(annual_consume_lowtarif)', func.round(group_df['sum(annual_consume_lowtarif)'],2).cast('float')).withColumnRenamed('sum(annual_consume_lowtarif)','annual_consume_lowtarif')
group_df = group_df.withColumn('sum(delivered)', func.round(group_df['sum(delivered)'],2).cast('float')).withColumnRenamed('sum(delivered)','delivered')

#Per facilitare le successive analisi, ricalcoliamo le colonne percentuali
group_df = group_df.withColumn('delivered_perc', func.round(((group_df['delivered']*100)/group_df['annual_consume']),2).cast('float'))
group_df = group_df.withColumn('annual_consume_lowtarif_perc', func.round(((group_df['annual_consume_lowtarif']*100)/group_df['annual_consume']),2).cast('float'))
group_df = group_df.withColumn('smartmeter_perc', func.round(((group_df['smartmeter']*100)/group_df['num_connections']),2).cast('float'))
group_df = group_df.withColumn('active_connections_perc', func.round(((group_df['active_connections']*100)/group_df['num_connections']),2).cast('float'))

#Cache del nuovo dataframe
raw_df.unpersist(blocking=True)
group_df.cache()
group_df.count()

#Leggo la tabella contenente le info sulle citta' e modifico i campi city e admin-name in uppercase
cities = sqlContext.table('netherland_cities')
cities = cities.withColumn('city', func.upper(func.regexp_replace(func.trim(raw_df['city']), "[^A-Z0-9_]", ""))).withColumn('admin_name', func.upper(cities['admin_name'])).withColumnRenamed('admin_name','region').withColumnRenamed('city','city_key')
cities = cities.select('city_key', 'region', 'lat', 'lng')

#Join tra i due dataframe per citta'
left_join = group_df.join(cities, group_df['city'] == cities['city_key'], how='left')
left_join.drop('city_key')

#Scriviamo i risultati
left_join.coalesce(1).write.save('/user/cloudera/target/electricity_cleaned/', format='json', mode='overwrite')
