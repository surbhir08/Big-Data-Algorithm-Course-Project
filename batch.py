import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

'''
spark = SparkSession.builder.\
        master('local').\
        appName('foo').\
        getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
'''

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

path = "/data_pipeline/Spark/2019-Oct.csv"

df = spark.read.option("header", True).csv(path)
df2 = df.withColumn('price', df.price.cast('float'))
#df2.show()

df3 = df2[df2['event_type'] == 'purchase']
#df3.show()

purchases = df2.groupby(['event_type']).sum('price')
purchases.show()


brand_sales_df = df3.groupby('brand').sum('price')
brand_sales_df.orderBy(desc('sum(price)')).show()




#brand_sales_df.sort('sum(price)').desc().show()
brand_item_df = df3.groupby('brand').count()
brand_item_df.orderBy(desc('count')).show()
#brand_item_df.sort('count').desc().show()

'''
???????????????
import pyspark.sql.functions as f
cum_sum = df_basket1.withColumn('cumsum', f.sum(df_basket1.Price).over(Window.partitionBy().orderBy().rowsBetween(-sys.maxsize, 0)))
cum_sum.show()
'''
