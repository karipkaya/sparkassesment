from pyspark import SparkContext,SparkConf,SparkFiles
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

config = SparkConf().setAppName('Task1_1').set('spark.network.timeout','500000')\
        .setMaster('local')
sc = SparkContext(conf=config)

## creating spark session
spark = SparkSession(sc)

## adding file to spark
sc.addFile('../sf-airbnb-clean.parquet',recursive=True)
## using spark session to read local file
df = spark.read.parquet('file:///'+SparkFiles.get("sf-airbnb-clean.parquet"))


df_min_price = df.select(f.min(f.col('price')).alias('price'))
# debug outputs
#df_min_price.show()
df_max_rating = df.select(f.max(f.col('review_scores_rating')).alias('review_scores_rating'))
# debug outputs
#df_max_rating.show()
####
#df.join(df_min_price,on=['price']).\
#    join(df_max_rating,on=['review_scores_rating']).\
#    show()

df.join(df_min_price,on=['price']).\
    join(df_max_rating,on=['review_scores_rating']).\
    select(f.col('beds')).\
    repartition(1).\
    write.mode('overwrite').option('header', True).csv('../out/out_2_4.txt')

sc.stop()