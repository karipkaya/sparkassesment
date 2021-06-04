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

## write min price, max price and count to out/out_2_2.txt

df.select(f.min(f.col('price')).alias('min_price'),
          f.max(f.col('price')).alias('max_price'),
          f.count('*').alias('row_count')).\
        write.mode('overwrite').option('header',True).csv('../out/out_2_2.txt')

# test prints
#print(df.count())
#print(df.orderBy(f.col('price')).head(1))
#print(df.orderBy(f.col('price')).tail(1))

sc.stop()