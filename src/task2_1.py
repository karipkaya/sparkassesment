from pyspark import SparkContext,SparkConf,SparkFiles
from pyspark.sql import SparkSession

config = SparkConf().setAppName('Task1_1').set('spark.network.timeout','500000')\
        .setMaster('local')
sc = SparkContext(conf=config)

## creating spark session
spark = SparkSession(sc)

## adding file to spark
sc.addFile('../sf-airbnb-clean.parquet',recursive=True)
## using spark session to read local file
df = spark.read.parquet('file:///'+SparkFiles.get("sf-airbnb-clean.parquet"))

df.show(10)

sc.stop()