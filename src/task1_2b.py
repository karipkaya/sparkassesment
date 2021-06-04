from pyspark import SparkContext,SparkConf,SparkFiles
#from pyspark.sql import SparkSession

# create spark conf
conf = SparkConf().setAppName('Task1_1').set('spark.network.timeout','500000')\
        .setMaster('local')
# create spark context
sc = SparkContext(conf=conf)
#spark = SparkSession(sc)

#read file
rdd = sc.textFile('../out/out_1_2a.txt')
#rdd = spark.read.csv('./out/out_1_2a.txt')

print('Count:')
print(rdd.count())

sc.stop()