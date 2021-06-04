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

## calculate average bathrooms and bedrooms
"""
it is equalent to 
select 
sum(case when price>5000 and review_scores_rating = 10 then bathrooms else 0 end)/count(*) as avg_bathrooms ,
sum(case when price>5000 and review_scores_rating = 10 then bedrooms else 0 end)/count(*) as avg_bedrooms
from sf-airbnb-clean 
"""

"""
df.where((f.col('price') > 5000) &
           ((f.col('review_scores_rating')== 10) |
            (f.col('review_scores_accuracy')== 10) |
            (f.col('review_scores_cleanliness')== 10) |
            (f.col('review_scores_checkin')== 10 )|
            (f.col('review_scores_communication')== 10) |
            (f.col('review_scores_location')== 10) |
            (f.col('review_scores_value')== 10))).\
    select((f.sum(f.col('bathrooms'))/f.count('*')).alias('avg_bathrooms'),
          (f.sum(f.col('bedrooms')) / f.count('*')).alias('avg_bedrooms')).\
    show()
"""

df.where((f.col('price') > 5000) &
           ((f.col('review_scores_rating')== 10) |
            (f.col('review_scores_accuracy')== 10) |
            (f.col('review_scores_cleanliness')== 10) |
            (f.col('review_scores_checkin')== 10 )|
            (f.col('review_scores_communication')== 10) |
            (f.col('review_scores_location')== 10) |
            (f.col('review_scores_value')== 10))).\
    select((f.sum(f.col('bathrooms'))/f.count('*')).alias('avg_bathrooms'),
          (f.sum(f.col('bedrooms')) / f.count('*')).alias('avg_bedrooms')).\
    repartition(1).\
    write.mode('overwrite').option('header', True).csv('../out/out_2_3.txt')

sc.stop()