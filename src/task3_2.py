from pyspark import SparkContext,SparkConf,SparkFiles
from pyspark.sql import SparkSession,Window
import pyspark.sql.functions as f
from pyspark.sql.types import DoubleType
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler

# creating spark config and context
config = SparkConf().setAppName('Task1_1').set('spark.network.timeout','500000')\
        .setMaster('local')
sc = SparkContext(conf=config)

## creating spark session
spark = SparkSession(sc)

## adding file to spark
sc.addFile('https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data')
## using spark session to read local file
df = spark.read.csv('file:///'+SparkFiles.get("iris.data"), header=False,inferSchema=True)
df = df.toDF("sepal_length", "sepal_width", "petal_length", "petal_width", "iris_class")
# caching data
df.cache()

## df_class_lookup is createad a class lookup table

window = Window.orderBy(f.lit(1))

df_class_lookup = df.select(f.col('iris_class')).\
    distinct().select(f.col('iris_class')).\
    withColumn('iris_class_code',f.row_number().over(window))

## class names converted to numerical values by using df_class_lookup
df = df.join(df_class_lookup,on = ['iris_class'])

## converting training data into features and labels ['features', 'iris_class_code']
vectorAssembler = VectorAssembler(inputCols = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], outputCol = 'features')
train_df = vectorAssembler.transform(df)
train_df = train_df.select(['features', 'iris_class_code'])
train_df.show(10)

## running training using training data provided
lr = LogisticRegression(featuresCol = 'features', labelCol='iris_class_code',maxIter=10)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficientMatrix))


## test data preparation from test_data.csv
sc.addFile('test_data.csv')
pred_data = spark.read.csv('file:///'+SparkFiles.get("test_data.csv"), header=False,inferSchema=True)
pred_data = pred_data.toDF("sepal_length", "sepal_width", "petal_length", "petal_width")
pred_data.cache()


vectorAssembler = VectorAssembler(inputCols = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], outputCol = 'features')
pred_data_df = vectorAssembler.transform(pred_data)
pred_data_df = pred_data_df.select(['features']).withColumn('iris_class_code',f.lit(0))
print(pred_data_df.count())
pred_data_df.show(10)

## prediction data
predict_test = lr_model.transform(pred_data_df)

predict_test.select("iris_class_code","prediction").show(10)
## joining with lookup table to check class names
result_control = predict_test.alias('a').join(df_class_lookup.alias('b'),
                                   (f.col("a.prediction") == f.col("b.iris_class_code"))
                                   , how='left')

result_control.show()
result_control.select(f.col('iris_class').alias('class'))\
    .repartition(1).write.option('header', True).\
    mode('overwrite').csv('../out/out_3_2.txt')

"""
Output will be like below.
+-----------------+---------------+--------------------+--------------------+----------+--------------+---------------+
|         features|iris_class_code|       rawPrediction|         probability|prediction|    iris_class|iris_class_code|
+-----------------+---------------+--------------------+--------------------+----------+--------------+---------------+
|[5.1,3.5,1.4,0.2]|              0|[-9.9770581535204...|[2.33589594632390...|       2.0|   Iris-setosa|              2|
|[6.2,3.4,5.4,2.3]|              0|[-11.120311739685...|[1.43838440631216...|       1.0|Iris-virginica|              1|
+-----------------+---------------+--------------------+--------------------+----------+--------------+---------------+
"""

sc.stop()
