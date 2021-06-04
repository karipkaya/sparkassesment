from pyspark import SparkContext,SparkConf,SparkFiles

# create spark conf
conf = SparkConf().setAppName('Task1_1').set('spark.network.timeout','500000')\
        .setMaster('local')
# create spark context
sc = SparkContext(conf=conf)
## adding remote file to spark (it will still be a local file not hdfs)
sc.addFile('https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv')
## using spark session to read local file
#path = SparkFiles.getRootDirectory()
#print('File Path : '+path)

# reading local file into an array
def readLocalFile():
    fileVal=[]
    with open(SparkFiles.get('groceries.csv'),'r') as testFile:
        for line in testFile.readlines() :
            fileVal.append(line.strip('\n').split(','))
    return fileVal

arr = readLocalFile()

#print(arr[0:2])
# local file to Spark rdd
rdd = sc.parallelize(arr)

# writing distinct products to a hadoop file
rdd.flatMap(lambda x: x).distinct().repartition(1).saveAsTextFile('../out/out_1_2a.txt')

sc.stop()