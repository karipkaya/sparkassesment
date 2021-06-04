from pyspark import SparkContext,SparkConf,SparkFiles

# create spark conf
conf = SparkConf().setAppName('Task1_1').set('spark.network.timeout','500000')\
        .setMaster('local')
# create spark context
sc = SparkContext(conf=conf)
## adding remote file to spark (it will still be a local file not hdfs)
sc.addFile('https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv')


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


rdd.flatMap(lambda x: x).\
    groupBy(lambda x: x).\
    map(lambda x: (x[0],len(x[1]))).\
    repartition(1).\
    saveAsTextFile('../out/out_1_3.txt')

sc.stop()