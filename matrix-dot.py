


from pyspark import SparkConf, SparkContext
conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)


def firstmap (imp):
     if imp[0]=='a':
       return (str(imp[1])+'_'+str(imp[2]),imp[3]);
     if imp[0]=='b':
       return (str(imp[2])+'_'+str(imp[1]),imp[3])


def reduceMult(a,b):
    return a*b;


    

col=sc.parallelize([['a',0,1,1],['a',0,2,2],['b',1,0,3],['b',2,0,4]]).map(firstmap).reduceByKey(lambda a, b: a * b).map(lambda x: (x[0].split('_')[0],x[1])).reduceByKey(lambda a, b: a + b).collect()

print col;


