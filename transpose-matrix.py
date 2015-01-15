'''
Created on 16/12/2014

@author: lpeinado
'''
from pyspark import SparkConf, SparkContext
conf = (SparkConf()
         .setMaster("local")
         .setAppName("transpose")
         .set("spark.executor.memory", "1g"))


def firstmap (data):
     row= data.keys()[0];
     value=data[row];    
     for i in range(len(value)):
        yield (i+1,{row:value[i]});
    
def red(dataA,dataB):
    keys= dataB.keys(); 
    dataA[keys[0]]=dataB[keys[0]] ;   
    return dataA

def transpose(sc,matrix):
       return sc.parallelize(matrix).flatMap(firstmap).map(lambda word: word) .reduceByKey(red);

def main():
    matrix=[{1:[1,2,3,4]},{2:[4,5,6,7]},{3:[7,8,9,10]} ];
    matrix=[{1:[1,2,3,4]},{2:[1,2,3,4]},{3:[1,2,3,4]} ];
    sc = SparkContext(conf = conf)
    matrixTRDD=transpose(sc,matrix);
    col=matrixTRDD.collect()
    print col;


if __name__ == '__main__':
    main();