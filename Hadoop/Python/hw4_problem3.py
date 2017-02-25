#Word count problem
import re
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("MyApp")
def lower_split( line ):
    line_lower=line.lower()
    line_split=re.split("\W", line_lower)
    return line_split
sc = SparkContext(conf = conf)
file = sc.textFile("ulysses/4300.txt")
#file=sc.textFile("ulysses_short/4300_l1000.txt")
#counts = file.flatMap(lambda line: line.lower().split()) \
counts = file.flatMap(lambda line: lower_split(line)) \
.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a +b)
counts.saveAsTextFile("pycounts_2251f")
sc.stop()
