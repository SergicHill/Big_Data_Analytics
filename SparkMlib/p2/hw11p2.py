# Solution for hw11 problem 2
# Author: S.K.
# Last Modified 22 April 2016
# Problem
# Treat: cylinders, displacement, manufacturer, model_year, origin and weight as
# features and use linear regression to predict two target variable: horsepower and acceleration. 
# Please note that some of those are categorical variables


from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.mllib.evaluation import RegressionMetrics
import numpy as np

f = open('out_p2.txt', 'w')

def printRDD(x):
    print >> f,  str(x[1])

def squared_error(actual, pred):
    return (pred - actual)**2


def abs_error(actual, pred):
    return np.abs(pred - actual)

def squared_log_error(pred, actual):
    return (np.log(pred + 1) - np.log(actual + 1))**2





sc = SparkContext("local", "hw11p1")
path = "file:///home/cloudera/Documents/hw11/data/Small_Car_Data.csv"
raw_data = sc.textFile(path)

sqlContext = SQLContext(sc)
parts = raw_data.map(lambda l: l.split(","))

pre_df=parts.map(lambda p: Row( accel = p[1], cyl=p[2],displacement = p[3],hspower = p[4], manuf= p[5], myear=p[7], origin=p[9], weight=p[10]))


df=sqlContext.createDataFrame(pre_df)

print >> f, "Before filtering count="
print >> f, df.count()


dff = df.where( (df.accel != 'NaN')  &   (df.displacement != 'NaN')  &  ( df.hspower != 'NaN') &  ( df.cyl != 'NaN') &  ( df.manuf != 'NaN') &  ( df.myear != 'NaN')   & (df.origin != 'NaN') & (df.weight != 'NaN')  )

# the number of rows in the dataset
K = dff.count()




def get_mapping(rdd, idx):
    return rdd.map(lambda fields: fields[idx]).distinct().zipWithIndex().collectAsMap()



print "Mapping of first categorical feature column-Manuf : %s" % get_mapping(dff, 4)
print "Mapping of first categorical feature column-ModelYear : %s" % get_mapping(dff, 5)
print '\n'
print "Mapping of first categorical feature column-Origin : %s" % get_mapping(dff, 6)

mappings = [get_mapping(dff, 4), get_mapping(dff, 5),get_mapping(dff, 6)  ]

cat_len = sum(map(len, mappings))
num_len = 3 
total_len = num_len + cat_len

print "Feature vector length for categorical features: %d" % cat_len
print "Feature vector length for numerical features: %d" % num_len
print "Total feature vector length: %d" % total_len

def extract_features(record, cat_len ):
    cat_vec = np.zeros(cat_len)
    i = 0
    step = 0
    for field in [record[4], record[5], record[6]  ] :
        m = mappings[i]
        idx = m[field]
        cat_vec[idx + step] = 1
        i = i + 1
        step = step + len(m)
    num_vec = np.array([float(field) for field in [record[1], record[2], record[7] ] ])
    return np.concatenate((cat_vec, num_vec))



def extract_label(record):
    return record[0]



df_lp  = dff.map(lambda r: LabeledPoint( extract_label(r)   ,extract_features(r,cat_len  )))


trainingData, testingData = df_lp.randomSplit([.9,.1],seed=1234)

print "trainingData.take(4)="
print trainingData.take(20)


model = LinearRegressionWithSGD.train( trainingData  , iterations=20, step=0.00000000001, initialWeights= [0.000005 for x in range(1, 41)], intercept=False )




print >> f,("############################ MODEL ESTIMATION  RESULTS1 starts ##############################")
print >> f,(model)
print >>f,("############################ MODEL ESTIMATION  RESULTS1 ends   ##############################")


true_vs_predicted = df_lp.map(lambda p: (p.label, model.predict(p.features)))

valuesAndPreds = df_lp.map(lambda p: (float(model.predict(p.features)), p.label))

true_vs_predicted_testing = testingData.map(lambda p: (p.label, model.predict(p.features)))


print >> f, ("--------------- Linear Model Prediction starts1----------------")
print >> f,  "Linear Model predictions : true_vs_predicted: " + str(true_vs_predicted.take(200))

print >> f,  "Linear Model predictions : valuesAndPreds: " + str(valuesAndPreds.take(200))

print >> f,  "Linear Model predictions : true_vs_predicted_testing: " + str(true_vs_predicted_testing.take(200))



metrics = RegressionMetrics( valuesAndPreds  )

print >> f, "metrics.r2="
print >> f,  metrics.r2

mse = true_vs_predicted_testing.map(lambda (t, p): squared_error(t, p)).mean()
mae = true_vs_predicted_testing.map(lambda (t, p): abs_error(t, p)).mean()

print >> f,  "Linear Model - Mean Squared Error: %2.4f" % mse
print >> f,  "Linear Model - Mean Absolute Error: %2.4f" % mae


f.close()
