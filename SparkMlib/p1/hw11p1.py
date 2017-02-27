# Solution for hw11 problem 1
# Author: S.K.
# Last Modified 22 April 2016
# Problem:
# Remove the header of the attached Samll_Car_Data.csv file and then 
# import it into Spark. Randomly select 10% of you data for testing and use remaining data 
# for training. Look initially at horsepower and displacement. Treat displacement as a 
# feature and horsepower as the target variable. Use MLlib linear regression to identify the model 
# for the relationship.


from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.mllib.evaluation import RegressionMetrics
import numpy as np

#open a file
f = open('out_p1.txt', 'w')


##################################################################
#### Some functions for evaluations of the results ###############
##################################################################

#print content of RDD for debugging purposes
def printRDD(x):
    print >> f,  str(x[1])

# Compute the square of the distance
def squared_error(actual, pred):
    return (pred - actual)**2

# Compute absolute error
def abs_error(actual, pred):
    return np.abs(pred - actual)

# Compute log of absolute error
def squared_log_error(pred, actual):
    return (np.log(pred + 1) - np.log(actual + 1))**2




###############  Start Spark ##################################
# get context, data and split the data
sc = SparkContext("local", "hw11p1")
path = "file:///home/cloudera/Documents/hw11/data/Small_Car_Data.csv"
raw_data = sc.textFile(path)

sqlContext = SQLContext(sc)
parts = raw_data.map(lambda l: l.split(","))

pre_df=parts.map(lambda p: Row(displacement = p[3],hspower = p[4]))

# create dataframe for cleaning the data later on
df=sqlContext.createDataFrame(pre_df)

# Count the number of rows before cleaning the data ( via filtering)
print >> f, "Before filtering count="
print >> f, df.count()

# cleaning the data
dff = df.where( (df.displacement != 'NaN')  &  ( df.hspower != 'NaN'))

# Count the number of rows after cleaning the data ( via filtering)
print >> f, "After filtering count="
print >> f,dff.count() 


# inspect the data
dff.show(300)


#leave this line
#df_lp=dff.map(lambda line: LabeledPoint(line[0], [line[1:]]))

# create a dataframe with labeledpoints, which are the input to spark regressionss
df_lp=dff.map(lambda line: LabeledPoint(line.hspower, [line.displacement]))

# inspect the data
print >> f, df_lp.take(4)

# split the data into training and testing parts
trainingData, testingData = df_lp.randomSplit([.9,.1],seed=1234)

#evaluate the regression
model = LinearRegressionWithSGD.train( trainingData  , iterations=2000, step=0.0001, initialWeights=[1.0], intercept=True  )



# print out the regression results
print >> f,("############################ MODEL ESTIMATION  RESULTS1 starts ##############################")
print >> f,(model)
print >>f,("############################ MODEL ESTIMATION  RESULTS1 ends   ##############################")


# compute different measures of predictions
true_vs_predicted = df_lp.map(lambda p: (p.label, model.predict(p.features)))

valuesAndPreds = df_lp.map(lambda p: (float(model.predict(p.features)), p.label))

true_vs_predicted_testing = testingData.map(lambda p: (p.label, model.predict(p.features)))



# compute additional metrics of regression quality
metrics = RegressionMetrics( valuesAndPreds  )

print >> f, "metrics.r2="
print >> f,  metrics.r2

mse = true_vs_predicted_testing.map(lambda (t, p): squared_error(t, p)).mean()
mae = true_vs_predicted_testing.map(lambda (t, p): abs_error(t, p)).mean()

print >> f,  "Linear Model - Mean Squared Error: %2.4f" % mse
print >> f,  "Linear Model - Mean Absolute Error: %2.4f" % mae


# save the results of regressions and predictions in the hdfs dfs
true_vs_predicted.map(lambda r: [r] ).saveAsTextFile("true_vs_predicted_p1")
true_vs_predicted_testing.map(lambda r: [r] ).saveAsTextFile("true_vs_predicted_testing_p1")
valuesAndPreds.map(lambda r: [r] ).saveAsTextFile("valuesAndPreds_p1")
# close the file for intermediate output
f.close()
