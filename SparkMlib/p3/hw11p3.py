from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.mllib.evaluation import RegressionMetrics
import numpy as np


path = "file:///home/cloudera/Documents/hw11/data/Small_Car_Data.csv"
sc = SparkContext("local", "hw11p3")

# Load and parse the data file into an RDD of LabeledPoint.

data = sc.textFile(path)
sqlContext = SQLContext(sc)

parts = data.map(lambda l: l.split(","))
pre_df=parts.map(lambda p: Row(displacement = p[3],hspower = p[4]))

df=sqlContext.createDataFrame(pre_df)


dff = df.where( (df.displacement != 'NaN')  &  ( df.hspower != 'NaN'))

df_lp=dff.map(lambda line: LabeledPoint(line.hspower, [line.displacement]))


# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = df_lp.randomSplit([0.7, 0.3])

# Train a DecisionTree model.
#  Empty categoricalFeaturesInfo indicates all features are continuous.
model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo={}, impurity='variance', maxDepth=5, maxBins=32)

# Evaluate model on test instances and compute test error
predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum()/float(testData.count())
print('Test Mean Squared Error = ' + str(testMSE))
print('Learned regression tree model:')
print(model.toDebugString())















