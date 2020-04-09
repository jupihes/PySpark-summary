
code/Structured_APIs-Chapter_10_Spark_SQL.py
```
## Proper explanation
```python

spark.read.json("/data/flight-data/json/2015-summary.json")\
  .createOrReplaceTempView("some_sql_view") # DF => SQL

spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count)
FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
""")\
  .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")\
  .count() # SQL => DF


```
## Proper explanation
```python

code/Advanced_Analytics_and_Machine_Learning-Chapter_24_Advanced_Analytics_and_Machine_Learning.py
```
## Proper explanation
```python

from pyspark.ml.linalg import Vectors
denseVec = Vectors.dense(1.0, 2.0, 3.0)
size = 3
idx = [1, 2] # locations of non-zero elements in vector
values = [2.0, 3.0]
sparseVec = Vectors.sparse(size, idx, values)


```
## Proper explanation
```python

df = spark.read.json("/data/simple-ml")
df.orderBy("value2").show()


```
## Proper explanation
```python

from pyspark.ml.feature import RFormula
supervised = RFormula(formula="lab ~ . + color:value1 + color:value2")


```
## Proper explanation
```python

fittedRF = supervised.fit(df)
preparedDF = fittedRF.transform(df)
preparedDF.show()


```
## Proper explanation
```python

train, test = preparedDF.randomSplit([0.7, 0.3])


```
## Proper explanation
```python

from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(labelCol="label",featuresCol="features")


```
## Proper explanation
```python

print lr.explainParams()


```
## Proper explanation
```python

fittedLR = lr.fit(train)


```
## Proper explanation
```python

train, test = df.randomSplit([0.7, 0.3])


```
## Proper explanation
```python

rForm = RFormula()
lr = LogisticRegression().setLabelCol("label").setFeaturesCol("features")


```
## Proper explanation
```python

from pyspark.ml import Pipeline
stages = [rForm, lr]
pipeline = Pipeline().setStages(stages)


```
## Proper explanation
```python

from pyspark.ml.tuning import ParamGridBuilder
params = ParamGridBuilder()\
  .addGrid(rForm.formula, [
    "lab ~ . + color:value1",
    "lab ~ . + color:value1 + color:value2"])\
  .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])\
  .addGrid(lr.regParam, [0.1, 2.0])\
  .build()


```
## Proper explanation
```python

from pyspark.ml.evaluation import BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator()\
  .setMetricName("areaUnderROC")\
  .setRawPredictionCol("prediction")\
  .setLabelCol("label")


```
## Proper explanation
```python

from pyspark.ml.tuning import TrainValidationSplit
tvs = TrainValidationSplit()\
  .setTrainRatio(0.75)\
  .setEstimatorParamMaps(params)\
  .setEstimator(pipeline)\
  .setEvaluator(evaluator)


```
## Proper explanation
```python

tvsFitted = tvs.fit(train)


# COMMAND ----------
