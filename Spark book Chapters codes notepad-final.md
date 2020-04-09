## Proper explanation
```python

code/Structured_APIs-Chapter_8_Joins.py
```
## Proper explanation
```python

person = spark.createDataFrame([
    (0, "Bill Chambers", 0, [100]),
    (1, "Matei Zaharia", 1, [500, 250, 100]),
    (2, "Michael Armbrust", 1, [250, 100])])\
  .toDF("id", "name", "graduate_program", "spark_status")
graduateProgram = spark.createDataFrame([
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley")])\
  .toDF("id", "degree", "department", "school")
sparkStatus = spark.createDataFrame([
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor")])\
  .toDF("id", "status")


```
## Proper explanation
```python

joinExpression = person["graduate_program"] == graduateProgram['id']


```
## Proper explanation
```python

wrongJoinExpression = person["name"] == graduateProgram["school"]


```
## Proper explanation
```python

joinType = "inner"


```
## Proper explanation
```python

gradProgram2 = graduateProgram.union(spark.createDataFrame([
    (0, "Masters", "Duplicated Row", "Duplicated School")]))

gradProgram2.createOrReplaceTempView("gradProgram2")


```
## Proper explanation
```python

from pyspark.sql.functions import expr

person.withColumnRenamed("id", "personId")\
  .join(sparkStatus, expr("array_contains(spark_status, id)")).show()


```
## Proper explanation
```python

code/Structured_APIs-Chapter_9_Data_Sources.py
```
## Proper explanation
```python

csvFile = spark.read.format("csv")\
  .option("header", "true")\
  .option("mode", "FAILFAST")\
  .option("inferSchema", "true")\
  .load("/data/flight-data/csv/2010-summary.csv")


```
## Proper explanation
```python

csvFile.write.format("csv").mode("overwrite").option("sep", "\t")\
  .save("/tmp/my-tsv-file.tsv")


```
## Proper explanation
```python

spark.read.format("json").option("mode", "FAILFAST")\
  .option("inferSchema", "true")\
  .load("/data/flight-data/json/2010-summary.json").show(5)


```
## Proper explanation
```python

csvFile.write.format("json").mode("overwrite").save("/tmp/my-json-file.json")


```
## Proper explanation
```python

spark.read.format("parquet")\
  .load("/data/flight-data/parquet/2010-summary.parquet").show(5)


```
## Proper explanation
```python

csvFile.write.format("parquet").mode("overwrite")\
  .save("/tmp/my-parquet-file.parquet")


```
## Proper explanation
```python

spark.read.format("orc").load("/data/flight-data/orc/2010-summary.orc").show(5)


```
## Proper explanation
```python

csvFile.write.format("orc").mode("overwrite").save("/tmp/my-json-file.orc")


```
## Proper explanation
```python

driver = "org.sqlite.JDBC"
path = "/data/flight-data/jdbc/my-sqlite.db"
url = "jdbc:sqlite:" + path
tablename = "flight_info"


```
## Proper explanation
```python

dbDataFrame = spark.read.format("jdbc").option("url", url)\
  .option("dbtable", tablename).option("driver",  driver).load()


```
## Proper explanation
```python

pgDF = spark.read.format("jdbc")\
  .option("driver", "org.postgresql.Driver")\
  .option("url", "jdbc:postgresql://database_server")\
  .option("dbtable", "schema.tablename")\
  .option("user", "username").option("password", "my-secret-password").load()


```
## Proper explanation
```python

dbDataFrame.filter("DEST_COUNTRY_NAME in ('Anguilla', 'Sweden')").explain()


```
## Proper explanation
```python

pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
  AS flight_info"""
dbDataFrame = spark.read.format("jdbc")\
  .option("url", url).option("dbtable", pushdownQuery).option("driver",  driver)\
  .load()


```
## Proper explanation
```python

dbDataFrame = spark.read.format("jdbc")\
  .option("url", url).option("dbtable", tablename).option("driver",  driver)\
  .option("numPartitions", 10).load()


```
## Proper explanation
```python

props = {"driver":"org.sqlite.JDBC"}
predicates = [
  "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
  "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'"]
spark.read.jdbc(url, tablename, predicates=predicates, properties=props).show()
spark.read.jdbc(url,tablename,predicates=predicates,properties=props)\
  .rdd.getNumPartitions() # 2


```
## Proper explanation
```python

props = {"driver":"org.sqlite.JDBC"}
predicates = [
  "DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'",
  "DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'"]
spark.read.jdbc(url, tablename, predicates=predicates, properties=props).count()


```
## Proper explanation
```python

colName = "count"
lowerBound = 0L
upperBound = 348113L # this is the max count in our database
numPartitions = 10


```
## Proper explanation
```python

spark.read.jdbc(url, tablename, column=colName, properties=props,
                lowerBound=lowerBound, upperBound=upperBound,
                numPartitions=numPartitions).count() # 255


```
## Proper explanation
```python

newPath = "jdbc:sqlite://tmp/my-sqlite.db"
csvFile.write.jdbc(newPath, tablename, mode="overwrite", properties=props)


```
## Proper explanation
```python

spark.read.jdbc(newPath, tablename, properties=props).count() # 255


```
## Proper explanation
```python

csvFile.write.jdbc(newPath, tablename, mode="append", properties=props)


```
## Proper explanation
```python

spark.read.jdbc(newPath, tablename, properties=props).count() # 765


```
## Proper explanation
```python


csvFile.limit(10).select("DEST_COUNTRY_NAME", "count")\
  .write.partitionBy("count").text("/tmp/five-csv-files2py.csv")


```
## Proper explanation
```python

csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME")\
  .save("/tmp/partitioned-files.parquet")


```
## Proper explanation
```python

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
