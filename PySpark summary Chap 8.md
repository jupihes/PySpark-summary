
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
