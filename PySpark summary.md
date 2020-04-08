# Chapter 2 
Based on [Spark: The DefinitiveGuide](https://github.com/databricks/Spark-The-Definitive-Guide) book.

temp

```python
s = "Python syntax highlighting"
print(s)
```

## reading csv file, infering schema and header
```python
flightData2015 = spark\
.read\
.option(" inferSchema" , " true" )\
.option(" header" , " true" )\
.csv(" /data/flight- data/csv/2015- summary. csv" )
```

```python
myRange = spark.range(1000).toDF("number")
```

# COMMAND ----------
```python
divisBy2 = myRange.where("number % 2 = 0")
```

# COMMAND ----------
```python
flightData2015 = spark\
  .read\
  .option("inferSchema", "true")\
  .option("header", "true")\
  .csv("/data/flight-data/csv/2015-summary.csv")
```
# COMMAND ----------
```python
flightData2015.createOrReplaceTempView("flight_data_2015")
```

# COMMAND ----------
```python
sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

dataFrameWay = flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .count()

sqlWay.explain()
dataFrameWay.explain()
```

# COMMAND ----------
```python
from pyspark.sql.functions import max

flightData2015.select(max("count")).take(1)
```

# COMMAND ----------
```python
maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show()
```

# COMMAND ----------
```python
from pyspark.sql.functions import desc

flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .show()
```

# COMMAND ----------
```python
flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .explain()
```
