book name : Spark The Definitive Guide
Github link for book codes:
https://github.com/databricks/Spark-The-Definitive-Guide

Chapter2: 	82 	lines
Chapter3: 	152 lines
Chapter4: 	17 	lines
Chapter5: 	273	lines
Chapter6: 	396	lines
Chapter7: 	181	lines
Chapter8: 	50 	lines
Chapter9: 	162	lines
Chapter10:	13	lines
Chapter24: 	104 lines


code/A_Gentle_Introduction_to_Spark-Chapter_2_A_Gentle_Introduction_to_Spark.py
```
# COMMAND ----------
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
# COMMAND ----------
```python

code/A_Gentle_Introduction_to_Spark-Chapter_3_A_Tour_of_Sparks_Toolset.py
```
# COMMAND ----------
```python

staticDataFrame = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/data/retail-data/by-day/*.csv")

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema

```
# COMMAND ----------
```python

from pyspark.sql.functions import window, column, desc, col
staticDataFrame\
  .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")\
  .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
  .sum("total_cost")\
  .sort(desc("sum(total_cost)"))\
  .show(5)


```
# COMMAND ----------
```python

streamingDataFrame = spark.readStream\
    .schema(staticSchema)\
    .option("maxFilesPerTrigger", 1)\
    .format("csv")\
    .option("header", "true")\
    .load("/data/retail-data/by-day/*.csv")


```
# COMMAND ----------
```python

purchaseByCustomerPerHour = streamingDataFrame\
  .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")\
  .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
  .sum("total_cost")


```
# COMMAND ----------
```python

purchaseByCustomerPerHour.writeStream\
    .format("memory")\
    .queryName("customer_purchases")\
    .outputMode("complete")\
    .start()


```
# COMMAND ----------
```python

spark.sql("""
  SELECT *
  FROM customer_purchases
  ORDER BY `sum(total_cost)` DESC
  """)\
  .show(5)


```
# COMMAND ----------
```python

from pyspark.sql.functions import date_format, col
preppedDataFrame = staticDataFrame\
  .na.fill(0)\
  .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))\
  .coalesce(5)


```
# COMMAND ----------
```python

trainDataFrame = preppedDataFrame\
  .where("InvoiceDate < '2011-07-01'")
testDataFrame = preppedDataFrame\
  .where("InvoiceDate >= '2011-07-01'")


```
# COMMAND ----------
```python

from pyspark.ml.feature import StringIndexer
indexer = StringIndexer()\
  .setInputCol("day_of_week")\
  .setOutputCol("day_of_week_index")


```
# COMMAND ----------
```python

from pyspark.ml.feature import OneHotEncoder
encoder = OneHotEncoder()\
  .setInputCol("day_of_week_index")\
  .setOutputCol("day_of_week_encoded")


```
# COMMAND ----------
```python

from pyspark.ml.feature import VectorAssembler

vectorAssembler = VectorAssembler()\
  .setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"])\
  .setOutputCol("features")


```
# COMMAND ----------
```python

from pyspark.ml import Pipeline

transformationPipeline = Pipeline()\
  .setStages([indexer, encoder, vectorAssembler])


```
# COMMAND ----------
```python

fittedPipeline = transformationPipeline.fit(trainDataFrame)


```
# COMMAND ----------
```python

transformedTraining = fittedPipeline.transform(trainDataFrame)


```
# COMMAND ----------
```python

from pyspark.ml.clustering import KMeans
kmeans = KMeans()\
  .setK(20)\
  .setSeed(1L)


```
# COMMAND ----------
```python

kmModel = kmeans.fit(transformedTraining)


```
# COMMAND ----------
```python

transformedTest = fittedPipeline.transform(testDataFrame)


```
# COMMAND ----------
```python

from pyspark.sql import Row

spark.sparkContext.parallelize([Row(1), Row(2), Row(3)]).toDF()


```
# COMMAND ----------
```python

code/Structured_APIs-Chapter_4_Structured_API_Overview.py
```
# COMMAND ----------
```python

df = spark.range(500).toDF("number")
df.select(df["number"] + 10)


```
# COMMAND ----------
```python

spark.range(2).collect()


```
# COMMAND ----------
```python

from pyspark.sql.types import *
b = ByteType()


```
# COMMAND ----------
```python

code/Structured_APIs-Chapter_5_Basic_Structured_Operations.py
```
# COMMAND ----------
```python

df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")


```
# COMMAND ----------
```python

spark.read.format("json").load("/data/flight-data/json/2015-summary.json").schema


```
# COMMAND ----------
```python

from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False, metadata={"hello":"world"})
])
df = spark.read.format("json").schema(myManualSchema)\
  .load("/data/flight-data/json/2015-summary.json")


```
# COMMAND ----------
```python

from pyspark.sql.functions import col, column
col("someColumnName")
column("someColumnName")


```
# COMMAND ----------
```python

from pyspark.sql.functions import expr
expr("(((someCol + 5) * 200) - 6) < otherCol")


```
# COMMAND ----------
```python

from pyspark.sql import Row
myRow = Row("Hello", None, 1, False)


```
# COMMAND ----------
```python

myRow[0]
myRow[2]


```
# COMMAND ----------
```python

df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")
df.createOrReplaceTempView("dfTable")


```
# COMMAND ----------
```python

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
  StructField("some", StringType(), True),
  StructField("col", StringType(), True),
  StructField("names", LongType(), False)
])
myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()


```
# COMMAND ----------
```python

df.select("DEST_COUNTRY_NAME").show(2)


```
# COMMAND ----------
```python

df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)


```
# COMMAND ----------
```python

from pyspark.sql.functions import expr, col, column
df.select(
    expr("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"))\
  .show(2)


```
# COMMAND ----------
```python

df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)


```
# COMMAND ----------
```python

df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))\
  .show(2)


```
# COMMAND ----------
```python

df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)


```
# COMMAND ----------
```python

df.selectExpr(
  "*", # all original columns
  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
  .show(2)


```
# COMMAND ----------
```python

df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)


```
# COMMAND ----------
```python

from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("One")).show(2)


```
# COMMAND ----------
```python

df.withColumn("numberOne", lit(1)).show(2)


```
# COMMAND ----------
```python

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))\
  .show(2)


```
# COMMAND ----------
```python

df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns


```
# COMMAND ----------
```python

dfWithLongColName = df.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME"))


```
# COMMAND ----------
```python

dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")\
  .show(2)


```
# COMMAND ----------
```python

dfWithLongColName.select(expr("`This Long Column-Name`")).columns


```
# COMMAND ----------
```python

df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia")\
  .show(2)


```
# COMMAND ----------
```python

df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()


```
# COMMAND ----------
```python

df.select("ORIGIN_COUNTRY_NAME").distinct().count()


```
# COMMAND ----------
```python

seed = 5
withReplacement = False
fraction = 0.5
df.sample(withReplacement, fraction, seed).count()


```
# COMMAND ----------
```python

dataFrames = df.randomSplit([0.25, 0.75], seed)
dataFrames[0].count() > dataFrames[1].count() # False


```
# COMMAND ----------
```python

from pyspark.sql import Row
schema = df.schema
newRows = [
  Row("New Country", "Other Country", 5L),
  Row("New Country 2", "Other Country 3", 1L)
]
parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows, schema)


```
# COMMAND ----------
```python

df.union(newDF)\
  .where("count = 1")\
  .where(col("ORIGIN_COUNTRY_NAME") != "United States")\
  .show()


```
# COMMAND ----------
```python

df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)


```
# COMMAND ----------
```python

from pyspark.sql.functions import desc, asc
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)


```
# COMMAND ----------
```python

spark.read.format("json").load("/data/flight-data/json/*-summary.json")\
  .sortWithinPartitions("count")


```
# COMMAND ----------
```python

df.limit(5).show()


```
# COMMAND ----------
```python

df.orderBy(expr("count desc")).limit(6).show()


```
# COMMAND ----------
```python

df.rdd.getNumPartitions() # 1


```
# COMMAND ----------
```python

df.repartition(5)


```
# COMMAND ----------
```python

df.repartition(col("DEST_COUNTRY_NAME"))


```
# COMMAND ----------
```python

df.repartition(5, col("DEST_COUNTRY_NAME"))


```
# COMMAND ----------
```python

df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)


```
# COMMAND ----------
```python

collectDF = df.limit(10)
collectDF.take(5) # take works with an Integer count
collectDF.show() # this prints it out nicely
collectDF.show(5, False)
collectDF.collect()


```
# COMMAND ----------
```python

code/Structured_APIs-Chapter_6_Working_with_Different_Types_of_Data.py
```
# COMMAND ----------
```python

df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/data/retail-data/by-day/2010-12-01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")


```
# COMMAND ----------
```python

from pyspark.sql.functions import lit
df.select(lit(5), lit("five"), lit(5.0))


```
# COMMAND ----------
```python

from pyspark.sql.functions import col
df.where(col("InvoiceNo") != 536365)\
  .select("InvoiceNo", "Description")\
  .show(5, False)


```
# COMMAND ----------
```python

from pyspark.sql.functions import instr
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()


```
# COMMAND ----------
```python

from pyspark.sql.functions import instr
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
  .where("isExpensive")\
  .select("unitPrice", "isExpensive").show(5)


```
# COMMAND ----------
```python

from pyspark.sql.functions import expr
df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))\
  .where("isExpensive")\
  .select("Description", "UnitPrice").show(5)


```
# COMMAND ----------
```python

from pyspark.sql.functions import expr, pow
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)


```
# COMMAND ----------
```python

df.selectExpr(
  "CustomerId",
  "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)


```
# COMMAND ----------
```python

from pyspark.sql.functions import lit, round, bround

df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)


```
# COMMAND ----------
```python

from pyspark.sql.functions import corr
df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()


```
# COMMAND ----------
```python

df.describe().show()


```
# COMMAND ----------
```python

from pyspark.sql.functions import count, mean, stddev_pop, min, max


```
# COMMAND ----------
```python

colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
df.stat.approxQuantile("UnitPrice", quantileProbs, relError) # 2.51


```
# COMMAND ----------
```python

df.stat.crosstab("StockCode", "Quantity").show()


```
# COMMAND ----------
```python

df.stat.freqItems(["StockCode", "Quantity"]).show()


```
# COMMAND ----------
```python

from pyspark.sql.functions import monotonically_increasing_id
df.select(monotonically_increasing_id()).show(2)


```
# COMMAND ----------
```python

from pyspark.sql.functions import initcap
df.select(initcap(col("Description"))).show()


```
# COMMAND ----------
```python

from pyspark.sql.functions import lower, upper
df.select(col("Description"),
    lower(col("Description")),
    upper(lower(col("Description")))).show(2)


```
# COMMAND ----------
```python

from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
    ltrim(lit("    HELLO    ")).alias("ltrim"),
    rtrim(lit("    HELLO    ")).alias("rtrim"),
    trim(lit("    HELLO    ")).alias("trim"),
    lpad(lit("HELLO"), 3, " ").alias("lp"),
    rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)


```
# COMMAND ----------
```python

from pyspark.sql.functions import regexp_replace
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
  regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
  col("Description")).show(2)


```
# COMMAND ----------
```python

from pyspark.sql.functions import translate
df.select(translate(col("Description"), "LEET", "1337"),col("Description"))\
  .show(2)


```
# COMMAND ----------
```python

from pyspark.sql.functions import regexp_extract
extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
     regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
     col("Description")).show(2)


```
# COMMAND ----------
```python

from pyspark.sql.functions import instr
containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite)\
  .where("hasSimpleColor")\
  .select("Description").show(3, False)


```
# COMMAND ----------
```python

from pyspark.sql.functions import expr, locate
simpleColors = ["black", "white", "red", "green", "blue"]
def color_locator(column, color_string):
  return locate(color_string.upper(), column)\
          .cast("boolean")\
          .alias("is_" + color_string)
selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*")) # has to a be Column type

df.select(*selectedColumns).where(expr("is_white OR is_red"))\
  .select("Description").show(3, False)


```
# COMMAND ----------
```python

from pyspark.sql.functions import current_date, current_timestamp
dateDF = spark.range(10)\
  .withColumn("today", current_date())\
  .withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")


```
# COMMAND ----------
```python

from pyspark.sql.functions import date_add, date_sub
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)


```
# COMMAND ----------
```python

from pyspark.sql.functions import datediff, months_between, to_date
dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
  .select(datediff(col("week_ago"), col("today"))).show(1)

dateDF.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))\
  .select(months_between(col("start"), col("end"))).show(1)


```
# COMMAND ----------
```python

from pyspark.sql.functions import to_date, lit
spark.range(5).withColumn("date", lit("2017-01-01"))\
  .select(to_date(col("date"))).show(1)


```
# COMMAND ----------
```python

from pyspark.sql.functions import to_date
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")


```
# COMMAND ----------
```python

from pyspark.sql.functions import to_timestamp
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()


```
# COMMAND ----------
```python

from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show()


```
# COMMAND ----------
```python

df.na.drop("all", subset=["StockCode", "InvoiceNo"])


```
# COMMAND ----------
```python

df.na.fill("all", subset=["StockCode", "InvoiceNo"])


```
# COMMAND ----------
```python

fill_cols_vals = {"StockCode": 5, "Description" : "No Value"}
df.na.fill(fill_cols_vals)


```
# COMMAND ----------
```python

df.na.replace([""], ["UNKNOWN"], "Description")

code/Structured_APIs-Chapter_7_Aggregations.py
```
# COMMAND ----------
```python

df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/data/retail-data/all/*.csv")\
  .coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")


```
# COMMAND ----------
```python

from pyspark.sql.functions import count
df.select(count("StockCode")).show() # 541909


```
# COMMAND ----------
```python

from pyspark.sql.functions import countDistinct
df.select(countDistinct("StockCode")).show() # 4070


```
# COMMAND ----------
```python

from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("StockCode", 0.1)).show() # 3364


```
# COMMAND ----------
```python

from pyspark.sql.functions import first, last
df.select(first("StockCode"), last("StockCode")).show()


```
# COMMAND ----------
```python

from pyspark.sql.functions import min, max
df.select(min("Quantity"), max("Quantity")).show()


```
# COMMAND ----------
```python

from pyspark.sql.functions import sum
df.select(sum("Quantity")).show() # 5176450


```
# COMMAND ----------
```python

from pyspark.sql.functions import sumDistinct
df.select(sumDistinct("Quantity")).show() # 29310


```
# COMMAND ----------
```python

from pyspark.sql.functions import sum, count, avg, expr

df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))\
  .selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases").show()


```
# COMMAND ----------
```python

from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp
df.select(var_pop("Quantity"), var_samp("Quantity"),
  stddev_pop("Quantity"), stddev_samp("Quantity")).show()


```
# COMMAND ----------
```python

from pyspark.sql.functions import skewness, kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()


```
# COMMAND ----------
```python

from pyspark.sql.functions import corr, covar_pop, covar_samp
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity")).show()


```
# COMMAND ----------
```python

from pyspark.sql.functions import collect_set, collect_list
df.agg(collect_set("Country"), collect_list("Country")).show()


```
# COMMAND ----------
```python

from pyspark.sql.functions import count

df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()


```
# COMMAND ----------
```python

df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)"))\
  .show()


```
# COMMAND ----------
```python

from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")


```
# COMMAND ----------
```python

from pyspark.sql.window import Window
from pyspark.sql.functions import desc
windowSpec = Window\
  .partitionBy("CustomerId", "date")\
  .orderBy(desc("Quantity"))\
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)


```
# COMMAND ----------
```python

from pyspark.sql.functions import max
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)


```
# COMMAND ----------
```python

from pyspark.sql.functions import dense_rank, rank
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)


```
# COMMAND ----------
```python

from pyspark.sql.functions import col

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
  .select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()


```
# COMMAND ----------
```python

dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")


```
# COMMAND ----------
```python

rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))\
  .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")\
  .orderBy("Date")
rolledUpDF.show()


```
# COMMAND ----------
```python

from pyspark.sql.functions import sum

dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))\
  .select("Date", "Country", "sum(Quantity)").orderBy("Date").show()


```
# COMMAND ----------
```python

pivoted = dfWithDate.groupBy("date").pivot("Country").sum()


```
# COMMAND ----------
```python

code/Structured_APIs-Chapter_8_Joins.py
```
# COMMAND ----------
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
# COMMAND ----------
```python

joinExpression = person["graduate_program"] == graduateProgram['id']


```
# COMMAND ----------
```python

wrongJoinExpression = person["name"] == graduateProgram["school"]


```
# COMMAND ----------
```python

joinType = "inner"


```
# COMMAND ----------
```python

gradProgram2 = graduateProgram.union(spark.createDataFrame([
    (0, "Masters", "Duplicated Row", "Duplicated School")]))

gradProgram2.createOrReplaceTempView("gradProgram2")


```
# COMMAND ----------
```python

from pyspark.sql.functions import expr

person.withColumnRenamed("id", "personId")\
  .join(sparkStatus, expr("array_contains(spark_status, id)")).show()


```
# COMMAND ----------
```python

code/Structured_APIs-Chapter_9_Data_Sources.py
```
# COMMAND ----------
```python

csvFile = spark.read.format("csv")\
  .option("header", "true")\
  .option("mode", "FAILFAST")\
  .option("inferSchema", "true")\
  .load("/data/flight-data/csv/2010-summary.csv")


```
# COMMAND ----------
```python

csvFile.write.format("csv").mode("overwrite").option("sep", "\t")\
  .save("/tmp/my-tsv-file.tsv")


```
# COMMAND ----------
```python

spark.read.format("json").option("mode", "FAILFAST")\
  .option("inferSchema", "true")\
  .load("/data/flight-data/json/2010-summary.json").show(5)


```
# COMMAND ----------
```python

csvFile.write.format("json").mode("overwrite").save("/tmp/my-json-file.json")


```
# COMMAND ----------
```python

spark.read.format("parquet")\
  .load("/data/flight-data/parquet/2010-summary.parquet").show(5)


```
# COMMAND ----------
```python

csvFile.write.format("parquet").mode("overwrite")\
  .save("/tmp/my-parquet-file.parquet")


```
# COMMAND ----------
```python

spark.read.format("orc").load("/data/flight-data/orc/2010-summary.orc").show(5)


```
# COMMAND ----------
```python

csvFile.write.format("orc").mode("overwrite").save("/tmp/my-json-file.orc")


```
# COMMAND ----------
```python

driver = "org.sqlite.JDBC"
path = "/data/flight-data/jdbc/my-sqlite.db"
url = "jdbc:sqlite:" + path
tablename = "flight_info"


```
# COMMAND ----------
```python

dbDataFrame = spark.read.format("jdbc").option("url", url)\
  .option("dbtable", tablename).option("driver",  driver).load()


```
# COMMAND ----------
```python

pgDF = spark.read.format("jdbc")\
  .option("driver", "org.postgresql.Driver")\
  .option("url", "jdbc:postgresql://database_server")\
  .option("dbtable", "schema.tablename")\
  .option("user", "username").option("password", "my-secret-password").load()


```
# COMMAND ----------
```python

dbDataFrame.filter("DEST_COUNTRY_NAME in ('Anguilla', 'Sweden')").explain()


```
# COMMAND ----------
```python

pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
  AS flight_info"""
dbDataFrame = spark.read.format("jdbc")\
  .option("url", url).option("dbtable", pushdownQuery).option("driver",  driver)\
  .load()


```
# COMMAND ----------
```python

dbDataFrame = spark.read.format("jdbc")\
  .option("url", url).option("dbtable", tablename).option("driver",  driver)\
  .option("numPartitions", 10).load()


```
# COMMAND ----------
```python

props = {"driver":"org.sqlite.JDBC"}
predicates = [
  "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
  "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'"]
spark.read.jdbc(url, tablename, predicates=predicates, properties=props).show()
spark.read.jdbc(url,tablename,predicates=predicates,properties=props)\
  .rdd.getNumPartitions() # 2


```
# COMMAND ----------
```python

props = {"driver":"org.sqlite.JDBC"}
predicates = [
  "DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'",
  "DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'"]
spark.read.jdbc(url, tablename, predicates=predicates, properties=props).count()


```
# COMMAND ----------
```python

colName = "count"
lowerBound = 0L
upperBound = 348113L # this is the max count in our database
numPartitions = 10


```
# COMMAND ----------
```python

spark.read.jdbc(url, tablename, column=colName, properties=props,
                lowerBound=lowerBound, upperBound=upperBound,
                numPartitions=numPartitions).count() # 255


```
# COMMAND ----------
```python

newPath = "jdbc:sqlite://tmp/my-sqlite.db"
csvFile.write.jdbc(newPath, tablename, mode="overwrite", properties=props)


```
# COMMAND ----------
```python

spark.read.jdbc(newPath, tablename, properties=props).count() # 255


```
# COMMAND ----------
```python

csvFile.write.jdbc(newPath, tablename, mode="append", properties=props)


```
# COMMAND ----------
```python

spark.read.jdbc(newPath, tablename, properties=props).count() # 765


```
# COMMAND ----------
```python


csvFile.limit(10).select("DEST_COUNTRY_NAME", "count")\
  .write.partitionBy("count").text("/tmp/five-csv-files2py.csv")


```
# COMMAND ----------
```python

csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME")\
  .save("/tmp/partitioned-files.parquet")


```
# COMMAND ----------
```python

code/Structured_APIs-Chapter_10_Spark_SQL.py
```
# COMMAND ----------
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
# COMMAND ----------
```python

code/Advanced_Analytics_and_Machine_Learning-Chapter_24_Advanced_Analytics_and_Machine_Learning.py
```
# COMMAND ----------
```python

from pyspark.ml.linalg import Vectors
denseVec = Vectors.dense(1.0, 2.0, 3.0)
size = 3
idx = [1, 2] # locations of non-zero elements in vector
values = [2.0, 3.0]
sparseVec = Vectors.sparse(size, idx, values)


```
# COMMAND ----------
```python

df = spark.read.json("/data/simple-ml")
df.orderBy("value2").show()


```
# COMMAND ----------
```python

from pyspark.ml.feature import RFormula
supervised = RFormula(formula="lab ~ . + color:value1 + color:value2")


```
# COMMAND ----------
```python

fittedRF = supervised.fit(df)
preparedDF = fittedRF.transform(df)
preparedDF.show()


```
# COMMAND ----------
```python

train, test = preparedDF.randomSplit([0.7, 0.3])


```
# COMMAND ----------
```python

from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(labelCol="label",featuresCol="features")


```
# COMMAND ----------
```python

print lr.explainParams()


```
# COMMAND ----------
```python

fittedLR = lr.fit(train)


```
# COMMAND ----------
```python

train, test = df.randomSplit([0.7, 0.3])


```
# COMMAND ----------
```python

rForm = RFormula()
lr = LogisticRegression().setLabelCol("label").setFeaturesCol("features")


```
# COMMAND ----------
```python

from pyspark.ml import Pipeline
stages = [rForm, lr]
pipeline = Pipeline().setStages(stages)


```
# COMMAND ----------
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
# COMMAND ----------
```python

from pyspark.ml.evaluation import BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator()\
  .setMetricName("areaUnderROC")\
  .setRawPredictionCol("prediction")\
  .setLabelCol("label")


```
# COMMAND ----------
```python

from pyspark.ml.tuning import TrainValidationSplit
tvs = TrainValidationSplit()\
  .setTrainRatio(0.75)\
  .setEstimatorParamMaps(params)\
  .setEstimator(pipeline)\
  .setEvaluator(evaluator)


```
# COMMAND ----------
```python

tvsFitted = tvs.fit(train)


# COMMAND ----------