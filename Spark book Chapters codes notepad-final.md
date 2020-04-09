code/A_Gentle_Introduction_to_Spark-Chapter_2_A_Gentle_Introduction_to_Spark.py

## Proper explanation
```python

myRange = spark.range(1000).toDF("number")


```
## Proper explanation
```python

divisBy2 = myRange.where("number % 2 = 0")


```
## Proper explanation
```python

flightData2015 = spark\
  .read\
  .option("inferSchema", "true")\
  .option("header", "true")\
  .csv("/data/flight-data/csv/2015-summary.csv")

```
## Proper explanation
```python

flightData2015.createOrReplaceTempView("flight_data_2015")


```
## Proper explanation
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
## Proper explanation
```python

from pyspark.sql.functions import max

flightData2015.select(max("count")).take(1)


```
## Proper explanation
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
## Proper explanation
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
## Proper explanation
```python

flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .explain()


```
## Proper explanation
```python

code/A_Gentle_Introduction_to_Spark-Chapter_3_A_Tour_of_Sparks_Toolset.py
```
## Proper explanation
```python

staticDataFrame = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/data/retail-data/by-day/*.csv")

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema


```
## Proper explanation
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
## Proper explanation
```python

streamingDataFrame = spark.readStream\
    .schema(staticSchema)\
    .option("maxFilesPerTrigger", 1)\
    .format("csv")\
    .option("header", "true")\
    .load("/data/retail-data/by-day/*.csv")


```
## Proper explanation
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
## Proper explanation
```python

purchaseByCustomerPerHour.writeStream\
    .format("memory")\
    .queryName("customer_purchases")\
    .outputMode("complete")\
    .start()


```
## Proper explanation
```python

spark.sql("""
  SELECT *
  FROM customer_purchases
  ORDER BY `sum(total_cost)` DESC
  """)\
  .show(5)


```
## Proper explanation
```python

from pyspark.sql.functions import date_format, col
preppedDataFrame = staticDataFrame\
  .na.fill(0)\
  .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))\
  .coalesce(5)


```
## Proper explanation
```python

trainDataFrame = preppedDataFrame\
  .where("InvoiceDate < '2011-07-01'")
testDataFrame = preppedDataFrame\
  .where("InvoiceDate >= '2011-07-01'")


```
## Proper explanation
```python

from pyspark.ml.feature import StringIndexer
indexer = StringIndexer()\
  .setInputCol("day_of_week")\
  .setOutputCol("day_of_week_index")


```
## Proper explanation
```python

from pyspark.ml.feature import OneHotEncoder
encoder = OneHotEncoder()\
  .setInputCol("day_of_week_index")\
  .setOutputCol("day_of_week_encoded")


```
## Proper explanation
```python

from pyspark.ml.feature import VectorAssembler

vectorAssembler = VectorAssembler()\
  .setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"])\
  .setOutputCol("features")


```
## Proper explanation
```python

from pyspark.ml import Pipeline

transformationPipeline = Pipeline()\
  .setStages([indexer, encoder, vectorAssembler])


```
## Proper explanation
```python

fittedPipeline = transformationPipeline.fit(trainDataFrame)


```
## Proper explanation
```python

transformedTraining = fittedPipeline.transform(trainDataFrame)


```
## Proper explanation
```python

from pyspark.ml.clustering import KMeans
kmeans = KMeans()\
  .setK(20)\
  .setSeed(1L)


```
## Proper explanation
```python

kmModel = kmeans.fit(transformedTraining)


```
## Proper explanation
```python

transformedTest = fittedPipeline.transform(testDataFrame)


```
## Proper explanation
```python

from pyspark.sql import Row

spark.sparkContext.parallelize([Row(1), Row(2), Row(3)]).toDF()


```
## Proper explanation
```python

code/Structured_APIs-Chapter_4_Structured_API_Overview.py
```
## Proper explanation
```python

df = spark.range(500).toDF("number")
df.select(df["number"] + 10)


```
## Proper explanation
```python

spark.range(2).collect()


```
## Proper explanation
```python

from pyspark.sql.types import *
b = ByteType()


```
## Proper explanation
```python

code/Structured_APIs-Chapter_5_Basic_Structured_Operations.py
```
## Proper explanation
```python

df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")


```
## Proper explanation
```python

spark.read.format("json").load("/data/flight-data/json/2015-summary.json").schema


```
## Proper explanation
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
## Proper explanation
```python

from pyspark.sql.functions import col, column
col("someColumnName")
column("someColumnName")


```
## Proper explanation
```python

from pyspark.sql.functions import expr
expr("(((someCol + 5) * 200) - 6) < otherCol")


```
## Proper explanation
```python

from pyspark.sql import Row
myRow = Row("Hello", None, 1, False)


```
## Proper explanation
```python

myRow[0]
myRow[2]


```
## Proper explanation
```python

df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")
df.createOrReplaceTempView("dfTable")


```
## Proper explanation
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
## Proper explanation
```python

df.select("DEST_COUNTRY_NAME").show(2)


```
## Proper explanation
```python

df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)


```
## Proper explanation
```python

from pyspark.sql.functions import expr, col, column
df.select(
    expr("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"))\
  .show(2)


```
## Proper explanation
```python

df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)


```
## Proper explanation
```python

df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))\
  .show(2)


```
## Proper explanation
```python

df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)


```
## Proper explanation
```python

df.selectExpr(
  "*", # all original columns
  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
  .show(2)


```
## Proper explanation
```python

df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)


```
## Proper explanation
```python

from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("One")).show(2)


```
## Proper explanation
```python

df.withColumn("numberOne", lit(1)).show(2)


```
## Proper explanation
```python

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))\
  .show(2)


```
## Proper explanation
```python

df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns


```
## Proper explanation
```python

dfWithLongColName = df.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME"))


```
## Proper explanation
```python

dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")\
  .show(2)


```
## Proper explanation
```python

dfWithLongColName.select(expr("`This Long Column-Name`")).columns


```
## Proper explanation
```python

df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia")\
  .show(2)


```
## Proper explanation
```python

df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()


```
## Proper explanation
```python

df.select("ORIGIN_COUNTRY_NAME").distinct().count()


```
## Proper explanation
```python

seed = 5
withReplacement = False
fraction = 0.5
df.sample(withReplacement, fraction, seed).count()


```
## Proper explanation
```python

dataFrames = df.randomSplit([0.25, 0.75], seed)
dataFrames[0].count() > dataFrames[1].count() # False


```
## Proper explanation
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
## Proper explanation
```python

df.union(newDF)\
  .where("count = 1")\
  .where(col("ORIGIN_COUNTRY_NAME") != "United States")\
  .show()


```
## Proper explanation
```python

df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)


```
## Proper explanation
```python

from pyspark.sql.functions import desc, asc
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)


```
## Proper explanation
```python

spark.read.format("json").load("/data/flight-data/json/*-summary.json")\
  .sortWithinPartitions("count")


```
## Proper explanation
```python

df.limit(5).show()


```
## Proper explanation
```python

df.orderBy(expr("count desc")).limit(6).show()


```
## Proper explanation
```python

df.rdd.getNumPartitions() # 1


```
## Proper explanation
```python

df.repartition(5)


```
## Proper explanation
```python

df.repartition(col("DEST_COUNTRY_NAME"))


```
## Proper explanation
```python

df.repartition(5, col("DEST_COUNTRY_NAME"))


```
## Proper explanation
```python

df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)


```
## Proper explanation
```python

collectDF = df.limit(10)
collectDF.take(5) # take works with an Integer count
collectDF.show() # this prints it out nicely
collectDF.show(5, False)
collectDF.collect()


```
## Proper explanation
```python

code/Structured_APIs-Chapter_6_Working_with_Different_Types_of_Data.py
```
## Proper explanation
```python

df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/data/retail-data/by-day/2010-12-01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")


```
## Proper explanation
```python

from pyspark.sql.functions import lit
df.select(lit(5), lit("five"), lit(5.0))


```
## Proper explanation
```python

from pyspark.sql.functions import col
df.where(col("InvoiceNo") != 536365)\
  .select("InvoiceNo", "Description")\
  .show(5, False)


```
## Proper explanation
```python

from pyspark.sql.functions import instr
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()


```
## Proper explanation
```python

from pyspark.sql.functions import instr
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
  .where("isExpensive")\
  .select("unitPrice", "isExpensive").show(5)


```
## Proper explanation
```python

from pyspark.sql.functions import expr
df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))\
  .where("isExpensive")\
  .select("Description", "UnitPrice").show(5)


```
## Proper explanation
```python

from pyspark.sql.functions import expr, pow
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)


```
## Proper explanation
```python

df.selectExpr(
  "CustomerId",
  "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)


```
## Proper explanation
```python

from pyspark.sql.functions import lit, round, bround

df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)


```
## Proper explanation
```python

from pyspark.sql.functions import corr
df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()


```
## Proper explanation
```python

df.describe().show()


```
## Proper explanation
```python

from pyspark.sql.functions import count, mean, stddev_pop, min, max


```
## Proper explanation
```python

colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
df.stat.approxQuantile("UnitPrice", quantileProbs, relError) # 2.51


```
## Proper explanation
```python

df.stat.crosstab("StockCode", "Quantity").show()


```
## Proper explanation
```python

df.stat.freqItems(["StockCode", "Quantity"]).show()


```
## Proper explanation
```python

from pyspark.sql.functions import monotonically_increasing_id
df.select(monotonically_increasing_id()).show(2)


```
## Proper explanation
```python

from pyspark.sql.functions import initcap
df.select(initcap(col("Description"))).show()


```
## Proper explanation
```python

from pyspark.sql.functions import lower, upper
df.select(col("Description"),
    lower(col("Description")),
    upper(lower(col("Description")))).show(2)


```
## Proper explanation
```python

from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
    ltrim(lit("    HELLO    ")).alias("ltrim"),
    rtrim(lit("    HELLO    ")).alias("rtrim"),
    trim(lit("    HELLO    ")).alias("trim"),
    lpad(lit("HELLO"), 3, " ").alias("lp"),
    rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)


```
## Proper explanation
```python

from pyspark.sql.functions import regexp_replace
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
  regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
  col("Description")).show(2)


```
## Proper explanation
```python

from pyspark.sql.functions import translate
df.select(translate(col("Description"), "LEET", "1337"),col("Description"))\
  .show(2)


```
## Proper explanation
```python

from pyspark.sql.functions import regexp_extract
extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
     regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
     col("Description")).show(2)


```
## Proper explanation
```python

from pyspark.sql.functions import instr
containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite)\
  .where("hasSimpleColor")\
  .select("Description").show(3, False)


```
## Proper explanation
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
## Proper explanation
```python

from pyspark.sql.functions import current_date, current_timestamp
dateDF = spark.range(10)\
  .withColumn("today", current_date())\
  .withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")


```
## Proper explanation
```python

from pyspark.sql.functions import date_add, date_sub
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)


```
## Proper explanation
```python

from pyspark.sql.functions import datediff, months_between, to_date
dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
  .select(datediff(col("week_ago"), col("today"))).show(1)

dateDF.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))\
  .select(months_between(col("start"), col("end"))).show(1)


```
## Proper explanation
```python

from pyspark.sql.functions import to_date, lit
spark.range(5).withColumn("date", lit("2017-01-01"))\
  .select(to_date(col("date"))).show(1)


```
## Proper explanation
```python

from pyspark.sql.functions import to_date
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")


```
## Proper explanation
```python

from pyspark.sql.functions import to_timestamp
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()


```
## Proper explanation
```python

from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show()


```
## Proper explanation
```python

df.na.drop("all", subset=["StockCode", "InvoiceNo"])


```
## Proper explanation
```python

df.na.fill("all", subset=["StockCode", "InvoiceNo"])


```
## Proper explanation
```python

fill_cols_vals = {"StockCode": 5, "Description" : "No Value"}
df.na.fill(fill_cols_vals)


```
## Proper explanation
```python

df.na.replace([""], ["UNKNOWN"], "Description")

code/Structured_APIs-Chapter_7_Aggregations.py
```
## Proper explanation
```python

df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/data/retail-data/all/*.csv")\
  .coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")


```
## Proper explanation
```python

from pyspark.sql.functions import count
df.select(count("StockCode")).show() # 541909


```
## Proper explanation
```python

from pyspark.sql.functions import countDistinct
df.select(countDistinct("StockCode")).show() # 4070


```
## Proper explanation
```python

from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("StockCode", 0.1)).show() # 3364


```
## Proper explanation
```python

from pyspark.sql.functions import first, last
df.select(first("StockCode"), last("StockCode")).show()


```
## Proper explanation
```python

from pyspark.sql.functions import min, max
df.select(min("Quantity"), max("Quantity")).show()


```
## Proper explanation
```python

from pyspark.sql.functions import sum
df.select(sum("Quantity")).show() # 5176450


```
## Proper explanation
```python

from pyspark.sql.functions import sumDistinct
df.select(sumDistinct("Quantity")).show() # 29310


```
## Proper explanation
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
## Proper explanation
```python

from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp
df.select(var_pop("Quantity"), var_samp("Quantity"),
  stddev_pop("Quantity"), stddev_samp("Quantity")).show()


```
## Proper explanation
```python

from pyspark.sql.functions import skewness, kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()


```
## Proper explanation
```python

from pyspark.sql.functions import corr, covar_pop, covar_samp
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity")).show()


```
## Proper explanation
```python

from pyspark.sql.functions import collect_set, collect_list
df.agg(collect_set("Country"), collect_list("Country")).show()


```
## Proper explanation
```python

from pyspark.sql.functions import count

df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()


```
## Proper explanation
```python

df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)"))\
  .show()


```
## Proper explanation
```python

from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")


```
## Proper explanation
```python

from pyspark.sql.window import Window
from pyspark.sql.functions import desc
windowSpec = Window\
  .partitionBy("CustomerId", "date")\
  .orderBy(desc("Quantity"))\
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)


```
## Proper explanation
```python

from pyspark.sql.functions import max
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)


```
## Proper explanation
```python

from pyspark.sql.functions import dense_rank, rank
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)


```
## Proper explanation
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
## Proper explanation
```python

dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")


```
## Proper explanation
```python

rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))\
  .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")\
  .orderBy("Date")
rolledUpDF.show()


```
## Proper explanation
```python

from pyspark.sql.functions import sum

dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))\
  .select("Date", "Country", "sum(Quantity)").orderBy("Date").show()


```
## Proper explanation
```python

pivoted = dfWithDate.groupBy("date").pivot("Country").sum()


```
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
