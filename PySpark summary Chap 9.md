
## Reading CSV Files Options
```python

csvFile = spark.read.format("csv")\
  .option("header", "true")\
  .option("mode", "FAILFAST")\
  .option("inferSchema", "true")\
  .load("/data/flight-data/csv/2010-summary.csv")


```
## Take CSV File and Write it as a TSV File
```python

csvFile.write.format("csv").mode("overwrite").option("sep", "\t")\
  .save("/tmp/my-tsv-file.tsv")


```
## Reading JSON Files
```python

spark.read.format("json").option("mode", "FAILFAST")\
  .option("inferSchema", "true")\
  .load("/data/flight-data/json/2010-summary.json").show(5)


```
## Writing JSON Files
```python

csvFile.write.format("json").mode("overwrite").save("/tmp/my-json-file.json")


```
## Reading Parquet Files
```python

spark.read.format("parquet")\
  .load("/data/flight-data/parquet/2010-summary.parquet").show(5)


```
## Writing Parquet Files
```python

csvFile.write.format("parquet").mode("overwrite")\
  .save("/tmp/my-parquet-file.parquet")


```
## Reading ORC Files
```python

spark.read.format("orc").load("/data/flight-data/orc/2010-summary.orc").show(5)


```
## Writing ORC Files
```python

csvFile.write.format("orc").mode("overwrite").save("/tmp/my-json-file.orc")


```
## Reading from SQL Databases
```python

driver = "org.sqlite.JDBC"
path = "/data/flight-data/jdbc/my-sqlite.db"
url = "jdbc:sqlite:" + path
tablename = "flight_info"


```
## Read the DataFrame from the SQL Table
```python

dbDataFrame = spark.read.format("jdbc").option("url", url)\
  .option("dbtable", tablename).option("driver",  driver).load()


```
## Read Using PostgreSQL 
```python

pgDF = spark.read.format("jdbc")\
  .option("driver", "org.postgresql.Driver")\
  .option("url", "jdbc:postgresql://database_server")\
  .option("dbtable", "schema.tablename")\
  .option("user", "username").option("password", "my-secret-password").load()


```
## Query Pushdown with PushedFilters
```python

dbDataFrame.filter("DEST_COUNTRY_NAME in ('Anguilla', 'Sweden')").explain()


```
## Pass an Entire Query into SQL, Return Results as a DataFrame
```python

pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
  AS flight_info"""
dbDataFrame = spark.read.format("jdbc")\
  .option("url", url).option("dbtable", pushdownQuery).option("driver",  driver)\
  .load()


```
## Reading from Databases in Parallel
```python

dbDataFrame = spark.read.format("jdbc")\
  .option("url", url).option("dbtable", tablename).option("driver",  driver)\
  .option("numPartitions", 10).load()


```
## Specifying a List of Predicates While Creating the Data Source
```python

props = {"driver":"org.sqlite.JDBC"}
predicates = [
  "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
  "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'"]
spark.read.jdbc(url, tablename, predicates=predicates, properties=props).show()
spark.read.jdbc(url,tablename,predicates=predicates,properties=props)\
  .rdd.getNumPartitions() # 2


```
## Set of Predicates that Will Result in Duplicate Rows
```python

props = {"driver":"org.sqlite.JDBC"}
predicates = [
  "DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'",
  "DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'"]
spark.read.jdbc(url, tablename, predicates=predicates, properties=props).count()


```
## Partitioning Based on a Sliding Window
```python

colName = "count"
lowerBound = 0L
upperBound = 348113L # this is the max count in our database
numPartitions = 10


```
## Distribute the Intervals Equally from Low to High during Partitioning 
```python

spark.read.jdbc(url, tablename, column=colName, properties=props,
                lowerBound=lowerBound, upperBound=upperBound,
                numPartitions=numPartitions).count() # 255


```
## Writing to SQL Database
```python

newPath = "jdbc:sqlite://tmp/my-sqlite.db"
csvFile.write.jdbc(newPath, tablename, mode="overwrite", properties=props)


```
## Output of Writed SQL 
```python

spark.read.jdbc(newPath, tablename, properties=props).count() # 255


```
## Append a New Table to An Existing table
```python

csvFile.write.jdbc(newPath, tablename, mode="append", properties=props)


```
## Increase Count After Append
```python

spark.read.jdbc(newPath, tablename, properties=props).count() # 765


```
## Writing Text Files
```python


csvFile.limit(10).select("DEST_COUNTRY_NAME", "count")\
  .write.partitionBy("count").text("/tmp/five-csv-files2py.csv")


```
## Partitioning to Control What Data is Stored (And Where)
```python

csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME")\
  .save("/tmp/partitioned-files.parquet")

```
