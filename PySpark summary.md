# Chapter 2 
Based on [Spark-The-Definitive-Guide]: https://github.com/databricks/Spark-The-Definitive-Guide

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
