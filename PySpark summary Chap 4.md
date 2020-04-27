## Addition Purely in Spark (Not in Python)
```python

df = spark.range(500).toDF("number")
df.select(df["number"] + 10)


```
## Create a Row by Using a Range
```python

spark.range(2).collect()


```
## Work with the Correct Python Types
```python

from pyspark.sql.types import *
b = ByteType()
```
