# Ejercicio 2

Crea un rdd llamado nameRDD2 : [['Ana','Bob'],['Karen']], usa map o flatmap para regresar:
* \# ans1: ['Ana', 'Bob', 'plus', 'Caren', 'plus']
* \# ans2: [['Ana', 'Bob', 'plus'], ['Caren', 'plus']]

## Importaciones y creacion se SparkSession y SparkContext


```python
import findspark
findspark.init()

import pandas as pd
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
```

## Punto #1

Crea un rdd llamado nameRDD2 : [['Ana','Bob'],['Karen']],


```python
nameRDD2 = sc.parallelize([['Ana','Bob'],['Karen']])
nameRDD2.collect()
```




    [['Ana', 'Bob'], ['Karen']]



## Punto #2

usa map o flatmap para regresar:

* \# ans1: ['Ana', 'Bob', 'plus', 'Caren', 'plus']
* \# ans2: [['Ana', 'Bob', 'plus'], ['Caren', 'plus']]


```python
ans1 = nameRDD2.flatMap(lambda name: name + ["plus"])
ans1.collect()
```




    ['Ana', 'Bob', 'plus', 'Karen', 'plus']




```python
ans2 = nameRDD2.map(lambda name: name + ["plus"])
ans2.collect()
```




    [['Ana', 'Bob', 'plus'], ['Karen', 'plus']]


