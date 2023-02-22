# Ejercicio 3

Dadas las siguientes listas, crea un RDD para cada una y devuelve de los 2 RDDs su:
* Union
* Intersección
* Cartesiano

Listas:
* nameRDD1 = sc.parallelize(['Ana','Bob'])
* nameRDD2 = sc.parallelize(['Bob','Caren','Deric'])

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

# Punto #1
Creacion de las siguietne listas:
* nameRDD1 = sc.parallelize(['Ana','Bob'])
* nameRDD2 = sc.parallelize(['Bob','Caren','Deric'])


```python
nameRDD1 = sc.parallelize(['Ana','Bob'])
nameRDD2 = sc.parallelize(['Bob','Caren','Deric'])
```

## Punto #2

devuelve de los 2 RDDs su:

* Union
* Intersección
* Cartesiano

### Union


```python
nameRDD1.union(nameRDD2).collect()
```




    ['Ana', 'Bob', 'Bob', 'Caren', 'Deric']



### Intersección


```python
nameRDD1.intersection(nameRDD2).collect()
```




    ['Bob']



### Cartesiano


```python
nameRDD1.cartesian(nameRDD2).collect()
```




    [('Ana', 'Bob'),
     ('Ana', 'Caren'),
     ('Ana', 'Deric'),
     ('Bob', 'Bob'),
     ('Bob', 'Caren'),
     ('Bob', 'Deric')]


