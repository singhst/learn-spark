# pyspark-note

- [pyspark-note](#pyspark-note)
- [Concept](#concept)
- [Basic operation](#basic-operation)
  - [Transformations](#transformations)
    - [`.map()` v.s. `.mapPartitions()` v.s. `.mapPartitionsWithIndex()`](#map-vs-mappartitions-vs-mappartitionswithindex)
      - [1. `.map()`](#1-map)
      - [2. `.mapPartitions()`](#2-mappartitions)
      - [3. `.mapPartitionsWithIndex()`](#3-mappartitionswithindex)
    - [`.map()` v.s. `.flatmap()`](#map-vs-flatmap)
    - [`.foreach()` v.s. `.map()`](#foreach-vs-map)
  - [Actions](#actions)
    - [`sc.parallelize()`](#scparallelize)
    - [`.count()`](#count)
    - [`.collect()`](#collect)
    - [`.getNumPartitions()`](#getnumpartitions)
    - [`.glom().collect()`](#glomcollect)
    - [`.foreach()`](#foreach)
      - [`.map()` v.s. `.foreach()`](#map-vs-foreach)
    - [`.reduce()`](#reduce)
- [Spark recomputes transformations](#spark-recomputes-transformations)
  - [`.cache()`/`.persist()`](#cachepersist)
- [RDD - Closure](#rdd---closure)
  - [Closure example](#closure-example)
    - [Incorrect way - `global` variable as counter](#incorrect-way---global-variable-as-counter)
    - [Correct way (1) - `rdd.sum()`](#correct-way-1---rddsum)
    - [Correct way (2) - `.accumulator()`](#correct-way-2---accumulator)
      - [Note](#note)
  - [Accumulator](#accumulator)
- [Deal with `JSON` data](#deal-with-json-data)
  - [Details](#details)
- [Spark Dataframe](#spark-dataframe)
  - [Create sparkdf by reading `.csv`](#create-sparkdf-by-reading-csv)
  - [`.printSchema()` in df](#printschema-in-df)
  - [`.groupBy().count()`](#groupbycount)
  - [`df.createOrReplaceTempView("sql_table")`, allows to run SQL queries once register `df` as temporary tables](#dfcreateorreplacetempviewsql_table-allows-to-run-sql-queries-once-register-df-as-temporary-tables)
  - [`.join()`/`spark.sql()` dataframes](#joinsparksql-dataframes)
    - [`.join()`](#join)
    - [`spark.sql()` + `df.createOrReplaceTempView("sql_table")`](#sparksql--dfcreateorreplacetempviewsql_table)
  - [`df1.union(df2)` concat 2 dataframes](#df1uniondf2-concat-2-dataframes)
- [Graph, edge, vertice, Graphframe](#graph-edge-vertice-graphframe)
- [spark-install-macos](#spark-install-macos)
  - [How to start Jupyter Notebook with Spark + GraphFrames](#how-to-start-jupyter-notebook-with-spark--graphframes)
    - [Start it locally](#start-it-locally)
    - [Start in Google Colab](#start-in-google-colab)
    - [Use MongoDB in Spark](#use-mongodb-in-spark)
  - [Test Spark in Jupyter Notebook](#test-spark-in-jupyter-notebook)
- [Reference](#reference)


# Concept

The records/items/elemets are stored in RDD(s).

Each RDD composists of `Partitions`; each `Partition` contains equal number of `items`/`elements`.

<img src="img\rddDataset-partitions-items.png" height="200"/>

# Basic operation

RDD Programming Guide

==> https://spark.apache.org/docs/latest/rdd-programming-guide.html

## Transformations

### `.map()` v.s. `.mapPartitions()` v.s. `.mapPartitionsWithIndex()`

Spark的map，mapPartitions，mapPartitionsWithIndex詳解

==> https://blog.csdn.net/QQ1131221088/article/details/104051087

---

#### 1. `.map()`

Return a new distributed dataset formed by passing each element of the source through a function func.

```python
rdd_2 = sc.parallelize(range(10), 4)

new_rdd_2 = rdd_2.map(lambda x: str(x))
print('> .map() =\n', new_rdd_2.glom().collect())
print()

#################################################
from pyspark import TaskContext

result = rdd_2.map(lambda x :x+TaskContext().partitionId())
print('> Original RDD, rdd_2.glom().collect() =\n', rdd_2.glom().collect())
print()
print('> .map() with TaskContext().partitionId() =\n', result.glom().collect())
```

Output:

```shell
> .map() =
 [['0', '1'], ['2', '3', '4'], ['5', '6'], ['7', '8', '9']]

> Original RDD, rdd_2.glom().collect() =
 [[0, 1], [2, 3, 4], [5, 6], [7, 8, 9]]

> .map() with TaskContext().partitionId() =
 [[0, 1], [3, 4, 5], [7, 8], [10, 11, 12]]
```

#### 2. `.mapPartitions()`

Similar to `.map()`, but runs separately on each partition (block) of the RDD, so func must be of type `Iterator<T> => Iterator<U>` when running on an RDD of type T.

==> `Divide-and-Conquer` algorithm => master node `divides` the RDD into partitions, and distributes the partitions to workers, workers apply the same function to its partition. Then master node gets back (i.e. `conquer`) the processed resuts from all workers.

(1)
```python 
rdd_2 = sc.parallelize([1,2,3,4,5,'a','b','c','d','e'], 4)

def func(itemsIteratorInPartition):
    # apply this `func` to each partition (=the whole partition) of the RDD
    yield str(itemsIteratorInPartition)
    
rdd_func = rdd_2.mapPartitions(func)
print('rdd_2.mapPartitions(func) =\n', rdd_func.glom().collect())
print()
```

Output:
```shell
rdd_2.mapPartitions(func) =
 [['<itertools.chain object at 0x7ff8094580d0>'], ['<itertools.chain object at 0x7ff8094580d0>'], ['<itertools.chain object at 0x7ff8094580d0>'], ['<itertools.chain object at 0x7ff8094580d0>']]
```

(2)
```python
def func_2(itemsIteratorInPartition):
    # you loop through each item in each partition of the RDD
    # = just apply this `func_2` to each item in each partition
    for item in itemsIteratorInPartition: 
        yield str(item)

rdd_func_2 = rdd_2.mapPartitions(func_2)
print('rdd_2.mapPartitions(func_2) =\n', rdd_func_2.glom().collect())
```

Output:
```shell
rdd_2.mapPartitions(func_2) =
 [['1', '2'], ['3', '4'], ['5', 'a'], ['b', 'c', 'd', 'e']]
```

#### 3. `.mapPartitionsWithIndex()`

Similar to `mapPartitions`, but also provides func with an integer value representing the index of the partition, so func must be of type `(Int, Iterator<T>) => Iterator<U>` when running on an RDD of type T.

By using mapParitionsWithIndex you could output new elements which have their partition in it, then when you reduce you will know which partition you are handling the elements from. 

==> https://stackoverflow.com/questions/31281225/find-out-the-partition-no-id

```python
rdd_3 = sc.parallelize(range(10), 4)

# mapPartitionsWithIndex
def func(partitionIndex, itemsIteratorInPartition): 
    # apply this `func` to each partition (=the whole partition) of the RDD
    yield (partitionIndex, sum(itemsIteratorInPartition))
new_rdd_3 = rdd_3.mapPartitionsWithIndex(func)

# glom() flattens elements on the same partition
print('> rdd_3.glom().collect() =', rdd_3.glom().collect())
print('> new_rdd_3.glom().collect() =', new_rdd_3.glom().collect())


################################################################################

def func_2(partitionIndex, itemsIteratorInPartition):
    # you loop through each item in each partition of the RDD
    # = just apply this `func_2` to each item in each partition
    for item in itemsIteratorInPartition: 
        yield str(item+partitionIndex)
new_2_rdd_3 = rdd_3.mapPartitionsWithIndex(func_2)

# glom() flattens elements on the same partition
print()
print('>> new_2_rdd_3.glom().collect() =', new_2_rdd_3.glom().collect())
```

Output:

```shell
> rdd_3.glom().collect() = [[0, 1], [2, 3, 4], [5, 6], [7, 8, 9]]
> new_rdd_3.glom().collect() = [[(0, 1)], [(1, 9)], [(2, 11)], [(3, 24)]]

>> new_2_rdd_3.glom().collect() = [['0', '1'], ['3', '4', '5'], ['7', '8'], ['10', '11', '12']]
```

---

### `.map()` v.s. `.flatmap()`

```python
print(sc.version, '\n')

py_list = [str(x) for x in range(5)]
rdd = sc.parallelize(py_list)

# map
new_rdd = rdd.map(lambda item: item+'xx')
print('.map() =\n', new_rdd.collect())
print()

# flatmap
# same as .map(), but flatten the results before returns
# i.e. remove all `list of list`/`nested list`
new_rdd = rdd.flatMap(lambda item: item+'xx')
print('.flatmap() =\n', new_rdd.collect())
```

Output:

```shell
3.1.2

.map() =
['0xx', '1xx', '2xx', '3xx', '4xx']

.flatmap() =
['0', 'x', 'x', '1', 'x', 'x', '2', 'x', 'x', '3', 'x', 'x', '4', 'x', 'x']
```

### `.foreach()` v.s. `.map()`

==> See [`.foreach()`](#foreach) / [`.map()` v.s. `.foreach()`](#map-vs-foreach)


## Actions

**Below example shows there are 100 items/elements in this RDD, and this RDD is partitioned into 4 partitions (or items are grouped in 4 partitions).**

### `sc.parallelize()`

Store python list `[0,1,...,99]` as RDD in Spark. This dataset ***is not loaded in memory***. It is merely ***a pointer to the Python `py_list`***.

```python
# Stores python list `[0,1,...,99]` as RDD in Spark
## This dataset is not loaded in memory
## It is merely a pointer to the Python `py_list` 
py_list = range(100)
rdd = sc.parallelize(py_list)
print(rdd)
```

Output:

```shell
PythonRDD[11] at RDD at PythonRDD.scala:53
```

### `.count()`

Returns the number of items in this RDD

```python
#.count()
# Shows the number of items in this RDD
print('rdd.count()=', rdd.count())
```

Output:
```shell
100
```

### `.collect()`

Returns all the items in this RDD as python list

```python
#.collect()
# Returns all the items in this RDD as python list
print('rdd.collect()=', rdd.collect())
```

Output:
```shell
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99]
```

### `.getNumPartitions()`

Returns number of partitions in this RDD

```python
#.getNumPartitions()
# Gets number of partitions in this RDD
print('rdd.getNumPartitions()=', rdd.getNumPartitions())
```

Output:
```shell
4
```

### `.glom().collect()`

Returns the content of each partitions as `nested list` / `list of list`

```python
# Returns the content of each partitions as `nested list` / `list of list`
rdd.glom().collect()
```

Output:

```shell
[
 [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24], 
 [25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49], 
 [50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74], 
 [75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99]
]
```

### `.foreach()`

Just executes inside function for each data element in the **RDD**, but return ***NOTHING***. 

#### `.map()` v.s. `.foreach()`

Short answer
* `.map()`: 
  1. is for transforming one RDD into another, then return the transformed.
  2. Return a new RDD by applying a function to each element of this RDD.
* `.foreach()`: is for applying an operation/function on all elements of this RDD.

    Note: RDD = 1 collection of elements

==> [is-there-a-difference-between-foreach-and-map](https://stackoverflow.com/questions/354909/is-there-a-difference-between-foreach-and-map)

Long answer:

The important difference between them is that `map` accumulates all of the results into a collection, whereas `foreach` returns nothing. `map` is usually used when you want to transform a collection of elements with a function, whereas `foreach` simply executes an action for each element.

* In short, `foreach` is for applying an operation on each element of a collection of elements, whereas `map` is for transforming one collection into another.

* `foreach` works with a single collection of elements. This is the input collection.

* `map` works with two collections of elements: the input collection and the output collection.

### `.reduce()`

```python
rdd = sc.parallelize([('a',7),('a',2),('b',2)])
rdd.reduce(lambda a, b: a + b) #Merge the rdd values
```
```
('a',7,'a',2,'b',2)
```


# Spark recomputes transformations

Transformed RDD is thrown away from memory after execution. If afterward transformations/actions need it, PySpark recompiles it.

P.S. Better solution: [`.cache()`/`.persist()`](#cachepersist) the transformed RDD

```python
A = sc.parallelize(range(1, 1000)) 
t = 100
B = A.filter(lambda x: x*x < t)
print('B.collect()=', B.collect())  # B.collect()= [1, 2, 3, 4, 5, 6, 7, 8, 9]
## Here: B finishes execution and is thrown away from memory

t = 200
C = B.filter(lambda x: x*x < t) # C needs B again, so recomputes B, but t=200 not =100
                                # So, 
                                # B = A.filter(lambda x: x*x < 200)
                                # C = B.filter(lambda x: x*x < 200)
print('C.collect()=', C.collect())  # C.collect()= [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
print('C.count()=', C.count())      # C.count()= 14
```

## `.cache()`/`.persist()`
```python
A = sc.parallelize(range(1, 1000)) 
t = 100
B = A.filter(lambda x: x*x < t)
print('B.collect()=', B.collect())  # B.collect()= [1, 2, 3, 4, 5, 6, 7, 8, 9]

# save this RDD in memory/disk
B.cache()
# B.persist()

## Here: B is in memory

t = 200
C = B.filter(lambda x: x*x < t) # C needs B again, memory stores B, NO need to recompute B
                                # So, 
                                # B = previous B
                                # C = B.filter(lambda x: x*x < 200)
print('C.collect()=', C.collect())  # C.collect()= [1, 2, 3, 4, 5, 6, 7, 8, 9]
print('C.count()=', C.count())      # C.count()= 9
```


# RDD - Closure

https://mycupoftea00.medium.com/understanding-closure-in-spark-af6f280eebf9

https://spark.apache.org/docs/latest/rdd-programming-guide.html#understanding-closures-

## Closure example

### Incorrect way - `global` variable as counter

Q: Why printed counter is 0?

Ans: Because 
1. each executor (i.e. worker node) just applies `increment_counter()` func on its own copy of counter. 
2. Also, `.foreach()` returns nothing

```python
counter = 0
rdd = sc.parallelize(range(10))
print('rdd.collect() =', rdd.collect())

# Wrong: Don't do this!!
def increment_counter(x):
    global counter
    counter += x

rdd.foreach(increment_counter)
print('counter =', counter) # just print out `counter` from your driver program, not from Spark
```

Output:
```
rdd.collect() = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
counter = 0
```

### Correct way (1) - `rdd.sum()`

Correct way to do the above operation:
1. The `.sum()` action is executed in Spark executor(s)
2. `.sum()` returns the sum value

```python
print('rdd.sum() =', rdd.sum())
```

Output:
```
rdd.sum() = 45
```

### Correct way (2) - `.accumulator()`

Use `.accumulator()` can also solve the issue.

```python
rdd = sc.parallelize(range(10))
accum = sc.accumulator(0)

def g(x):
    global accum
    accum += x

a = rdd.foreach(g)

print(accum.value)
```

```
45
```

#### Note

Update in transformations may be applied more than once if tasks or job stages are re-executed.

```python
rdd = sc.parallelize(range(10))
accum = sc.accumulator(0)

def g(x):
    global accum
    accum += x
    return x * x

a = rdd.map(g)
print(type(accum))
print(accum.value) # 0, because no action presents, `accum` is not immediately computed (= laziness/lazy execution)
# print(a.reduce(lambda x, y: x+y))
a.cache()
tmp = a.count()
print(accum.value) # 45
print(rdd.reduce(lambda x, y: x+y)) # 45

tmp = a.count()
print(accum.value) # 45
print(rdd.reduce(lambda x, y: x+y)) # 45
```

Output:
```
<class 'pyspark.accumulators.Accumulator'>
0   #why it is 0? because of "lazy execution", if no actions present, "accum" is not compiled immediately
45
45
45
45
```

## Accumulator

https://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators

Note from lecture note: `Suggestion: Avoid using accumulators whenever possible. Use reduce() instead.`

# Deal with `JSON` data

**https://sparkbyexamples.com/pyspark/pyspark-maptype-dict-examples/**

Steps,
1. JSON from API 
2. get `list of dict` 
3. pySpark dataframe with `map type` 
4. access PySpark MapType Elements

## Details

1. 
    ```python
    # The nested json / list of dictionary
    data_json = [
        ('James', {'hair': 'black', 'eye': 'brown'}),
        ('Michael', {'hair': 'brown', 'eye': None}),
        ('Robert', {'hair': 'red', 'eye': 'black'}),
        ('Washington', {'hair': 'grey', 'eye': 'grey'}),
        ('Jefferson', {'hair': 'brown', 'eye': ''})
    ]
    df = spark.createDataFrame(data=data_json)
    df.printSchema()
    ```

    Output:

    ```
    root
    |-- Name: string (nullable = true)
    |-- properties: map (nullable = true)
    |    |-- key: string
    |    |-- value: string (valueContainsNull = true)

    +----------+-----------------------------+
    |Name      |properties                   |
    +----------+-----------------------------+
    |James     |[eye -> brown, hair -> black]|
    |Michael   |[eye ->, hair -> brown]      |
    |Robert    |[eye -> black, hair -> red]  |
    |Washington|[eye -> grey, hair -> grey]  |
    |Jefferson |[eye -> , hair -> brown]     |
    +----------+-----------------------------+
    ```

2. Access the elements in map datatype

    Method (1):
    ```python
    df3 = df.rdd.map(lambda x: \
                    (x.name, x.properties["hair"], x.properties["eye"])) \
                    .toDF(["name", "hair", "eye"])
    df3.printSchema()
    df3.show()
    ```

    OR

    Method (2):
    ```python
    df.withColumn("hair", df.properties.getItem("hair")) \
        .withColumn("eye", df.properties.getItem("eye")) \
        .drop("properties") \
        .show()

    df.withColumn("hair", df.properties["hair"]) \
        .withColumn("eye", df.properties["eye"]) \
        .drop("properties") \
        .show()
    ```

    Output:
    ```
    root
    |-- name: string (nullable = true)
    |-- hair: string (nullable = true)
    |-- eye: string (nullable = true)

    +----------+-----+-----+
    |      name| hair|  eye|
    +----------+-----+-----+
    |     James|black|brown|
    |   Michael|brown| null|
    |    Robert|  red|black|
    |Washington| grey| grey|
    | Jefferson|brown|     |
    +----------+-----+-----+
    ```

# Spark Dataframe

## Create sparkdf by reading `.csv`

* http://www.cse.ust.hk/msbd5003/data/customer.csv
* http://www.cse.ust.hk/msbd5003/data/orders.csv

`customer.csv`:
```shell
CUSTKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,MKTSEGMENT,COMMENT
1,Customer#000000001,IVhzIApeRb ot,c,E,15,25-989-741-2988,711.56,BUILDING,to the even, regular platelets. regular, ironic epitaphs nag e,
2,Customer#000000002,XSTf4,NCwDVaWNe6tEgvwfmRchLXak,13,23-768-687-3665,121.65,AUTOMOBILE,l accounts. blithely ironic theodolites integrate boldly: caref,
3,Customer#000000003,MG9kdTD2WBHm,1,11-719-748-3364,7498.12,AUTOMOBILE, deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov,
4,Customer#000000004,XxVSJsLAGtn,4,14-128-190-5944,2866.83,MACHINERY, requests. final, regular ideas sleep final accou,
...
```

`orders.csv`:
```shell
ORDERKEY,CUSTKEY,ORDERSTATUS,TOTALPRICE,ORDERDATE,ORDERPRIORITY,CLERK,SHIPPRIORITY,COMMENT
1,370,O,172799.49,1996-01-02,5-LOW,Clerk#000000951,0,nstructions sleep furiously among ,
2,781,O,38426.09,1996-12-01,1-URGENT,Clerk#000000880,0, foxes. pending accounts at the pending, silent asymptot,
3,1234,F,205654.30,1993-10-14,5-LOW,Clerk#000000955,0,sly final accounts boost. carefully regular ideas cajole carefully. depos,
4,1369,O,56000.91,1995-10-11,5-LOW,Clerk#000000124,0,sits. slyly regular warthogs cajole. regular, regular theodolites acro,
5,445,F,105367.67,1994-07-30,5-LOW,Clerk#000000925,0,quickly. bold deposits sleep slyly. packages use slyly,
...
```

```python
dfCustomer = spark.read.csv('customer.csv', header=True, inferSchema=True)
dfOrders = spark.read.csv('orders.csv', header=True, inferSchema=True)
```

## `.printSchema()` in df

```python
dfCustomer.printSchema()
print(dfCustomer.count())

dfOrders.printSchema()
print(dfOrders.count())
```

Output:
```shell
root
 |-- CUSTKEY: integer (nullable = true)
 |-- NAME: string (nullable = true)
 |-- ADDRESS: string (nullable = true)
 |-- NATIONKEY: string (nullable = true)
 |-- PHONE: string (nullable = true)
 |-- ACCTBAL: string (nullable = true)
 |-- MKTSEGMENT: string (nullable = true)
 |-- COMMENT: string (nullable = true)
1500

root
 |-- ORDERKEY: integer (nullable = true)
 |-- CUSTKEY: integer (nullable = true)
 |-- ORDERSTATUS: string (nullable = true)
 |-- TOTALPRICE: double (nullable = true)
 |-- ORDERDATE: string (nullable = true)
 |-- ORDERPRIORITY: string (nullable = true)
 |-- CLERK: string (nullable = true)
 |-- SHIPPRIORITY: integer (nullable = true)
 |-- COMMENT: string (nullable = true)
 15000
```

## `.groupBy().count()`

Find `count of orders` of each customer `CUSTKEY` has:
```python
dfOrders_groupby = dfOrders.groupBy('CUSTKEY').count()
dfOrders_groupby.toPandas()
```

Output:
```
	CUSTKEY	count
0	463	20
1	1342	20
2	496	18
3	148	15
4	1088	7
...	...	...
995	401	12
996	517	25
997	422	12
998	89	7
999	1138	23
1000 rows × 2 columns
```

## `df.createOrReplaceTempView("sql_table")`, allows to run SQL queries once register `df` as temporary tables

```python
dfOrders_groupby.createOrReplaceTempView("sql_table")

# Can run SQL query on it
df = spark.sql("SELECT customer.CUSTKEY, orders.count FROM customer left outer join orders on customer.CUSTKEY = orders.CUSTKEY")
df.toPandas()
```

Output:
```
    CUSTKEY	count
0	1	9.0
1	2	10.0
2	3	NaN
3	4	31.0
4	5	9.0
...	...	...
1495	1496	9.0
1496	1497	NaN
1497	1498	20.0
1498	1499	21.0
1499	1500	NaN
1500 rows × 2 columns
```

## `.join()`/`spark.sql()` dataframes
### `.join()`
```python
# join 2 df by `CUSTKEY`
joined_df = dfCustomer.join(dfOrders, on='CUSTKEY', how='leftouter')
df2 = joined_df.select('CUSTKEY', 'ORDERKEY').sort(asc('CUSTKEY'),desc('ORDERKEY'))
df2.toPandas() #view
```

`how`: str, optional
    default ``inner``. Must be one of: ``inner``, ``cross``, ``outer``,
    ``full``, ``fullouter``, ``full_outer``, ``left``, ``leftouter``, ``left_outer``,
    ``right``, ``rightouter``, ``right_outer``, ``semi``, ``leftsemi``, ``left_semi``,
    ``anti``, ``leftanti`` and ``left_anti``.

### `spark.sql()` + `df.createOrReplaceTempView("sql_table")`
```python
dfOrders.createOrReplaceTempView("orders")
dfCustomer.createOrReplaceTempView("customer")

# Can run SQL query on it
df2 = spark.sql("SELECT customer.CUSTKEY, orders.ORDERKEY FROM customer left outer join orders on customer.CUSTKEY = orders.CUSTKEY")
df2.toPandas()
```

Output:
```shell
    CUSTKEY	ORDERKEY
0	1	53283.0
1	1	52263.0
2	1	43879.0
3	1	36422.0
4	1	34019.0
...	...	...
15495	1499	7301.0
15496	1499	3523.0
15497	1499	1472.0
15498	1499	1252.0
15499	1500	NaN
15500 rows × 2 columns
```

## `df1.union(df2)` concat 2 dataframes

The dataframes may need to have identical columns, in which case you can use `withColumn()` to create `normal_1` and `normal_2`

```python
df_concat = df_1.union(df_2)
```

# Graph, edge, vertice, Graphframe

[link](https://github.com/cenzwong/tech/tree/master/Note/Spark#graphframe)


# spark-install-macos

Run the following in macOS terminal,

## How to start Jupyter Notebook with Spark + GraphFrames

### Start it locally

1. Modify the PATH variables,
    ``` shell
    $ nano ~/.bashrc
    ```

2. Add the following lines in `~/.bashrc`, so `spark` and `jupyter notebook` can be launched at the same time.

    ```shell
    # Setting PATH for Spark 3.1.2
    export SPARK_HOME=/usr/local/Cellar/apache-spark/3.1.2/libexec
    export PATH="$SPARK_HOME/bin/:$PATH"
    export PYSPARK_DRIVER_PYTHON="jupyter"
    export PYSPARK_DRIVER_PYTHON_OPTS="notebook"
    ```

    In terminal,
    
    <img src="img\nano bashrc PATH variables.png" style="zoom:40%;" />

3. Update $PATH variable,

    ```shell
    $ source ~/.bashrc
    ```

4. Ensure `graphframes-0.8.1-spark3.0-s_2.12.jar` presents in the `/Users/<USER_NAME>/.ivy2/jars` folder:

    <img src="img\folder-files.png"  height="300" />

    Start the `pyspark` with `graphframes` in terminal,
    
    ==> Needs 2 flags, `--packages` and `--jars`

    ==> Ensure the `graphframes` package name is same as `graphframes-0.8.1-spark3.0-s_2.12.jar` in folder.
    
    => Deal with error: `java.lang.ClassNotFoundException: org.graphframes.GraphFramePythonAPI`

    => https://blog.csdn.net/qq_42166929/article/details/105983616 
    
    ```shell
    $ pyspark --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 --jars graphframes-0.8.1-spark3.0-s_2.12.jar
    ```
    
    Terminal output:
    
    <img src="img\start-pyspark-terminal-output.png" height="500" />

    Jupyter Notebook:

    <img src="img\start-pyspark-jupyter-notebook.png" height="200" />

### Start in Google Colab

[Still need to investigate]

https://github.com/cenzwong/tech/tree/master/Note/Spark#graphframe

### Use MongoDB in Spark

    Terminal:

    ```shell 
    pyspark --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 --jars graphframes-0.8.1-spark3.0-s_2.12.jar \
              --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/test.application_test?readPreference=primaryPreferred" \
              --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/test.application_test" \
              --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1
    ```

    Notebook:
    ```python
    from pymongo import MongoClient 

    def _saveDfToMongoDB(sparkDF, mongoCollection):
        sparkDF.cache()
        print(f"Storing {sparkDF.count()} {mongoCollection} to db")

        start = datetime.now()

        sparkDF.write.format("mongo") \
            .mode("append") \
            .option("database", "msbd5003") \
            .option("collection", mongoCollection) \
            .save()

        end = datetime.now()
        spent = (end - start).total_seconds()
        print(f"Stored, time used: {spent} s")
    
    df = spark.read.json("file path")
    _saveDfToMongoDB(df, "mongodb collection name")
    ```

## Test Spark in Jupyter Notebook

Inside Jupyter Notebook:

==> Reference: https://medium.com/@roshinijohri/spark-with-jupyter-notebook-on-macos-2-0-0-and-higher-c61b971b5007

<img src="img\test-spark-jupyter-notebook.png" height="450" />

Cell 1:

```python
print(sc.version)
```

Output 1:

```shell
3.1.2
```

OR

```python
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
spark
```

Output:

```shell
SparkSession - hive
SparkContext

Spark UI
Version
v3.1.2
Master
local[*]
AppName
PySparkShell
```

Cell 2:

```python
path_to_the_jar_file = 'graphframes-0.8.1-spark3.0-s_2.12.jar'
sc.addPyFile(path_to_the_jar_file)
```

Cell 3:

```python
from pyspark.sql import SQLContext 
sqlContext = SQLContext(sc) 
v = sqlContext.createDataFrame([("a", ),("b", ),], ["id", ]) 
e = sqlContext.createDataFrame([("a", "b"),], ["src", "dst"]) 

from graphframes import * 
g = GraphFrame(v, e) 
g.inDegrees.show()
```

Output 3:

```shell
+---+--------+
| id|inDegree|
+---+--------+
|  b|       1|
+---+--------+
```

Cell 4:

```python
import pyspark
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName("spark test").getOrCreate()
columns = ['id', 'dogs', 'cats']
vals = [
     (1, 2, 0),
     (2, 0, 1)
]
# create DataFrame
df = spark.createDataFrame(vals, columns)
df.show()
```

Output 4:

```shell
+---+----+----+
| id|dogs|cats|
+---+----+----+
|  1|   2|   0|
|  2|   0|   1|
+---+----+----+
```


# Reference

1. Official pyspark example: 
https://github.com/spark-examples/pyspark-examples

2. 