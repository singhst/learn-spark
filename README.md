# pyspark-note

- [pyspark-note](#pyspark-note)
- [Concept](#concept)
  - [Where code runs](#where-code-runs)
- [RDD Basic operation](#rdd-basic-operation)
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
    - [`.glom().collect()`](#glomcollect)
    - [`.getNumPartitions()`](#getnumpartitions)
    - [`.foreach()`](#foreach)
      - [`.map()` v.s. `.foreach()`](#map-vs-foreach)
    - [`.reduce()`](#reduce)
  - [Spark recomputes transformations](#spark-recomputes-transformations)
    - [`.cache()`/`.persist()`](#cachepersist)
    - [Issue of `df.cache()` with two same reference variables](#issue-of-dfcache-with-two-same-reference-variables)
    - [Best practice - `.cache()`/`.persist()`](#best-practice---cachepersist)
  - [RDD - Closure](#rdd---closure)
    - [Closure example](#closure-example)
      - [Incorrect way - `global` variable as counter](#incorrect-way---global-variable-as-counter)
      - [Correct way (1) - `rdd.sum()`](#correct-way-1---rddsum)
      - [Correct way (2) - `.accumulator()`](#correct-way-2---accumulator)
      - [Note](#note)
    - [Accumulator](#accumulator)
- [Deal with `JSON` data](#deal-with-json-data)
  - [\[1\] Load `.json`/`.json.gz` files to pySpark dataframe](#1-load-jsonjsongz-files-to-pyspark-dataframe)
  - [\[2\] Load JSON from String / `.txt` files to pySpark dataframe](#2-load-json-from-string--txt-files-to-pyspark-dataframe)
    - [Read json from text files](#read-json-from-text-files)
    - [\[a\] WITHOUT schema definition](#a-without-schema-definition)
    - [\[b\] WITH schema definition](#b-with-schema-definition)
      - [\[b1\] Create schema manually](#b1-create-schema-manually)
      - [\[b2\] Create schema from JSON / Dict](#b2-create-schema-from-json--dict)
  - [Parse JSON from RESTful API](#parse-json-from-restful-api)
  - [\[NOT GOOD\] ~~Read `JSON` string to pySpark dataframe~~](#not-good-read-json-string-to-pyspark-dataframe)
    - [Read `JSON` to spark Dataframe first](#read-json-to-spark-dataframe-first)
    - [Details](#details)
  - [Use `.` and `:` syntax to query nested data](#use--and--syntax-to-query-nested-data)
- [Deal with `.parquet`](#deal-with-parquet)
  - [What is `.parquet`?](#what-is-parquet)
- [Spark Dataframe](#spark-dataframe)
  - [Create sparkdf by reading `.csv`](#create-sparkdf-by-reading-csv)
    - [Normal read](#normal-read)
    - [Read range of file names](#read-range-of-file-names)
      - [Character range `[a-b]` read](#character-range-a-b-read)
      - [Alternation `{a,b,c}` read](#alternation-abc-read)
  - [\[ing\] Speed Up Reading .csv/.json with schema](#ing-speed-up-reading-csvjson-with-schema)
  - [convert Map, Array, or Struct Type Columns into JSON Strings](#convert-map-array-or-struct-type-columns-into-json-strings)
    - [Exaplme Data](#exaplme-data)
    - [`Map` / `MapType` Column to JSON StringType](#map--maptype-column-to-json-stringtype)
    - [`List of MapType` column into JSON StringType](#list-of-maptype-column-into-json-stringtype)
    - [`ArrayType` column into JSON StringType](#arraytype-column-into-json-stringtype)
  - [Change Column Type in a StructType](#change-column-type-in-a-structtype)
  - [Merge/Union Two DataFrames with Different Columns or Schema](#mergeunion-two-dataframes-with-different-columns-or-schema)
    - [(1) `unionByName(allowMissingColumns=True)`](#1-unionbynameallowmissingcolumnstrue)
    - [(2) Create missing columns manually](#2-create-missing-columns-manually)
  - [Rename Columns](#rename-columns)
    - [(1.1) Built-in `withColumnsRenamed()` (New in version 3.4.0)](#11-built-in-withcolumnsrenamed-new-in-version-340)
    - [(1.2) Built-in `withColumnRenamed()`](#12-built-in-withcolumnrenamed)
    - [(2) `SELECT` method, `df.select(*[F.col(old_name).alias("new_name") for old_name in rename_map])`](#2-select-method-dfselectfcolold_namealiasnew_name-for-old_name-in-rename_map)
    - [Lowercase all column names](#lowercase-all-column-names)
    - [Lowercase values in all columns](#lowercase-values-in-all-columns)
  - [`.printSchema()` in df](#printschema-in-df)
  - [Deal with `datetime` / `timestamp`](#deal-with-datetime--timestamp)
    - [`F.unix_timestamp()`, convert timestamp `string` with custom format to `datetime object`](#funix_timestamp-convert-timestamp-string-with-custom-format-to-datetime-object)
    - [Change time zone](#change-time-zone)
    - [Get day of week from `datetime` / `timestamp`](#get-day-of-week-from-datetime--timestamp)
  - [`F.create_map()` in `df.withColumn()`, create a `dict` column](#fcreate_map-in-dfwithcolumn-create-a-dict-column)
  - [`.groupBy().count()`](#groupbycount)
  - [`groupBy().agg()`](#groupbyagg)
  - [`groupBy().collect_set()` / `groupBy().collect_list()`](#groupbycollect_set--groupbycollect_list)
  - [Combine array of map to single map, `groupBy().collect_list()`](#combine-array-of-map-to-single-map-groupbycollect_list)
  - [`df.createOrReplaceTempView("sql_table")`, allows to run SQL queries once register `df` as temporary tables](#dfcreateorreplacetempviewsql_table-allows-to-run-sql-queries-once-register-df-as-temporary-tables)
  - [Window functions](#window-functions)
  - [`.join()`/`spark.sql()` dataframes](#joinsparksql-dataframes)
    - [`.join()`](#join)
    - [`spark.sql()` + `df.createOrReplaceTempView("sql_table")`](#sparksql--dfcreateorreplacetempviewsql_table)
  - [`df1.union(df2)` concat 2 dataframes](#df1uniondf2-concat-2-dataframes)
  - [UDF `df.withColumn`, user defined function](#udf-dfwithcolumn-user-defined-function)
    - [Explicitly define `udf`](#explicitly-define-udf)
    - [Decorator `@udf`](#decorator-udf)
    - [Custom decorator `@py_or_udf`](#custom-decorator-py_or_udf)
  - [`melt()` - Wide to long](#melt---wide-to-long)
  - [Pandas Function API - Normal Pyspark UDF, pandas\_udf, applyInPandas, mapInPandas](#pandas-function-api---normal-pyspark-udf-pandas_udf-applyinpandas-mapinpandas)
    - [Normal Pyspark UDF](#normal-pyspark-udf)
  - [Find duplicates and non-duplicates](#find-duplicates-and-non-duplicates)
    - [(1) `exceptAll()` what is pyspark "exceptAll()" function? Explain it with example](#1-exceptall-what-is-pyspark-exceptall-function-explain-it-with-example)
    - [(2) `subtract()` - Is this the same as "subtract()"? Can I use join (but which type of join) to achieve this?](#2-subtract---is-this-the-same-as-subtract-can-i-use-join-but-which-type-of-join-to-achieve-this)
    - [`left anti join` - how to get the duplicates and non-duplicates if I use this "left anti" join?](#left-anti-join---how-to-get-the-duplicates-and-non-duplicates-if-i-use-this-left-anti-join)
- [Databricks](#databricks)
  - [Connect to Azure Data Lake Storage Gen2 and Blob Storage](#connect-to-azure-data-lake-storage-gen2-and-blob-storage)
    - [Access Files in Azure Storage Account](#access-files-in-azure-storage-account)
  - [Write string to a single .txt file](#write-string-to-a-single-txt-file)
  - [Asynchronous logic from Databricks](#asynchronous-logic-from-databricks)
    - [Async download images to local then upload to Azure Blob Storage](#async-download-images-to-local-then-upload-to-azure-blob-storage)
- [Graph, edge, vertice, Graphframe](#graph-edge-vertice-graphframe)
  - [`GraphFrame(v, e)`, Create GraphFrame](#graphframev-e-create-graphframe)
  - [Explore `GraphFrame`](#explore-graphframe)
  - [Filter, `g.filterVerices()` `g.filterEdges()`](#filter-gfilterverices-gfilteredges)
  - [`.find("(a)-[e]->(b)")`, Motif finding](#finda-e-b-motif-finding)
  - [Subgraphs](#subgraphs)
- [Database Connection](#database-connection)
  - [Read Database Concurrently](#read-database-concurrently)
- [spark-install-macos](#spark-install-macos)
  - [How to start Jupyter Notebook with Spark + GraphFrames](#how-to-start-jupyter-notebook-with-spark--graphframes)
    - [Start it locally](#start-it-locally)
    - [Start in Google Colab](#start-in-google-colab)
    - [Use MongoDB in Spark](#use-mongodb-in-spark)
      - [Method 1 - Setting in Code / Notebook](#method-1---setting-in-code--notebook)
      - [Method 2 - Setting in Terminal](#method-2---setting-in-terminal)
  - [Test Spark in Jupyter Notebook](#test-spark-in-jupyter-notebook)
- [Reference](#reference)


# Concept

The records/items/elemets are stored in RDD(s).

Each RDD composists of `Partitions`; each `Partition` contains equal number of `items`/`elements`.

<img src="img\rddDataset-partitions-items.png" height="200"/>

## Where code runs

<img src="img/cluster-overview.png" height="200"/>

Source: https://spark.apache.org/docs/latest/cluster-overview.html

Most Python code runs in driver (in our local PC), except for code passed to RDD transformations. 
* Transformations run at executors (in workers), 
* actions run at executors and driver.

# RDD Basic operation

RDD Programming Guide

==> https://spark.apache.org/docs/latest/rdd-programming-guide.html

> All _**transformations**_ in Spark are _**lazy**_, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). The _**transformations**_ are only computed when an _**action**_ requires a result to be returned to the driver program.

--- by [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)

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


## Spark recomputes transformations

Transformed RDD is thrown away from memory after execution. If afterward transformations/actions need it, PySpark recompiles it.

> By default, each transformed RDD may be recomputed each time you run an action on it. However, you may also persist an RDD in memory using the persist (or cache) method, in which case Spark will keep the elements around on the cluster for much faster access the next time you query it. There is also support for persisting RDDs on disk, or replicated across multiple nodes.

<img src="img/spark-rdd-without-cache.png" width="400"> <img src="img/spark-rdd-with-cache.png" width="500">

Image. RDD Without vs With `.cache()` / `.persist()`

P.S. Solution: 
[`.cache()`/`.persist()`](#cachepersist) the transformed RDD

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

### `.cache()`/`.persist()`
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

### Issue of `df.cache()` with two same reference variables

https://stackoverflow.com/questions/60255595/if-i-cache-a-spark-dataframe-and-then-overwrite-the-reference-will-the-original

xxx

### Best practice - `.cache()`/`.persist()`

reference ==> https://towardsdatascience.com/best-practices-for-caching-in-spark-sql-b22fb0f02d34

1. When you cache a DataFrame create a new variable for it `cachedDF = df.cache()`. This will allow you to bypass the problems that we were solving in our example, that sometimes it is not clear what is the analyzed plan and what was actually cached. Here whenever you call `cachedDF.select(...)` it will leverage the cached data.
2. Unpersist the DataFrame after it is no longer needed using `cachedDF.unpersist()`. If the caching layer becomes full, Spark will start evicting the data from memory using the LRU (least recently used) strategy. So it is good practice to use unpersist to stay more in control about what should be evicted. Also, the more space you have in memory the more can Spark use for execution, for instance, for building hash maps and so on.


## RDD - Closure

https://mycupoftea00.medium.com/understanding-closure-in-spark-af6f280eebf9

https://spark.apache.org/docs/latest/rdd-programming-guide.html#understanding-closures-

### Closure example

#### Incorrect way - `global` variable as counter

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

#### Correct way (1) - `rdd.sum()`

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

#### Correct way (2) - `.accumulator()`

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

### Accumulator

https://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators

Note from lecture note: `Suggestion: Avoid using accumulators whenever possible. Use reduce() instead.`

# Deal with `JSON` data

** There are different formats in `JSON`: single-line, multi-line

** Most useful ==> https://zhuanlan.zhihu.com/p/267353998

Another reference: https://sparkbyexamples.com/pyspark/pyspark-read-json-file-into-dataframe/

## [1] Load `.json`/`.json.gz` files to pySpark dataframe

NEED MODIFY, SOMETHING WRONG

**i.e. whole record is present in single line**

Example, a `.json` / `.json.gz`,

1. `jsonFile1.json` (2 records exist)
```json
[{"RecordNumber": 2, "Zipcode": 99999, "ZipCodeType": "STANDARD", "City": "99 PASEO COSTA DEL SUR", "State": "PR"},{"RecordNumber": 10, "Zipcode": 999999999, "ZipCodeType": "STANDARD", "City": "9 BDA SAN LUIS", "State": "PR"}]
```

2. `jsonFile2.json` (2 records exist)
```json
[{"RecordNumber": 99999, "Zipcode": 704, "ZipCodeType": "STANDARD", "City": "xxx PASEO COSTA DEL SUR", "State": "PR"},{"RecordNumber": 999999, "Zipcode": 709, "ZipCodeType": "STANDARD", "City": "xxxx BDA SAN LUIS", "State": "PR"}]
```

Code:
```python
# use `multiline = true` to read multi line JSON file
jsonFiles = ['jsonFile1','jsonFile2']
jsonFiles = [f+'.json' for f in jsonFiles]
print(jsonFiles)
df = spark.read.option('multiline', 'true').json('jsonFile*.json')
df.cache()

df.printSchema()
df.toPandas()
```

Output
```shell
['jsonFile1.json', 'jsonFile2.json']
root
 |-- City: string (nullable = true)
 |-- RecordNumber: long (nullable = true)
 |-- State: string (nullable = true)
 |-- ZipCodeType: string (nullable = true)
 |-- Zipcode: long (nullable = true)

+--------------------+------------+-----+-----------+---------+
|                City|RecordNumber|State|ZipCodeType|  Zipcode|
+--------------------+------------+-----+-----------+---------+
|xxx PASEO COSTA D...|       99999|   PR|   STANDARD|      704|
|   xxxx BDA SAN LUIS|      999999|   PR|   STANDARD|      709|
|99 PASEO COSTA DE...|           2|   PR|   STANDARD|    99999|
|      9 BDA SAN LUIS|          10|   PR|   STANDARD|999999999|
+--------------------+------------+-----+-----------+---------+
```

## [2] Load JSON from String / `.txt` files to pySpark dataframe

https://sparkbyexamples.com/pyspark/pyspark-parse-json-from-string-column-text-file/

`file1.txt`
```txt
{"Zipcode":703,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}
{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PASEO COSTA DEL SUR","State":"PR"}
```

`file2.txt`
```txt
{"Zipcode":299999,"ZipCodeType":"292999STANDARD","City":"PARC PARQUE","State":"PR"}
{"Zipcode":2999,"ZipCodeType":"9999999STANDARD","City":"PASEO COSTA DEL SUR","State":"PR"}
```

### Read json from text files
    ```python
    # (1) read json from text file
    dfFromTxt=spark.read.text("file*.txt")

    dfFromTxt.printSchema()
    dfFromTxt.show(truncate=False)
    ```

    Output
    ```shell
    root
      |-- value: string (nullable = true)

    +------------------------------------------------------------------------------------------+
    |value                                                                                     |
    +------------------------------------------------------------------------------------------+
    |{"Zipcode":299999,"ZipCodeType":"292999STANDARD","City":"PARC PARQUE","State":"PR"}       |
    |{"Zipcode":2999,"ZipCodeType":"9999999STANDARD","City":"PASEO COSTA DEL SUR","State":"PR"}|
    |{"Zipcode":703,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}                |
    |{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PASEO COSTA DEL SUR","State":"PR"}        |
    +------------------------------------------------------------------------------------------+
    ```

### [a] WITHOUT schema definition

    ```python
      # Originally
      # json_df = spark.read.json(dfFromTxt.rdd.map(lambda row: row.value))

      # Explain
      list_of_string = dfFromTxt.rdd.map(lambda row: row.value)
      display(list_of_string.collect())

      json_df = spark.read.json(list_of_string)
      json_df.printSchema()
      json_df.show()
    ```

    Output
    ```shell
      ['{"Zipcode":299999,"ZipCodeType":"292999STANDARD","City":"PARC PARQUE","State":"PR"}',
      '{"Zipcode":2999,"ZipCodeType":"9999999STANDARD","City":"PASEO COSTA DEL SUR","State":"PR"}',
      '{"Zipcode":703,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}',
      '{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PASEO COSTA DEL SUR","State":"PR"}']
      root
        |-- City: string (nullable = true)
        |-- State: string (nullable = true)
        |-- ZipCodeType: string (nullable = true)
        |-- Zipcode: long (nullable = true)

      +-------------------+-----+---------------+-------+
      |               City|State|    ZipCodeType|Zipcode|
      +-------------------+-----+---------------+-------+
      |        PARC PARQUE|   PR| 292999STANDARD| 299999|
      |PASEO COSTA DEL SUR|   PR|9999999STANDARD|   2999|
      |        PARC PARQUE|   PR|       STANDARD|    703|
      |PASEO COSTA DEL SUR|   PR|       STANDARD|    704|
      +-------------------+-----+---------------+-------+
    ```

### [b] WITH schema definition

#### [b1] Create schema manually
1.  Define schema by Pyspark type

    ```python
    # (2) Create Schema of the JSON column
    from pyspark.sql.types import StructType,StructField, StringType
    schema = StructType([ 
        StructField("Zipcode",StringType(),True), 
        StructField("ZipCodeType",StringType(),True), 
        StructField("City",StringType(),True), 
        StructField("State", StringType(), True)
      ])
    schema
    ```

    Output
    ```shell
    StructType(List(StructField(Zipcode,StringType,true),StructField(ZipCodeType,StringType,true),StructField(City,StringType,true),StructField(State,StringType,true)))
    ```

2.  Convert json column to multiple columns

    ```python
    # (3) Convert json column to multiple columns
    from pyspark.sql.functions import col,from_json
    dfJSON = dfFromTxt.withColumn("jsonData",from_json(col("value"),schema)) \
                      .select("jsonData.*")
    dfJSON.printSchema()
    dfJSON.show(truncate=False)
    ```

    Output
    ```shell
    root
      |-- Zipcode: string (nullable = true)
      |-- ZipCodeType: string (nullable = true)
      |-- City: string (nullable = true)
      |-- State: string (nullable = true)

    +-------+---------------+-------------------+-----+
    |Zipcode|ZipCodeType    |City               |State|
    +-------+---------------+-------------------+-----+
    |299999 |292999STANDARD |PARC PARQUE        |PR   |
    |2999   |9999999STANDARD|PASEO COSTA DEL SUR|PR   |
    |703    |STANDARD       |PARC PARQUE        |PR   |
    |704    |STANDARD       |PASEO COSTA DEL SUR|PR   |
    +-------+---------------+-------------------+-----+
    ```

#### [b2] Create schema from JSON / Dict
==> https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield/

  <details>
      <summary>Create schema in Dict / JSON</summary>

```python
  schema_json: dict = {
    "type" : "struct",
    "fields" : [ {
      "name" : "name",
      "type" : {
        "type" : "struct",
        "fields" : [ {
          "name" : "firstname",
          "type" : "string",
          "nullable" : true,
          "metadata" : { }
        }, {
          "name" : "middlename",
          "type" : "string",
          "nullable" : true,
          "metadata" : { }
        }, {
          "name" : "lastname",
          "type" : "string",
          "nullable" : true,
          "metadata" : { }
        } ]
      },
      "nullable" : true,
      "metadata" : { }
    }, {
      "name" : "dob",
      "type" : "string",
      "nullable" : true,
      "metadata" : { }
    }, {
      "name" : "gender",
      "type" : "string",
      "nullable" : true,
      "metadata" : { }
    }, {
      "name" : "salary",
      "type" : "integer",
      "nullable" : true,
      "metadata" : { }
    } ]
  }
```
  </details>

  Create schema from JSON:
  ```python
  import json

  # schemaFromJson = StructType.fromJson(json.loads(df2.schema.json()))
  schemaFromJson = StructType.fromJson(schema_json)
  df3 = spark.createDataFrame(
          spark.sparkContext.parallelize(structureData),schemaFromJson)
  df3.printSchema()
  df3.show(truncate=False)
  ```

  Result:
  ```shell
  root
  |-- name: struct (nullable = true)
  |    |-- firstname: string (nullable = true)
  |    |-- middlename: string (nullable = true)
  |    |-- lastname: string (nullable = true)
  |-- id: string (nullable = true)
  |-- gender: string (nullable = true)
  |-- salary: integer (nullable = true)

  +--------------------+-----+------+------+
  |name                |id   |gender|salary|
  +--------------------+-----+------+------+
  |[James, , Smith]    |36636|M     |3100  |
  |[Michael, Rose, ]   |40288|M     |4300  |
  |[Robert, , Williams]|42114|M     |1400  |
  |[Maria, Anne, Jones]|39192|F     |5500  |
  |[Jen, Mary, Brown]  |     |F     |-1    |
  +--------------------+-----+------+------+
  ```

## Parse JSON from RESTful API

1.  Call API
    ```python
    url = f'https://www.als.ogcio.gov.hk/lookup'
    headers = {'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'}
    params = {'q': requestAddress, 
              'n': 3
              }
    # sending get request and saving the response as response object
    response = requests.get(url=url, headers=headers, params=params)

    _json = response.json()
    _json
    ```

    Output (`list of dict` in py)
    ```python
      {'RequestAddress': {'AddressLine': ['何文田邨']},
       'SuggestedAddress': [{'Address': {'PremisesAddress': {'ChiPremisesAddress': {'BuildingName': '何文田政府合署',
            'ChiDistrict': {'DcDistrict': '九龍城區'},
            'ChiEstate': {'EstateName': '何文田邨'},
            'ChiStreet': {'BuildingNoFrom': '88', 'StreetName': '忠孝街'},
            'Region': '九龍'},
          'EngPremisesAddress': {'BuildingName': 'HO MAN TIN GOVERNMENT OFFICES',
            'EngDistrict': {'DcDistrict': 'KOWLOON CITY DISTRICT'},
            'EngEstate': {'EstateName': 'HO MAN TIN ESTATE'},
            'EngStreet': {'BuildingNoFrom': '88', 'StreetName': 'CHUNG HAU STREET'},
            'Region': 'KLN'},
          'GeoAddress': '3658519520T20050430',
          'GeospatialInformation': {'Easting': '836597',
            'Latitude': '22.31468',
            'Longitude': '114.18007',
            'Northing': '819521'}}},
        'ValidationInformation': {'Score': 75.0}},
        {'Address': {'PremisesAddress': {'ChiPremisesAddress': {'BuildingName': '何文田廣場',
            'ChiDistrict': {'DcDistrict': '九龍城區'},
            'ChiEstate': {'EstateName': '何文田邨'},
            'ChiStreet': {'BuildingNoFrom': '80', 'StreetName': '佛光街'},
            'Region': '九龍'},
          'EngPremisesAddress': {'BuildingName': 'HOMANTIN PLAZA',
            'EngDistrict': {'DcDistrict': 'KOWLOON CITY DISTRICT'},
            'EngEstate': {'EstateName': 'HO MAN TIN ESTATE'},
            'EngStreet': {'BuildingNoFrom': '80', 'StreetName': 'FAT KWONG STREET'},
            'Region': 'KLN'},
          'GeoAddress': '3677919691P20060311',
          'GeospatialInformation': {'Easting': '836780',
            'Latitude': '22.31622',
            'Longitude': '114.18184',
            'Northing': '819692'}}},
        'ValidationInformation': {'Score': 75.0}},
        {'Address': {'PremisesAddress': {'ChiPremisesAddress': {'BuildingName': '靜文樓',
            'ChiDistrict': {'DcDistrict': '九龍城區'},
            'ChiEstate': {'EstateName': '何文田邨'},
            'ChiStreet': {'BuildingNoFrom': '68', 'StreetName': '佛光街'},
            'Region': '九龍'},
          'EngPremisesAddress': {'BuildingName': 'CHING MAN HOUSE',
            'EngDistrict': {'DcDistrict': 'KOWLOON CITY DISTRICT'},
            'EngEstate': {'EstateName': 'HO MAN TIN ESTATE'},
            'EngStreet': {'BuildingNoFrom': '68', 'StreetName': 'FAT KWONG STREET'},
            'Region': 'KLN'},
          'GeoAddress': '3683619541T20050430',
          'GeospatialInformation': {'Easting': '836839',
            'Latitude': '22.31497',
            'Longitude': '114.18242',
            'Northing': '819553'}}},
        'ValidationInformation': {'Score': 62.95}}
        ]
      }
    ```

2.  Convert `list of dict` to PySpark RDD to Dataframe

    ```python
      rdd = sc.parallelize([_json])

      readComplexJSONDF = spark.read.option("multiLine","true").json(rdd)
      readComplexJSONDF.show(truncate=False)
      readComplexJSONDF.printSchema()
    ```

    Output
    ```shell
      +--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
      |RequestAddress|SuggestedAddress                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
      +--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
      | [[何文田邨]]  |   [[[[[何文田政府合署, [九龍城區], [何文田邨], [88, 忠孝街], 九龍], [HO MAN TIN GOVERNMENT OFFICES, [KOWLOON CITY DISTRICT], [HO MAN TIN ESTATE], [88, CHUNG HAU STREET], KLN], 3658519520T20050430, [836597, 22.31468, 114.18007, 819521]]], [75.0]], [[[[何文田政府合署, [九龍城區], [何文田邨], [68, 佛光街], 九龍], [HO MAN TIN GOVERNMENT OFFICES, [KOWLOON CITY DISTRICT], [HO MAN TIN ESTATE], [68, FAT KWONG STREET], KLN], 3658519520T20050430, [836597, 22.31468, 114.18007, 819521]]], [75.0]], [[[[何文田廣場, [九龍城區], [何文田邨], [80, 佛光街], 九龍], [HOMANTIN PLAZA, [KOWLOON CITY DISTRICT], [HO MAN TIN ESTATE], [80, FAT KWONG STREET], KLN], 3677919691P20060311, [836780, 22.31622, 114.18184, 819692]]], [75.0]]]                 |
      +--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

      root
        |-- RequestAddress: struct (nullable = true)
        |    |-- AddressLine: array (nullable = true)
        |    |    |-- element: string (containsNull = true)
        |-- SuggestedAddress: array (nullable = true)
        |    |-- element: struct (containsNull = true)
        |    |    |-- Address: struct (nullable = true)
        |    |    |    |-- PremisesAddress: struct (nullable = true)
        |    |    |    |    |-- ChiPremisesAddress: struct (nullable = true)
        |    |    |    |    |    |-- BuildingName: string (nullable = true)
        |    |    |    |    |    |-- ChiDistrict: struct (nullable = true)
        |    |    |    |    |    |    |-- DcDistrict: string (nullable = true)
        |    |    |    |    |    |-- ChiEstate: struct (nullable = true)
        |    |    |    |    |    |    |-- EstateName: string (nullable = true)
        |    |    |    |    |    |-- ChiStreet: struct (nullable = true)
        |    |    |    |    |    |    |-- BuildingNoFrom: string (nullable = true)
        |    |    |    |    |    |    |-- StreetName: string (nullable = true)
        |    |    |    |    |    |-- Region: string (nullable = true)
        |    |    |    |    |-- EngPremisesAddress: struct (nullable = true)
        |    |    |    |    |    |-- BuildingName: string (nullable = true)
        |    |    |    |    |    |-- EngDistrict: struct (nullable = true)
        |    |    |    |    |    |    |-- DcDistrict: string (nullable = true)
        |    |    |    |    |    |-- EngEstate: struct (nullable = true)
        |    |    |    |    |    |    |-- EstateName: string (nullable = true)
        |    |    |    |    |    |-- EngStreet: struct (nullable = true)
        |    |    |    |    |    |    |-- BuildingNoFrom: string (nullable = true)
        |    |    |    |    |    |    |-- StreetName: string (nullable = true)
        |    |    |    |    |    |-- Region: string (nullable = true)
        |    |    |    |    |-- GeoAddress: string (nullable = true)
        |    |    |    |    |-- GeospatialInformation: struct (nullable = true)
        |    |    |    |    |    |-- Easting: string (nullable = true)
        |    |    |    |    |    |-- Latitude: string (nullable = true)
        |    |    |    |    |    |-- Longitude: string (nullable = true)
        |    |    |    |    |    |-- Northing: string (nullable = true)
        |    |    |-- ValidationInformation: struct (nullable = true)
        |    |    |    |-- Score: double (nullable = true)
    ```

3.  Explode Array to Structure

    ```python
      # Explode Array to Structure
      explodeArrarDF = readComplexJSONDF.withColumn('Explode_SuggestedAddress', F.explode(F.col('SuggestedAddress'))).drop('SuggestedAddress')
      explodeArrarDF.printSchema()
      explodeArrarDF.show()

      # Read location and name
      dfReadSpecificStructure = explodeArrarDF.select("Explode_SuggestedAddress.Address.PremisesAddress.ChiPremisesAddress.BuildingName",
                                                      "Explode_SuggestedAddress.Address.PremisesAddress.ChiPremisesAddress.ChiDistrict.*",
                                                      "Explode_SuggestedAddress.Address.PremisesAddress.ChiPremisesAddress.ChiEstate.*",
                                                      "Explode_SuggestedAddress.Address.PremisesAddress.ChiPremisesAddress.ChiStreet.*",
                                                      "Explode_SuggestedAddress.Address.PremisesAddress.ChiPremisesAddress.Region",
                                                      "Explode_SuggestedAddress.Address.PremisesAddress.GeospatialInformation.*",
                                                      "Explode_SuggestedAddress.ValidationInformation.*") 
      dfReadSpecificStructure.show(truncate=False)
    ```

    Output
    ```shell
      root
        |-- RequestAddress: struct (nullable = true)
        |    |-- AddressLine: array (nullable = true)
        |    |    |-- element: string (containsNull = true)
        |-- Explode_SuggestedAddress: struct (nullable = true)
        |    |-- Address: struct (nullable = true)
        |    |    |-- PremisesAddress: struct (nullable = true)
        |    |    |    |-- ChiPremisesAddress: struct (nullable = true)
        |    |    |    |    |-- BuildingName: string (nullable = true)
        |    |    |    |    |-- ChiDistrict: struct (nullable = true)
        |    |    |    |    |    |-- DcDistrict: string (nullable = true)
        |    |    |    |    |-- ChiEstate: struct (nullable = true)
        |    |    |    |    |    |-- EstateName: string (nullable = true)
        |    |    |    |    |-- ChiStreet: struct (nullable = true)
        |    |    |    |    |    |-- BuildingNoFrom: string (nullable = true)
        |    |    |    |    |    |-- StreetName: string (nullable = true)
        |    |    |    |    |-- Region: string (nullable = true)
        |    |    |    |-- EngPremisesAddress: struct (nullable = true)
        |    |    |    |    |-- BuildingName: string (nullable = true)
        |    |    |    |    |-- EngDistrict: struct (nullable = true)
        |    |    |    |    |    |-- DcDistrict: string (nullable = true)
        |    |    |    |    |-- EngEstate: struct (nullable = true)
        |    |    |    |    |    |-- EstateName: string (nullable = true)
        |    |    |    |    |-- EngStreet: struct (nullable = true)
        |    |    |    |    |    |-- BuildingNoFrom: string (nullable = true)
        |    |    |    |    |    |-- StreetName: string (nullable = true)
        |    |    |    |    |-- Region: string (nullable = true)
        |    |    |    |-- GeoAddress: string (nullable = true)
        |    |    |    |-- GeospatialInformation: struct (nullable = true)
        |    |    |    |    |-- Easting: string (nullable = true)
        |    |    |    |    |-- Latitude: string (nullable = true)
        |    |    |    |    |-- Longitude: string (nullable = true)
        |    |    |    |    |-- Northing: string (nullable = true)
        |    |-- ValidationInformation: struct (nullable = true)
        |    |    |-- Score: double (nullable = true)

      +--------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
      |RequestAddress|  Explode_SuggestedAddress                                                                                                                                                                                                                        |
      +--------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
      |[[何文田邨]]   |  [[[[何文田政府合署, [九龍城區], [何文田邨], [88, 忠孝街], 九龍], [HO MAN TIN GOVERNMENT OFFICES, [KOWLOON CITY DISTRICT], [HO MAN TIN ESTATE], [88, CHUNG HAU STREET], KLN], 3658519520T20050430, [836597, 22.31468, 114.18007, 819521]]], [75.0]]     |
      |[[何文田邨]]   |  [[[[何文田政府合署, [九龍城區], [何文田邨], [68, 佛光街], 九龍], [HO MAN TIN GOVERNMENT OFFICES, [KOWLOON CITY DISTRICT], [HO MAN TIN ESTATE], [68, FAT KWONG STREET], KLN], 3658519520T20050430, [836597, 22.31468, 114.18007, 819521]]], [75.0]]     |
      |[[何文田邨]]   |  [[[[何文田廣場, [九龍城區], [何文田邨], [80, 佛光街], 九龍], [HOMANTIN PLAZA, [KOWLOON CITY DISTRICT], [HO MAN TIN ESTATE], [80, FAT KWONG STREET], KLN], 3677919691P20060311, [836780, 22.31622, 114.18184, 819692]]], [75.0]]                       |
      +--------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

      +--------------+----------+----------+--------------+----------+------+-------+--------+---------+--------+-----+
      |BuildingName  |DcDistrict|EstateName|BuildingNoFrom|StreetName|Region|Easting|Latitude|Longitude|Northing|Score|
      +--------------+----------+----------+--------------+----------+------+-------+--------+---------+--------+-----+
      |何文田政府合署   |九龍城區   |何文田邨    |88           |忠孝街     |九龍   |836597 |22.31468|114.18007|819521  |75.0 |
      |何文田政府合署   |九龍城區   |何文田邨    |68           |佛光街     |九龍   |836597 |22.31468|114.18007|819521  |75.0 |
      |何文田廣場      |九龍城區   |何文田邨    |80           |佛光街     |九龍   |836780 |22.31622|114.18184|819692  |75.0 |
      +--------------+----------+----------+--------------+----------+------+-------+--------+---------+--------+-----+
    ```

## [NOT GOOD] ~~Read `JSON` string to pySpark dataframe~~

### Read `JSON` to spark Dataframe first

**https://sparkbyexamples.com/pyspark/pyspark-maptype-dict-examples/**

Steps,
1. JSON from API 
2. get `list of dict` 
3. pySpark dataframe with `map type` 
4. access PySpark MapType Elements

### Details

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

    # same as above
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

## Use `.` and `:` syntax to query nested data

  Data, nested JSON in `value`:
  ```
  +-------------------+---------------------------------------------------------------------------------------------------------------------------+
  |                key|                                                                                                                      value|
  +-------------------+---------------------------------------------------------------------------------------------------------------------------+
  | UA000000107384208 | {"device":"macOS","ecommerce":{},"event_name":"checkout","event_previous_timestamp":1593880801027797,"event_timestamp":1593880822506642,"geo":{"city":"Traverse City","state":"MI"},"items":[{"item_id":"M_STAN_T","item_name":"Standard Twin Mattress","item_revenue_in_usd":595.0,"price_in_usd":595.0,"quantity":1}],"traffic_source":"google","user_first_touch_timestamp":1593879413256859,"user_id":"UA000000107384208"}
  | UA000000107388621 | {"device":"Windows","ecommerce":{},"event_name":"email_coupon","event_previous_timestamp":1593880770092554,"event_timestamp":1593880829320848,"geo":{"city":"Hickory","state":"NC"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_F","item_name":"Standard Full Mattress","item_revenue_in_usd":850.5,"price_in_usd":945.0,"quantity":1}],"traffic_source":"direct","user_first_touch_timestamp":1593879889503719,"user_id":"UA000000107388621"}
  | UA000000106459577 | {"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1047.6,"total_item_quantity":2,"unique_items":2},"event_name":"finalize","event_previous_timestamp":1593879787820475,"event_timestamp":1593879948830076,"geo":{"city":"Huntington Park","state":"CA"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_Q","item_name":"Standard Queen Mattress","item_revenue_in_usd":940.5,"price_in_usd":1045.0,"quantity":1},{"coupon":"NEWBED10","item_id":"P_DOWN_S","item_name":"Standard Down Pillow","item_revenue_in_usd":107.10000000000001,"price_in_usd":119.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593583891412316,"user_id":"UA000000106459577"}|
  +-------------------+---------------------------------------------------------------------------------------------------------------------------+
  ```

  SQL
  * Use : syntax in queries to access subfields in JSON strings
  * Use . syntax in queries to access subfields in struct types
  ```SQL
  SELECT * FROM events_strings WHERE value:event_name = "finalize" ORDER BY key LIMIT 1
  ```

  OR Python:
  ```python
  display(events_stringsDF
      .where("value:event_name = 'finalize'")
      .orderBy("key")
      .limit(1)
  )
  ```

  Output:
  ```
  +-------------------+---------------------------------------------------------------------------------------------------------------------------+
  |                key|                                                                                                                      value|
  +-------------------+---------------------------------------------------------------------------------------------------------------------------+
  | UA000000106459577 | {"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1047.6,"total_item_quantity":2,"unique_items":2},"event_name":"finalize","event_previous_timestamp":1593879787820475,"event_timestamp":1593879948830076,"geo":{"city":"Huntington Park","state":"CA"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_Q","item_name":"Standard Queen Mattress","item_revenue_in_usd":940.5,"price_in_usd":1045.0,"quantity":1},{"coupon":"NEWBED10","item_id":"P_DOWN_S","item_name":"Standard Down Pillow","item_revenue_in_usd":107.10000000000001,"price_in_usd":119.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593583891412316,"user_id":"UA000000106459577"}|
  +-------------------+---------------------------------------------------------------------------------------------------------------------------+
  ```


# Deal with `.parquet`

[ xxx ]

* https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/

## What is `.parquet`?

==> columnar data format. Convenient for data analysis, because program just reads a whole column data instead of scanning all rows.

# Spark Dataframe

## Create sparkdf by reading `.csv`

### Normal read

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

### Read range of file names

[Reference](https://docs.microsoft.com/en-us/azure/databricks/kb/scala/pattern-match-files-in-path)

Generate example .csv:
```python
values = [
"""Id,RecordNumber,Zipcode,ZipCodeType,State
{Id},99999,704,STANDARD,PR
{Id},999999,563,STANDARD,PR
""",
"""Id,RecordNumber,Zipcode,ZipCodeType,State
{Id},99999,704,STANDARD,PR
{Id},999999,563,STANDARD,PR
""",
"""Id,RecordNumber,Zipcode,ZipCodeType,State
{Id},99999,704,STANDARD,PR
{Id},999999,563,STANDARD,PR
""",
]

for i,value in enumerate(values):
    for j,value in enumerate(values):
        with open(f'file_{i}{j}.csv', 'w') as f:
            f.write(value.format(Id=f'id_{i}{j}'))
            # f.write('\n')
```

```shell
$ ls -l
-rw-r--r--  1 root root        99 Jul  8 03:43 file_00.csv
-rw-r--r--  1 root root        99 Jul  8 03:43 file_01.csv
-rw-r--r--  1 root root        99 Jul  8 03:43 file_02.csv
-rw-r--r--  1 root root        99 Jul  8 03:43 file_10.csv
-rw-r--r--  1 root root        99 Jul  8 03:43 file_11.csv
-rw-r--r--  1 root root        99 Jul  8 03:43 file_12.csv
-rw-r--r--  1 root root        99 Jul  8 03:43 file_20.csv
-rw-r--r--  1 root root        99 Jul  8 03:43 file_21.csv
-rw-r--r--  1 root root        99 Jul  8 03:43 file_22.csv
```

`file_00.csv`:
```
Id,RecordNumber,Zipcode,ZipCodeType,State
id_00,99999,704,STANDARD,PR
id_00,999999,563,STANDARD,PR
```

`file_01.csv`:
```
Id,RecordNumber,Zipcode,ZipCodeType,State
id_01,99999,704,STANDARD,PR
id_01,999999,563,STANDARD,PR
```

`file_10.csv`:
```
Id,RecordNumber,Zipcode,ZipCodeType,State
id_10,99999,704,STANDARD,PR
id_10,999999,563,STANDARD,PR
```

`file_21.csv`:
```
Id,RecordNumber,Zipcode,ZipCodeType,State
id_21,99999,704,STANDARD,PR
id_21,999999,563,STANDARD,PR
```

#### Character range `[a-b]` read

`[a-b]` - The character class matches a single character in the range of values. It is represented by the range of characters you want to match inside a set of brackets.

Reason of `file_1x.csv`,`file2x.csv` are included: 

`1x` and `2x` match `1` and `2` in `[0-2]`

```python
filename = "file_[0-2]*.csv"
print(filename)

dfFromTxt = (spark
             .read.option("header",True)
             .csv(filename) #load csv
)

dfFromTxt.printSchema()
dfFromTxt.show(truncate=False)
```

```shell
file_[0-2]*.csv
root
 |-- Id: string (nullable = true)
 |-- RecordNumber: string (nullable = true)
 |-- Zipcode: string (nullable = true)
 |-- ZipCodeType: string (nullable = true)
 |-- State: string (nullable = true)

+-----+------------+-------+-----------+-----+
|Id   |RecordNumber|Zipcode|ZipCodeType|State|
+-----+------------+-------+-----------+-----+
|id_00|99999       |704    |STANDARD   |PR   |
|id_00|999999      |563    |STANDARD   |PR   |
|id_01|99999       |704    |STANDARD   |PR   |
|id_01|999999      |563    |STANDARD   |PR   |
|id_02|99999       |704    |STANDARD   |PR   |
|id_02|999999      |563    |STANDARD   |PR   |
|id_10|99999       |704    |STANDARD   |PR   |
|id_10|999999      |563    |STANDARD   |PR   |
|id_11|99999       |704    |STANDARD   |PR   |
|id_11|999999      |563    |STANDARD   |PR   |
|id_12|99999       |704    |STANDARD   |PR   |
|id_12|999999      |563    |STANDARD   |PR   |
|id_20|99999       |704    |STANDARD   |PR   |
|id_20|999999      |563    |STANDARD   |PR   |
|id_21|99999       |704    |STANDARD   |PR   |
|id_21|999999      |563    |STANDARD   |PR   |
|id_22|99999       |704    |STANDARD   |PR   |
|id_22|999999      |563    |STANDARD   |PR   |
+-----+------------+-------+-----------+-----+
```

#### Alternation `{a,b,c}` read

```python 
filename = "file_{00,10}*.csv"
print(filename)

dfFromTxt = (spark
             .read.option("header",True)
             .csv(filename) #load csv
)

dfFromTxt.printSchema()
dfFromTxt.show(truncate=False)
```

```shell
file_{00,10}*.csv
root
 |-- Id: string (nullable = true)
 |-- RecordNumber: string (nullable = true)
 |-- Zipcode: string (nullable = true)
 |-- ZipCodeType: string (nullable = true)
 |-- State: string (nullable = true)

+-----+------------+-------+-----------+-----+
|Id   |RecordNumber|Zipcode|ZipCodeType|State|
+-----+------------+-------+-----------+-----+
|id_00|99999       |704    |STANDARD   |PR   |
|id_00|999999      |563    |STANDARD   |PR   |
|id_10|99999       |704    |STANDARD   |PR   |
|id_10|999999      |563    |STANDARD   |PR   |
+-----+------------+-------+-----------+-----+
```


## [ing] Speed Up Reading .csv/.json with schema

Reading .csv/.json by a pre-defined schema can speed up data import, because Spark doesn't need to scan values in each column/attribute to auto-build the schema based on data.

* [Using schemas to speed up reading into Spark DataFrames](https://t-redactyl.io/blog/2020/08/using-schemas-to-speed-up-reading-into-spark-dataframes.html)
* [Spark read JSON with or without schema](https://sparkbyexamples.com/spark/spark-read-json-with-schema/)


## convert Map, Array, or Struct Type Columns into JSON Strings

[reference](https://azurelib.com/how-to-convert-map-array-struct-type-into-json-string-in-pyspark-dataframe-in-azure-databricks)

### Exaplme Data
```python
from pyspark.sql.session import SparkSession

spark = SparkSession.builder 
    .master("local[*]") 
    .appName("azurelib.com") 
    .getOrCreate()

sc = spark.sparkContext

data = [
    {"id": 1, "name": {"first_name": "Etta", "last_name": "Burrel"}, "details": [{"gender": "Female"}, {"age": "46"}], "preferences": ["District of Columbia", "Colorado"]},
    {"id": 2, "name": {"first_name": "Ky", "last_name": "Fiddyment"}, "details": [{"gender": "Male"}, {"age": "35"}], "preferences": ["California", "Massachusetts"]},
    {"id": 3, "name": {"first_name": "Rod", "last_name": "Meineken"}, "details": [{"gender": "Male"}, {"age": "50"}], "preferences": ["North Carolina", "Minnesota"]},
    {"id": 4, "name": {"first_name": "Selestina", "last_name": "Ley"}, "details": [{"gender": "Female"}, {"age": "47"}], "preferences": ["Michigan", "Pennsylvania"]},
    {"id": 5, "name": {"first_name": "Alvan", "last_name": "Shee"}, "details": [{"gender": "Male"}, {"age": "34"}], "preferences": ["Montana", "California"]}
]

df = spark.createDataFrame(data).select("id", "name", "details", "preferences")
df.printSchema()
df.show(truncate=False)

"""
root
 |-- id: long (nullable = true)
 |-- name: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
 |-- details: array (nullable = true)
 |    |-- element: map (containsNull = true)
 |    |    |-- key: string
 |    |    |-- value: string (valueContainsNull = true)
 |-- preferences: array (nullable = true)
 |    |-- element: string (containsNull = true)

+---+-------------------------------------------+---------------------------------+--------------------------------+
|id |name                                       |details                          |preferences                     |
+---+-------------------------------------------+---------------------------------+--------------------------------+
|1  |{last_name -> Burrel, first_name -> Etta}  |[{gender -> Female}, {age -> 46}]|[District of Columbia, Colorado]|
|2  |{last_name -> Fiddyment, first_name -> Ky} |[{gender -> Male}, {age -> 35}]  |[California, Massachusetts]     |
|3  |{last_name -> Meineken, first_name -> Rod} |[{gender -> Male}, {age -> 50}]  |[North Carolina, Minnesota]     |
|4  |{last_name -> Ley, first_name -> Selestina}|[{gender -> Female}, {age -> 47}]|[Michigan, Pennsylvania]        |
|5  |{last_name -> Shee, first_name -> Alvan}   |[{gender -> Male}, {age -> 34}]  |[Montana, California]           |
+---+-------------------------------------------+---------------------------------+--------------------------------+
"""
```

### `Map` / `MapType` Column to JSON StringType

```python
from pyspark.sql.functions import to_json

df1 = df.select("name", to_json("name").alias("str_name"))
df1.printSchema()
df1.show(truncate=False)

"""
Output:

root
 |-- name: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
 |-- str_name: string (nullable = true)

+-------------------------------------------+--------------------------------------------+
|name                                       |str_name                                    |
+-------------------------------------------+--------------------------------------------+
|{last_name -> Burrel, first_name -> Etta}  |{"last_name":"Burrel","first_name":"Etta"}  |
|{last_name -> Fiddyment, first_name -> Ky} |{"last_name":"Fiddyment","first_name":"Ky"} |
|{last_name -> Meineken, first_name -> Rod} |{"last_name":"Meineken","first_name":"Rod"} |
|{last_name -> Ley, first_name -> Selestina}|{"last_name":"Ley","first_name":"Selestina"}|
|{last_name -> Shee, first_name -> Alvan}   |{"last_name":"Shee","first_name":"Alvan"}   |
+-------------------------------------------+--------------------------------------------+

"""
```


### `List of MapType` column into JSON StringType

```python
from pyspark.sql.functions import to_json

df2 = df.select("details", to_json("details").alias("str_details"))
df2.printSchema()
df2.show(truncate=False)

"""
Output:

root
 |-- details: array (nullable = true)
 |    |-- element: map (containsNull = true)
 |    |    |-- key: string
 |    |    |-- value: string (valueContainsNull = true)
 |-- str_details: string (nullable = true)

+---------------------------------+----------------------------------+
|details                          |str_details                       |
+---------------------------------+----------------------------------+
|[{gender -> Female}, {age -> 46}]|[{"gender":"Female"},{"age":"46"}]|
|[{gender -> Male}, {age -> 35}]  |[{"gender":"Male"},{"age":"35"}]  |
|[{gender -> Male}, {age -> 50}]  |[{"gender":"Male"},{"age":"50"}]  |
|[{gender -> Female}, {age -> 47}]|[{"gender":"Female"},{"age":"47"}]|
|[{gender -> Male}, {age -> 34}]  |[{"gender":"Male"},{"age":"34"}]  |
+---------------------------------+----------------------------------+

"""
```

### `ArrayType` column into JSON StringType

```python
from pyspark.sql.functions import to_json

df3 = df.select("preferences", to_json("preferences").alias("str_preferences"))
df3.printSchema()
df3.show(truncate=False)

"""
Output:

root
 |-- preferences: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- str_preferences: string (nullable = true)

+--------------------------------+-----------------------------------+
|preferences                     |str_preferences                    |
+--------------------------------+-----------------------------------+
|[District of Columbia, Colorado]|["District of Columbia","Colorado"]|
|[California, Massachusetts]     |["California","Massachusetts"]     |
|[North Carolina, Minnesota]     |["North Carolina","Minnesota"]     |
|[Michigan, Pennsylvania]        |["Michigan","Pennsylvania"]        |
|[Montana, California]           |["Montana","California"]           |
+--------------------------------+-----------------------------------+

"""
```


## Change Column Type in a StructType

Changing `phone_number` column (under Struct `info`) type to `Interger` from `String`

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Sample data
data = [
    (("John", "A", "Doe"), ("1234567890", "123 Main St", "30"), "001", "M", 3000),
    (("Jane", "B", "Smith"), ("0987654321", "456 Elm St", "25"), "002", "F", 4000)
]

# Original schema
structureSchema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('info', StructType([
        StructField('phone_number', StringType(), True),
        StructField('address', StringType(), True),
        StructField('age', StringType(), True)
    ])),
    StructField('id', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema=structureSchema)

df.printSchema()
df.show(truncate=False)

"""
root
 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- info: struct (nullable = true)
 |    |-- phone_number: string (nullable = true)
 |    |-- address: string (nullable = true)
 |    |-- age: string (nullable = true)
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)

+----------------+-----------------------------+---+------+------+
|name            |info                         |id |gender|salary|
+----------------+-----------------------------+---+------+------+
|{John, A, Doe}  |{1234567890, 123 Main St, 30}|001|M     |3000  |
|{Jane, B, Smith}|{0987654321, 456 Elm St, 25} |002|F     |4000  |
+----------------+-----------------------------+---+------+------+
"""
```

Change type:
```python
# Change the type of 'age' column from StringType to IntegerType within the 'info' struct
df = df.withColumn("info", col("info").withField("age", col("info.age").cast(IntegerType())))

# Show the updated DataFrame
df.printSchema()
df.show(truncate=False)

"""
root
 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- info: struct (nullable = true)
 |    |-- phone_number: string (nullable = true)
 |    |-- address: string (nullable = true)
 |    |-- age: integer (nullable = true)
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)

+----------------+-----------------------------+---+------+------+
|name            |info                         |id |gender|salary|
+----------------+-----------------------------+---+------+------+
|{John, A, Doe}  |{1234567890, 123 Main St, 30}|001|M     |3000  |
|{Jane, B, Smith}|{0987654321, 456 Elm St, 25} |002|F     |4000  |
+----------------+-----------------------------+---+------+------+
"""
```


## Merge/Union Two DataFrames with Different Columns or Schema

### (1) `unionByName(allowMissingColumns=True)`
[pyspark native function](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.unionByName.html)

  ```python
  df1 = spark.createDataFrame([[1, 2, 3]], ["col0", "col1", "col2"])
  df2 = spark.createDataFrame([[4, 5, 6]], ["col1", "col2", "col3"])
  df1.unionByName(df2, allowMissingColumns=True).show()
  ```

  Output:
  ```shell
  +----+----+----+----+
  |col0|col1|col2|col3|
  +----+----+----+----+
  |   1|   2|   3|null|
  |null|   4|   5|   6|
  +----+----+----+----+
  ```

### (2) Create missing columns manually

[reference](https://www.geeksforgeeks.org/pyspark-merge-two-dataframes-with-different-columns-or-schema/)

Steps,
  1. Create the missing columns for both dataframes and filled them will NULL
  2. UNION two dataframes

Step 1 - Create the missing columns:
  ```python
  import pyspark.sql.functions as F

  data = [["1", "sravan", "kakumanu"], 
          ["2", "ojaswi", "hyd"], 
          ["3", "rohith", "delhi"], 
          ["4", "sridevi", "kakumanu"], 
          ["5", "bobby", "guntur"]] 
  columns = ['ID', 'NAME', 'Address'] 
  dataframe1 = spark.createDataFrame(data, columns) 
    
  data = [["1", 23], 
          ["2", 21], 
          ["3", 32], 
          ] 
  columns = ['ID', 'Age'] 
  dataframe2 = spark.createDataFrame(data, columns) 

  overall_columns = set(
      [*df_sdu_suppliers.columns, *df_sdu_buyers.columns, *df_crunchbase.columns]
  )
  print(len(overall_columns), overall_columns)
  ```

  ```python
  def createMissingDfColumns(df: DataFrame, target_columns:list):
    _columns = list(filter(lambda _col: _col not in df.columns, target_columns))
    print(_columns)
    for column in _columns: 
        df = df.withColumn(column, F.lit(None)) 
    return df.select(*target_columns)

  dataframe1_target = createMissingDfColumns(df=dataframe1, target_columns=overall_columns)
  dataframe2_target = createMissingDfColumns(df=dataframe2, target_columns=overall_columns)
  ```


## Rename Columns

### (1.1) Built-in `withColumnsRenamed()` (New in version 3.4.0)

https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumnsRenamed.html

  ```python
  df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
  df = df.withColumns({'age2': df.age + 2, 'age3': df.age + 3})
  df.withColumnsRenamed({'age2': 'age4', 'age3': 'age5'}).show()
  ```

  Output:
  ```
  +---+-----+----+----+
  |age| name|age4|age5|
  +---+-----+----+----+
  |  2|Alice|   4|   5|
  |  5|  Bob|   7|   8|
  +---+-----+----+----+
  ```

### (1.2) Built-in `withColumnRenamed()`

[Ref](https://sparkbyexamples.com/pyspark/pyspark-rename-dataframe-column/)

```python
df2 = df.withColumnRenamed(existingName, newNam)
```

### (2) `SELECT` method, `df.select(*[F.col(old_name).alias("new_name") for old_name in rename_map])`

Reference: [PySpark - rename more than one column using withColumnRenamed](https://stackoverflow.com/questions/38798567/pyspark-rename-more-than-one-column-using-withcolumnrenamed)

Code:
```python
def renameColumns(df, mapping):
    '''df: PySpark DataFrame. Return PySpark DataFrame '''

    if isinstance(mapping, dict):
        '''mapping.get(old_name, default_name)
           D.get(k[,d]) -> D[k] if k in D, else d.  d defaults to None.'''
        return df.select(*[F.col(col_name).alias(mapping.get(col_name, col_name)) for col_name in df.columns])
    else:
        raise ValueError("'mapping' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")
```

Explain:
```python
mapping = {"old": "new"}

print(mapping.get('old', 'default'))
print(mapping.get('xxx', 'default'))

# ==== output
# new
# default
```

### Lowercase all column names

[reference](https://stackoverflow.com/questions/43005744/convert-columns-of-pyspark-data-frame-to-lowercase/43005939#43005939)

```python
import pyspark.sql.functions as F

df = xxxx
# lowercase all column names
df = df.toDF(*[c.lower() for c in df.columns])
```

### Lowercase values in all columns

[reference](performing-operations-on-multiple-columns-in-a-pyspark-dataframe-36e97896c378)

```python
import pyspark.sql.functions as F

df = xxxx
df = df.select(*[F.lower(F.col(col)).name(col) for col in df.columns])

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

## Deal with `datetime` / `timestamp`
### `F.unix_timestamp()`, convert timestamp `string` with custom format to `datetime object`

==> Reference:  https://stackoverflow.com/questions/54951348/pyspark-milliseconds-of-timestamp/54961415#54961415

```python
df.withColumn('new_timestamp', F.unix_timestamp('timestamp_str_with_custom_format', format=timeFmt))
```

Example:
```python
import pyspark.sql.functions as F

timeFmt = "yyyy-MM-dd't'HH:mm:ss.SSS"
data = [
    (1, '2018-07-25t17:15:06.390', '1532538906390'),  # note the '390'
    (2, '2018-07-25t11:12:48.883', '1532560368883')
]

df = spark.createDataFrame(data, ['ID', 'timestamp_string', 'timestamp'])

df = df.withColumn('timestamp_string_1', F.unix_timestamp('timestamp_string', format=timeFmt))\
    .withColumn('timestamp_string_2', (F.col("timestamp_string_1")).cast(TimestampType()))\
    .withColumn('timestamp_2', (F.col("timestamp") / 1000).cast(TimestampType()))\
    .select('timestamp', 'timestamp_2', 'timestamp_string', 'timestamp_string_1', 'timestamp_string_2')

df.show(truncate=False)
df.printSchema()
```

Output:
```shell
+-------------+-----------------------+-----------------------+------------------+-------------------+
|timestamp    |timestamp_2            |timestamp_string       |timestamp_string_1|timestamp_string_2 |
+-------------+-----------------------+-----------------------+------------------+-------------------+
|1532538906390|2018-07-26 01:15:06.39 |2018-07-25t17:15:06.390|1532510106        |2018-07-25 17:15:06|
|1532560368883|2018-07-26 07:12:48.883|2018-07-25t11:12:48.883|1532488368        |2018-07-25 11:12:48|
+-------------+-----------------------+-----------------------+------------------+-------------------+

root
 |-- timestamp: string (nullable = true)
 |-- timestamp_2: timestamp (nullable = true)
 |-- timestamp_string: string (nullable = true)
 |-- timestamp_string_1: long (nullable = true)
 |-- timestamp_string_2: timestamp (nullable = true)
```
### Change time zone

Example:
```python
import pyspark.sql.functions as F

timeFmt = "yyyy-MM-dd't'HH:mm:ss.SSS"
data = [
    (1, '2018-07-25t17:15:06.390', '1532538906390'),  # note the '390'
    (2, '2018-07-25t11:12:48.883', '1532560368883')
]

df = spark.createDataFrame(data, ['ID', 'timestamp_string', 'timestamp'])

### change datetime from current timestamp
df = df.withColumn("hk_timezone", F.from_utc_timestamp(F.current_timestamp(),"Asia/Hong_Kong"))

### method 2
df = df.withColumn("existing_datetime_hk_timezone", F.from_utc_timestamp("existing_datetime", "Asia/Hong_Kong"))
```

### Get day of week from `datetime` / `timestamp`
https://stackoverflow.com/questions/38928919/how-to-get-the-weekday-from-day-of-month-using-pyspark/68018862#68018862

Code:
```python
import pyspark.sql.functions as F
df = df.withColumn('day_of_week', ((F.dayofweek("yyyymmddhh start")+5)%7)+1)
df = df.withColumn('is_weekday', ((F.dayofweek("yyyymmddhh start")+5)%7)+1 < 6)
```

Result:
```python
df.printSchema()
df.select("journey id", "yyyymmddhh start", "day_of_week", "is_weekday").show()
```

```shell
root
 |-- journey id: string (nullable = true)
 |-- yyyymmddhh start: timestamp (nullable = true)
 |-- yyyymmddhh end: timestamp (nullable = true)
 |-- day_of_week: integer (nullable = true)
 |-- is_weekday: boolean (nullable = true)

+----------+-------------------+-----------+----------+
|journey id|   yyyymmddhh start|day_of_week|is_weekday|
+----------+-------------------+-----------+----------+
|       953|2017-09-19 17:26:00|          2|      true|
|     14659|2017-09-13 19:13:00|          3|      true|
|      2351|2017-09-14 14:31:00|          4|      true|
|      7252|2017-09-17 17:00:00|          7|     false|
|      9782|2017-09-17 17:00:00|          7|     false|
|     13500|2017-09-15 13:33:00|          5|      true|
|     11205|2017-09-15 15:47:00|          5|      true|
+----------+-------------------+-----------+----------+
```

## `F.create_map()` in `df.withColumn()`, create a `dict` column

https://sparkbyexamples.com/pyspark/pyspark-convert-dataframe-columns-to-maptype-dict/

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [ ("36636","Finance",3000,"USA"), 
    ("40288","Finance",5000,"IND"), 
    ("42114","Sales",3900,"USA"), 
    ("39192","Marketing",2500,"CAN"), 
    ("34534","Sales",6500,"USA") ]
schema = StructType([
     StructField('id', StringType(), True),
     StructField('dept', StringType(), True),
     StructField('salary', IntegerType(), True),
     StructField('location', StringType(), True)
     ])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)
```

```shell
root
 |-- id: string (nullable = true)
 |-- dept: string (nullable = true)
 |-- salary: integer (nullable = true)
 |-- location: string (nullable = true)

+-----+---------+------+--------+
|id   |dept     |salary|location|
+-----+---------+------+--------+
|36636|Finance  |3000  |USA     |
|40288|Finance  |5000  |IND     |
|42114|Sales    |3900  |USA     |
|39192|Marketing|2500  |CAN     |
|34534|Sales    |6500  |USA     |
+-----+---------+------+--------+
```

```python
#Convert columns to Map
from pyspark.sql.functions import col,lit,create_map
df = df.withColumn("propertiesMap",create_map(
        lit("salary"),col("salary"),
        lit("location"),col("location")
        )).drop("salary","location")
df.printSchema()
df.show(truncate=False)
```

```shell 
root
 |-- id: string (nullable = true)
 |-- dept: string (nullable = true)
 |-- propertiesMap: map (nullable = false)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)

+-----+---------+-----------------------------------+
|id   |dept     |propertiesMap                      |
+-----+---------+-----------------------------------+
|36636|Finance  |{'salary': 3000, 'location': 'USA'}|
|40288|Finance  |{'salary': 5000, 'location': 'IND'}|
|42114|Sales    |{'salary': 3900, 'location': 'USA'}|
|39192|Marketing|{'salary': 2500, 'location': 'CAN'}|
|34534|Sales    |{'salary': 6500, 'location': 'USA'}|
+-----+---------+-----------------------------------+
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

## `groupBy().agg()`

[ xxxx ]

1. https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html
2. https://spark.apache.org/docs/2.4.4/sql-pyspark-pandas-with-arrow.html#grouped-map

## `groupBy().collect_set()` / `groupBy().collect_list()`

[ xxx ]

1. https://sparkbyexamples.com/spark/spark-collect-list-and-collect-set-functions/

## Combine array of map to single map, `groupBy().collect_list()`

[reference](https://stackoverflow.com/questions/43723864/combine-array-of-maps-into-single-map-in-pyspark-dataframe/43724338#43724338)

```python
from pyspark.sql.functions import udf,collect_list
from pyspark.sql.types import MapType,StringType

combineMap = udf(lambda maps: {key:f[key] for f in maps for key in f},
                 MapType(StringType(),StringType()))

df.groupBy('id')\
    .agg(collect_list('map')\
    .alias('maps'))\
    .select('id', combineMap('maps').alias('combined_map')).show()
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

## Window functions

https://sparkbyexamples.com/pyspark/pyspark-window-functions/


## `.join()`/`spark.sql()` dataframes
### `.join()`
```python
# join 2 df by `CUSTKEY`
joined_df = dfCustomer.join(dfOrders, on='CUSTKEY', how='leftouter')
df2 = joined_df.select('CUSTKEY', 'ORDERKEY').sort(asc('CUSTKEY'),desc('ORDERKEY')) #ascending by 'CUSTKEY', descending by 'ORDERKET'
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

## UDF `df.withColumn`, user defined function

[Reference](https://sparkbyexamples.com/pyspark/pyspark-udf-user-defined-function/)

### Explicitly define `udf`

https://medium.com/@ayplam/developing-pyspark-udfs-d179db0ccc87

Explicitly define a udf that you can use as a pyspark function.
```python
  from pyspark.sql.types import StringType
  from pyspark.sql.functions import udf, col
  def say_hello(name : str) -> str:
      return f"Hello {name}"
  assert say_hello("Summer") == "Hello Summer"
  say_hello_udf = udf(lambda name: say_hello(name), StringType())
  df = spark.createDataFrame([("Rick,"),("Morty,")], ["name"])
  df.withColumn("greetings", say_hello_udf(col("name"))).show()

  # +------+------------+
  # |  name|   greetings|
  # +------+------------+
  # |  Rick|  Hello Rick|
  # | Morty| Hello Morty|
  # +------+------------+
```
However, this means that for every pyspark UDF, there are two functions to keep track of — a regular python one and another pyspark `_udf` one. For a cleaner pattern, the udf function is also a built in decorator.

### Decorator `@udf`

https://medium.com/@ayplam/developing-pyspark-udfs-d179db0ccc87

```python
  @udf(returnType=StringType())
  def say_hello(name):
      return f"Hello {name}"
  # Below `assert` doesn't work anymore if decorator `@udf` is used
  # assert say_hello("Summer") == "Hello Summer"
  df.withColumn("greetings", say_hello(col("name"))).show()
```

### Custom decorator `@py_or_udf`

https://medium.com/@ayplam/developing-pyspark-udfs-d179db0ccc87

Introducing — `py_or_udf` — a decorator that allows a method to act as either a regular python method or a pyspark UDF

```python
from typing import Callable
from pyspark.sql import Column
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType, ArrayType, DataType

class py_or_udf:
    def __init__(self, returnType : DataType=StringType()):
        self.spark_udf_type = returnType
        
    def __call__(self, func : Callable):
        def wrapped_func(*args, **kwargs):
            if any([isinstance(arg, Column) for arg in args]) or \
                any([isinstance(vv, Column) for vv in kwargs.values()]):
                return udf(func, self.spark_udf_type)(*args, **kwargs)
            else:
                return func(*args, **kwargs)
            
        return wrapped_func

@py_or_udf(returnType=StringType())
def say_hello(name):
     return f"Hello {name}"
# This works
assert say_hello("world") == "Hello world"
# This also works
df.withColumn("greeting", say_hello(col("name"))).show()
```


## `melt()` - Wide to long
* https://stackoverflow.com/questions/41670103/how-to-melt-spark-dataframe
* https://gist.github.com/korkridake/972e315e5ce094096e17c6ad1ef599fd

Code example:
```python
from pyspark.sql.functions import array, col, explode, lit, struct
from pyspark.sql import DataFrame
from typing import Iterable 

def melt(
        df: DataFrame, 
        id_vars: Iterable[str], value_vars: Iterable[str], 
        var_name: str="variable", value_name: str="value") -> DataFrame:
    """
    Convert :class:`DataFrame` from wide to long format.
    Source: https://stackoverflow.com/questions/41670103/how-to-melt-spark-dataframe
    """

    # -------------------------------------------------------------------------------
    # Create array<struct<variable: str, value: ...>>
    # -------------------------------------------------------------------------------
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name)) 
        for c in value_vars))

    # -------------------------------------------------------------------------------
    # Add to the DataFrame and explode
    # -------------------------------------------------------------------------------
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)
    
# -------------------------------------------------------------------------------
# Let's Implement Wide to Long in Pyspark!
# -------------------------------------------------------------------------------
melt(df_web_browsing_full_test, 
     id_vars=['ID_variable'], 
     value_vars=['VALUE_variable_1', 'VALUE_variable_2']
     var_name='variable',
     value_name='value',
     ).show()
```

Result:
```
+-----------+--------+----------------+
|ID_variable|variable|           value|
+-----------+--------+----------------+
| id00000001| 2023-01|             0.0|
| id03947263| 2023-02|       488.49382|
| id58723942| 2023-03|      8644.84643|
| id09474733| 2023-04|      1431.49900|
| id00012398| 2023-05|             0.0|
+-----------+--------+----------------+
```

## Pandas Function API - Normal Pyspark UDF, pandas_udf, applyInPandas, mapInPandas

https://community.databricks.com/t5/technical-blog/understanding-pandas-udf-applyinpandas-and-mapinpandas/ba-p/75717

Example Data,
```python
from pyspark.sql.functions import udf, collect_list, avg
from pyspark.sql.types import DoubleType, ArrayType, IntegerType, LongType
from pyspark.sql.functions import rand, pandas_udf, col
import pandas as pd

def generate_initial_df(num_rows, num_devices, num_trips):
    return (
        spark.range(num_rows)
        .withColumn('device_id', (rand()*num_devices).cast('int'))
        .withColumn('trip_id', (rand()*num_trips).cast('int'))
        .withColumn('sensor_reading', (rand()*1000))
        .drop('id')
    )

df = generate_initial_df(5000000, 10000, 50)
df.orderBy("device_id").show(truncate=False)

""" output
+---------+-------+------------------+
|device_id|trip_id|sensor_reading    |
+---------+-------+------------------+
|0        |17     |200.07186141915702|
|0        |38     |638.7099344992462 |
|0        |19     |942.5320746012178 |
|0        |20     |20.102324196202392|
|0        |0      |417.3651121879528 |
|0        |44     |424.3013093384167 |
|0        |49     |998.0085705148036 |
|0        |39     |237.19837662658406|
|0        |4      |139.5120905811834 |
|0        |15     |990.9374600845076 |
|0        |38     |954.0722887672762 |
|0        |24     |280.13474776039624|
|0        |40     |537.5108315942482 |
|0        |32     |836.6487462519016 |
|0        |30     |607.0676516498482 |
|0        |3      |772.9327004583042 |
|0        |5      |526.6132950210414 |
|0        |25     |394.5216215010717 |
|0        |46     |982.572540274019  |
|0        |45     |115.4914894051453 |
+---------+-------+------------------+
only showing top 20 rows
"""
```

### Normal Pyspark UDF

```python

def calculate_sqrt(sensor_reading):
    return sensor_reading ** 0.9

calculate_sqrt_udf = udf(calculate_sqrt, DoubleType())

_df = df.withColumn('sqrt_reading', calculate_sqrt_udf(col('sensor_reading')))
_df.show(truncate=False)

"""
+---------+-------+------------------+------------------+
|device_id|trip_id|sensor_reading    |sqrt_reading      |
+---------+-------+------------------+------------------+
|807      |11     |303.155046930383  |171.19785640082105|
|462      |37     |168.04397122565152|100.6654971353733 |
|2324     |23     |650.5689308485618 |340.379963201701  |
|9822     |43     |408.5758326136424 |223.9470814553646 |
|8892     |16     |282.79723439206003|160.8154025463611 |
|7908     |14     |96.31487228780422 |60.99918293258891 |
|4167     |31     |737.4119455035295 |381.0124366204807 |
|34       |5      |375.9284965487937 |207.77568513046444|
|6484     |32     |279.59012193469204|159.17308776443187|
|2562     |25     |380.076049281175  |209.8376688138082 |
|2872     |8      |893.8158674890465 |453.0261319595933 |
|4788     |17     |388.30903456471447|213.92411580482394|
|6465     |0      |433.37027916046833|236.14198599166204|
|2956     |37     |859.2930712416693 |437.2473428513343 |
|6914     |35     |313.4830502165571 |176.43821582093645|
|7230     |33     |783.1829762605923 |402.23227463683037|
|6785     |4      |905.4933908060339 |458.3494940321099 |
|1223     |25     |343.5432330887045 |191.59459739681853|
|3386     |35     |689.2770859549281 |358.5539052904503 |
|3110     |48     |711.8998032327357 |369.1279965973804 |
+---------+-------+------------------+------------------+
only showing top 20 rows
"""
```

### pandas_udf

Next, we’ll show an example of using a Pandas UDF to calculate the square root of the sensor_reading column. Once we’ve defined the function and decorated it with @pandas_udf, we can now use it like we would a normal Spark function. Note that this example operates on one column at a time, but Pandas UDFs can be very flexible in the data structures they use. Pandas UDFs are commonly used to return predictions made by machine learning models. For more details and examples, check our documentation. 

```python
@pandas_udf('double')
def calculate_sqrt(sensor_reading: pd.Series) -> pd.Series:
    return sensor_reading.apply(lambda x: x**0.9)

df = df.withColumn('sqrt_reading', calculate_sqrt(col('sensor_reading')))
df.show(truncate=False)

"""
+---------+-------+------------------+------------------+
|device_id|trip_id|sensor_reading    |sqrt_reading      |
+---------+-------+------------------+------------------+
|807      |11     |303.155046930383  |171.19785640082105|
|462      |37     |168.04397122565152|100.6654971353733 |
|2324     |23     |650.5689308485618 |340.379963201701  |
|9822     |43     |408.5758326136424 |223.9470814553646 |
|8892     |16     |282.79723439206003|160.8154025463611 |
|7908     |14     |96.31487228780422 |60.99918293258891 |
|4167     |31     |737.4119455035295 |381.0124366204807 |
|34       |5      |375.9284965487937 |207.77568513046444|
|6484     |32     |279.59012193469204|159.17308776443187|
|2562     |25     |380.076049281175  |209.8376688138082 |
|2872     |8      |893.8158674890465 |453.0261319595933 |
|4788     |17     |388.30903456471447|213.92411580482394|
|6465     |0      |433.37027916046833|236.14198599166204|
|2956     |37     |859.2930712416693 |437.2473428513343 |
|6914     |35     |313.4830502165571 |176.43821582093645|
|7230     |33     |783.1829762605923 |402.23227463683037|
|6785     |4      |905.4933908060339 |458.3494940321099 |
|1223     |25     |343.5432330887045 |191.59459739681853|
|3386     |35     |689.2770859549281 |358.5539052904503 |
|3110     |48     |711.8998032327357 |369.1279965973804 |
+---------+-------+------------------+------------------+
only showing top 20 rows
"""
```

### applyInPandas

Another technique for distributing Pandas operations is applyInPandas. We can use applyInPandas for operations that we want to run on individual groups in parallel, such as by device_id. Common uses include custom aggregations, normalizing per grouping, or training a machine learning model per grouping. In this example, we’ll run a custom aggregation in Pandas which reduces the granularity of our DataFrame down to the device_id column. The trip_id column will be converted into a list of the values per device_id, and the sensor_reading and sqrt_reading columns will be averaged for each device_id. The output will be one row per device_id. Importantly, applyInPandas requires your function to accept and return a Pandas DataFrame, and the schema of the returned DataFrame must be defined ahead of time so that PyArrow can serialize it efficiently. For an example of using applyInPandas to train models for each grouping of some key, check notebook four in this solution accelerator.

```python
def denormalize(pdf: pd.DataFrame) -> pd.DataFrame:
    aggregated_df = pdf.groupby('device_id', as_index=False).agg(
        {'trip_id': lambda x: list(x), 'sensor_reading': 'mean', 'sqrt_reading': 'mean'}
    )
    return aggregated_df

expected_schema = 'device_id int, trip_id array<int>, sensor_reading long, sqrt_reading long'
df = df.groupBy('device_id').applyInPandas(denormalize, schema=expected_schema)
df.show(truncate=False)

"""
+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+------------+
|device_id|trip_id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |sensor_reading|sqrt_reading|
+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+------------+
|31       |[12, 29, 22, 43, 33, 20, 5, 44, 1, 0, 34, 11, 22, 2, 40, 16, 34, 42, 2, 15, 0, 12, 46, 8, 44, 11, 34, 33, 45, 11, 17, 39, 43, 11, 43, 46, 20, 45, 42, 40, 42, 16, 23, 33, 46, 8, 7, 46, 41, 0, 15, 47, 39, 28, 19, 10, 45, 21, 35, 1, 5, 44, 2, 33, 3, 25, 16, 10, 49, 4, 6, 36, 34, 18, 38, 14, 22, 41, 15, 2, 14, 48, 16, 42, 33, 33, 4, 38, 15, 46, 45, 40, 17, 37, 24, 32, 31, 12, 6, 49, 18, 11, 7, 13, 33, 12, 31, 25, 25, 16, 1, 43, 8, 3, 26, 45, 1, 2, 25, 9, 17, 4, 31, 25, 48, 9, 3, 21, 11, 0, 11, 21, 22, 0, 19, 13, 19, 13, 12, 5, 7, 41, 22, 35, 16, 23, 34, 15, 6, 26, 18, 45, 6, 39, 16, 7, 31, 18, 2, 39, 41, 45, 40, 7, 8, 46, 34, 30, 17, 45, 6, 3, 30, 0, 3, 43, 32, 47, 4, 31, 42, 22, 44, 29, 13, 6, 8, 21, 10, 38, 29, 33, 28, 48, 13, 42, 42, 38, 16, 34, 46, 49, 44, 48, 8, 23, 12, 30, 10, 25, 19, 49, 43, 44, 9, 9, 47, 40, 38, 45, 36, 13, 8, 10, 29, 33, 4, 24, 5, 5, 33, 48, 49, 34, 23, 44, 4, 35, 11, 28, 30, 14, 38, 20, 24, 25, 28, 49, 23, 7, 30, 8, 35, 23, 29, 27, 11, 2, 49, 2, 38, 22, 3, 13, 26, 15, 20, 18, 27, 27, 5, 19, 47, 32, 47, 45, 30, 35, 45, 8, 46, 2, 21, 7, 24, 49, 23, 28, 2, 7, 10, 26, 32, 46, 21, 27, 19, 46, 31, 30, 46, 46, 16, 18, 23, 20, 5, 16, 26, 25, 26, 7, 8, 29, 30, 37, 29, 40, 11, 6, 24, 49, 17, 30, 29, 47, 27, 29, 30, 5, 15, 26, 17, 10, 4, 6, 45, 26, 49, 34, 19, 32, 25, 47, 28, 42, 30, 29, 5, 10, 21, 13, 43, 39, 34, 35, 11, 49, 35, 48, 47, 44, 23, 3, 29, 30, 0, 1, 46, 2, 16, 2, 40, 8, 27, 30, 47, 10, 30, 0, 1, 29, 6, 27, 20, 27, 33, 7, 12, 39, 24, 1, 18, 23, 35, 7, 5, 12, 13, 44, 38, 30, 24, 3, 22, 47, 5, 43, 4, 5, 9, 44, 22, 47, 46, 49, 36, 0, 24, 26, 49, 46, 5, 39, 17, 48, 13, 27, 24, 22, 26, 27, 10, 35, 10, 12, 17, 13, 38, 8, 11, 32, 48, 15, 13, 43, 15, 45, 9, 49, 8, 42, 0, 44, 23, 29, 40, 46, 43, 36, 49, 3, 24, 46, 32, 18, 27, 31, 20, 0, 42, 33, 36, 20, 38, 43, 42, 8, 24, 5, 1, 6, 28, 16, 37, 35, 40, 8, 37, 6, 10, 8, 24, 35, 23, 35, 20, 42, 45, 10, 31, 42, 46, 17, 7, 8, 16, 45, 13, 46, 8, 33, 15, 25, 7, 30, 26, 7, 24, 24, 46, 19, 45, 44, 30, 46, 35, 15, 48, 39, 24, 23, 0, 19, 20, 4, 7]|489           |257         |
|34       |[5, 41, 36, 33, 5, 35, 22, 27, 31, 2, 19, 16, 38, 39, 48, 33, 33, 4, 24, 47, 23, 9, 31, 17, 48, 3, 36, 3, 42, 9, 31, 18, 47, 11, 15, 42, 39, 40, 44, 23, 25, 48, 44, 44, 25, 46, 23, 30, 13, 43, 16, 4, 40, 21, 22, 44, 29, 17, 16, 32, 30, 32, 16, 11, 21, 19, 20, 17, 18, 45, 41, 23, 22, 16, 42, 38, 8, 42, 46, 29, 13, 1, 8, 14, 46, 26, 36, 43, 49, 25, 1, 0, 35, 17, 4, 14, 3, 14, 31, 2, 22, 23, 21, 45, 34, 20, 15, 23, 34, 17, 0, 13, 43, 41, 24, 28, 24, 4, 43, 32, 41, 22, 9, 14, 0, 3, 22, 19, 31, 30, 8, 19, 42, 26, 6, 44, 36, 0, 7, 19, 44, 18, 49, 34, 41, 16, 3, 4, 10, 49, 7, 41, 6, 3, 39, 11, 38, 4, 37, 32, 2, 5, 3, 23, 36, 8, 36, 29, 40, 47, 19, 37, 16, 45, 23, 25, 21, 10, 41, 44, 14, 37, 12, 11, 23, 14, 20, 34, 18, 41, 5, 17, 36, 44, 21, 1, 30, 10, 45, 36, 47, 18, 25, 45, 30, 22, 26, 0, 46, 41, 43, 38, 35, 20, 17, 3, 11, 30, 24, 5, 47, 47, 30, 1, 19, 36, 0, 16, 12, 1, 45, 43, 24, 44, 36, 11, 20, 20, 19, 27, 6, 26, 35, 38, 17, 14, 49, 11, 21, 2, 33, 35, 49, 4, 43, 37, 36, 13, 26, 23, 44, 34, 34, 17, 12, 4, 40, 16, 20, 0, 21, 35, 2, 15, 30, 5, 35, 43, 24, 2, 37, 26, 36, 12, 17, 22, 35, 19, 17, 49, 28, 22, 15, 1, 20, 19, 49, 18, 34, 13, 40, 41, 21, 12, 31, 40, 32, 34, 47, 3, 41, 38, 8, 16, 43, 34, 44, 1, 6, 13, 49, 8, 25, 33, 30, 41, 7, 27, 7, 47, 39, 47, 41, 13, 28, 15, 15, 8, 45, 49, 14, 2, 6, 16, 7, 5, 20, 42, 0, 27, 0, 6, 28, 17, 9, 0, 10, 31, 39, 38, 12, 47, 37, 46, 37, 22, 8, 3, 47, 38, 33, 20, 17, 25, 24, 3, 21, 29, 13, 4, 45, 43, 0, 16, 34, 34, 37, 15, 4, 4, 26, 45, 8, 18, 29, 7, 49, 7, 39, 33, 42, 16, 46, 22, 39, 13, 13, 9, 12, 10, 7, 23, 32, 8, 31, 47, 37, 45, 21, 33, 44, 35, 6, 47, 11, 10, 10, 7, 8, 26, 15, 37, 26, 42, 25, 42, 33, 20, 48, 40, 45, 36, 19, 12, 1, 2, 29, 6, 35, 36, 39, 7, 18, 13, 44, 11, 40, 14, 26, 32, 42, 23, 26, 32, 24, 12, 30, 13, 9, 22, 12, 47, 30, 41, 38, 44, 14, 16, 25, 5, 45, 32, 37, 45, 20, 15, 15, 39, 9, 7, 0, 48, 42, 5, 12, 16, 9, 34, 42, 26, 48, 42, 37, 7, 18, 13, 41, 1, 46, 12, 1, 3, 41, 39, 28, 15, 46, 41, 17, 11, 28, 11, 14, 9, 25, 35, 18, 31, 16, 27, 47, 22]        |492           |260         |
|53       |[3, 48, 5, 44, 44, 45, 40, 21, 11, 5, 38, 19, 29, 7, 46, 48, 2, 17, 45, 42, 15, 34, 46, 46, 26, 14, 31, 10, 13, 34, 43, 7, 9, 24, 40, 24, 42, 16, 45, 6, 37, 2, 45, 33, 12, 16, 20, 19, 25, 45, 45, 44, 29, 47, 20, 0, 34, 46, 10, 14, 20, 23, 39, 39, 20, 20, 21, 3, 8, 32, 36, 17, 47, 37, 36, 40, 19, 5, 27, 31, 23, 0, 16, 17, 22, 31, 10, 19, 20, 10, 24, 28, 0, 38, 38, 2, 6, 10, 41, 41, 12, 21, 29, 15, 25, 46, 38, 1, 4, 46, 18, 35, 39, 37, 49, 12, 24, 47, 19, 19, 20, 49, 26, 33, 40, 37, 31, 3, 47, 16, 36, 3, 1, 7, 49, 9, 11, 15, 40, 43, 26, 19, 14, 47, 48, 40, 6, 34, 36, 46, 46, 38, 0, 39, 20, 13, 25, 48, 24, 23, 20, 32, 28, 25, 43, 8, 5, 8, 26, 2, 34, 10, 33, 27, 21, 20, 49, 11, 40, 7, 13, 9, 26, 43, 11, 49, 41, 10, 34, 9, 8, 39, 7, 22, 30, 38, 40, 22, 13, 36, 42, 11, 46, 10, 21, 46, 7, 4, 41, 38, 20, 23, 3, 27, 46, 39, 41, 42, 39, 5, 35, 20, 34, 31, 26, 8, 8, 7, 5, 44, 42, 48, 22, 49, 21, 41, 46, 19, 10, 39, 34, 38, 38, 16, 19, 10, 47, 30, 43, 10, 45, 34, 35, 42, 3, 41, 2, 6, 38, 22, 24, 43, 23, 48, 42, 44, 9, 13, 37, 27, 19, 41, 22, 39, 48, 39, 38, 12, 3, 20, 20, 37, 7, 26, 18, 16, 8, 4, 42, 39, 21, 12, 9, 41, 20, 48, 2, 42, 6, 24, 48, 26, 22, 25, 6, 36, 5, 1, 46, 39, 11, 9, 31, 14, 13, 25, 48, 32, 41, 34, 37, 18, 2, 14, 42, 33, 17, 18, 3, 37, 10, 32, 45, 18, 25, 30, 2, 4, 33, 27, 25, 42, 40, 39, 35, 37, 36, 30, 19, 15, 2, 3, 30, 2, 42, 34, 31, 30, 8, 35, 16, 28, 1, 47, 37, 32, 38, 4, 40, 35, 37, 6, 33, 45, 18, 39, 12, 7, 14, 47, 19, 26, 26, 10, 18, 21, 27, 17, 43, 28, 45, 7, 29, 0, 38, 2, 13, 41, 28, 34, 36, 9, 28, 45, 9, 16, 44, 18, 45, 5, 12, 22, 39, 27, 20, 41, 6, 26, 27, 45, 6, 40, 35, 6, 43, 13, 49, 39, 20, 17, 25, 15, 49, 11, 44, 11, 40, 22, 43, 45, 21, 8, 48, 45, 16, 33, 7, 40, 11, 49, 34, 8, 10, 38, 6, 41, 23, 28, 39, 15]                                                                                                                                                                                                                                                                                     |490           |258         |
|65       |[26, 20, 41, 17, 7, 0, 16, 3, 17, 36, 41, 10, 20, 21, 42, 8, 38, 22, 21, 20, 21, 12, 34, 27, 14, 17, 49, 7, 12, 1, 17, 14, 43, 9, 46, 46, 37, 45, 23, 46, 13, 0, 14, 38, 43, 33, 39, 41, 32, 21, 38, 13, 8, 8, 15, 7, 12, 7, 19, 20, 4, 23, 1, 46, 0, 40, 12, 0, 20, 34, 9, 40, 29, 18, 11, 15, 4, 4, 15, 28, 47, 48, 44, 41, 18, 10, 48, 3, 30, 14, 41, 27, 18, 12, 16, 18, 47, 13, 25, 6, 33, 30, 32, 15, 10, 22, 21, 21, 14, 19, 9, 14, 29, 17, 35, 20, 27, 4, 8, 22, 24, 17, 47, 31, 9, 47, 38, 13, 12, 45, 6, 14, 14, 28, 38, 35, 38, 35, 23, 24, 16, 26, 39, 35, 13, 19, 41, 2, 29, 25, 4, 6, 0, 26, 20, 13, 33, 3, 36, 13, 46, 38, 22, 20, 41, 45, 24, 32, 27, 19, 22, 42, 33, 21, 10, 38, 26, 48, 13, 47, 7, 44, 27, 17, 29, 4, 26, 40, 43, 16, 29, 23, 14, 26, 14, 15, 29, 16, 29, 11, 23, 34, 23, 18, 48, 33, 9, 42, 22, 12, 2, 15, 36, 19, 36, 45, 43, 37, 7, 32, 40, 11, 37, 38, 5, 48, 11, 31, 45, 32, 16, 16, 26, 24, 0, 15, 43, 41, 39, 37, 0, 13, 6, 26, 25, 2, 38, 1, 10, 39, 39, 49, 45, 3, 46, 27, 29, 1, 19, 26, 46, 22, 23, 31, 9, 15, 38, 6, 3, 48, 47, 28, 43, 49, 18, 1, 1, 41, 1, 24, 0, 31, 33, 2, 30, 23, 37, 3, 40, 10, 20, 24, 38, 33, 9, 40, 27, 10, 41, 43, 48, 42, 18, 29, 49, 46, 32, 42, 28, 3, 12, 26, 47, 47, 25, 9, 44, 26, 26, 48, 6, 20, 30, 19, 7, 34, 12, 36, 3, 15, 27, 40, 47, 15, 7, 21, 6, 31, 44, 38, 9, 4, 39, 33, 14, 23, 24, 29, 20, 0, 33, 14, 31, 42, 23, 24, 5, 34, 16, 43, 25, 22, 7, 40, 43, 20, 33, 8, 38, 4, 37, 3, 10, 22, 24, 11, 39, 25, 13, 40, 41, 39, 42, 8, 20, 45, 7, 2, 18, 33, 28, 6, 15, 38, 20, 9, 28, 49, 7, 14, 14, 32, 33, 46, 30, 25, 7, 25, 8, 13, 16, 19, 29, 15, 46, 42, 43, 24, 30, 4, 6, 5, 28, 42, 21, 14, 20, 3, 26, 37, 47, 23, 45, 31, 34, 18, 33, 43, 22, 45, 30, 46, 23, 8, 28, 7, 40, 36, 43, 42, 46, 19, 43, 4, 35, 8, 19, 18, 6, 12, 14, 16, 44, 15, 22, 15, 33, 35, 20, 24, 25, 8, 39, 8, 38, 30, 22, 45, 33, 8, 36, 0, 2, 30, 3, 5, 3, 49, 21, 39, 17, 16, 21, 29, 3, 29, 27, 13, 25, 26, 34]                                                                                                                             |486           |256         |
|78       |[15, 15, 0, 11, 19, 38, 45, 37, 20, 19, 37, 34, 12, 3, 19, 42, 8, 33, 21, 21, 9, 46, 17, 22, 23, 38, 29, 42, 11, 8, 16, 5, 31, 6, 23, 13, 20, 35, 43, 14, 41, 10, 23, 13, 11, 12, 44, 44, 26, 49, 32, 36, 46, 20, 31, 25, 43, 29, 47, 24, 49, 37, 28, 37, 0, 36, 40, 18, 40, 14, 15, 45, 19, 21, 24, 18, 5, 21, 44, 30, 27, 21, 37, 10, 25, 35, 41, 0, 20, 22, 35, 47, 24, 31, 10, 27, 28, 31, 35, 29, 30, 37, 27, 33, 14, 23, 47, 21, 19, 13, 26, 25, 35, 44, 47, 6, 43, 36, 49, 24, 23, 18, 35, 32, 26, 8, 48, 4, 33, 19, 1, 45, 44, 47, 1, 21, 6, 18, 10, 0, 6, 1, 33, 3, 36, 28, 3, 32, 47, 7, 43, 44, 2, 0, 8, 26, 2, 12, 11, 40, 22, 4, 4, 27, 32, 25, 32, 40, 35, 26, 26, 15, 44, 9, 1, 23, 19, 12, 48, 11, 6, 23, 23, 17, 3, 11, 16, 15, 48, 46, 41, 20, 43, 0, 24, 0, 9, 18, 40, 47, 10, 11, 26, 42, 16, 25, 42, 33, 46, 21, 1, 18, 0, 27, 36, 13, 24, 35, 44, 20, 8, 10, 38, 26, 24, 15, 18, 31, 38, 26, 8, 9, 32, 21, 21, 35, 24, 37, 10, 15, 1, 3, 19, 25, 42, 30, 33, 17, 35, 21, 0, 21, 47, 45, 5, 22, 39, 32, 6, 39, 28, 10, 16, 9, 41, 11, 39, 38, 0, 21, 9, 25, 16, 44, 29, 16, 38, 45, 37, 0, 28, 21, 0, 42, 31, 9, 5, 35, 44, 12, 48, 33, 45, 38, 6, 36, 5, 42, 26, 26, 39, 46, 0, 35, 45, 27, 40, 18, 14, 0, 6, 13, 9, 10, 16, 28, 13, 44, 44, 17, 13, 30, 31, 41, 21, 20, 19, 18, 27, 46, 39, 12, 29, 44, 43, 45, 10, 31, 22, 43, 47, 5, 20, 12, 11, 20, 13, 6, 26, 41, 7, 20, 17, 5, 7, 0, 27, 1, 34, 44, 44, 45, 26, 15, 1, 10, 19, 33, 32, 25, 26, 39, 10, 48, 48, 43, 17, 40, 24, 33, 29, 46, 15, 20, 10, 49, 18, 10, 47, 3, 11, 26, 38, 4, 42, 46, 44, 15, 25, 45, 33, 21, 18, 20, 26, 44, 26, 42, 15, 19, 15, 22, 11, 32, 13, 28, 40, 21, 10, 24, 47, 18, 34, 41, 47, 38, 2, 8, 49, 48, 19, 14, 31, 18, 13, 11, 21, 27, 19, 33, 15, 9, 9, 1, 16, 34, 16, 6, 18, 29, 27, 46, 14, 17, 44, 35, 7, 6, 8, 28, 0, 23, 36, 16, 25, 48, 28, 18, 4, 27, 31, 24, 6, 17, 2, 30, 42, 13, 39, 41, 15, 35, 36, 14, 43, 44, 2, 32, 26, 32, 12, 49, 48, 9, 35, 8, 31, 9, 18, 24, 40, 10, 34, 25, 1, 18, 26, 15, 28, 44, 28, 26, 1]                                                                     |476           |252         |
|85       |[11, 37, 46, 28, 26, 42, 37, 31, 49, 46, 6, 14, 32, 28, 7, 48, 48, 37, 43, 40, 29, 11, 25, 3, 12, 23, 31, 17, 46, 13, 14, 5, 15, 16, 48, 42, 9, 11, 21, 18, 36, 44, 48, 4, 26, 2, 8, 40, 39, 9, 26, 24, 5, 23, 48, 17, 25, 27, 11, 8, 28, 42, 10, 2, 44, 38, 13, 41, 26, 22, 42, 9, 29, 5, 3, 2, 10, 16, 5, 2, 44, 10, 27, 2, 43, 28, 10, 48, 29, 18, 26, 14, 17, 44, 44, 45, 1, 41, 0, 36, 20, 20, 35, 48, 8, 2, 42, 2, 45, 2, 44, 41, 37, 23, 19, 35, 40, 15, 10, 33, 37, 45, 9, 35, 47, 27, 32, 22, 15, 8, 8, 0, 43, 6, 15, 24, 38, 6, 26, 1, 5, 19, 33, 39, 37, 35, 28, 37, 8, 48, 30, 3, 38, 16, 37, 47, 49, 32, 31, 1, 34, 10, 3, 46, 35, 18, 30, 7, 25, 43, 25, 6, 45, 7, 43, 31, 38, 13, 29, 41, 20, 5, 15, 16, 0, 30, 45, 3, 1, 48, 3, 1, 34, 48, 27, 16, 43, 35, 20, 4, 41, 41, 36, 17, 23, 18, 19, 5, 8, 42, 6, 10, 10, 0, 20, 6, 23, 37, 49, 12, 26, 18, 33, 33, 10, 1, 27, 6, 23, 49, 37, 4, 28, 29, 5, 18, 23, 3, 7, 42, 8, 48, 18, 30, 5, 27, 35, 25, 27, 38, 29, 40, 0, 12, 3, 19, 33, 2, 44, 8, 2, 10, 13, 45, 39, 47, 4, 30, 22, 36, 11, 34, 18, 8, 40, 0, 34, 11, 41, 46, 17, 47, 47, 27, 38, 34, 1, 21, 16, 15, 11, 11, 30, 19, 2, 35, 12, 25, 39, 11, 24, 0, 47, 16, 8, 18, 10, 18, 9, 29, 9, 35, 27, 3, 3, 15, 41, 21, 16, 21, 4, 2, 47, 22, 12, 42, 48, 14, 17, 3, 2, 33, 23, 4, 28, 5, 4, 16, 29, 10, 8, 16, 29, 41, 28, 36, 42, 44, 8, 29, 21, 31, 31, 2, 8, 21, 9, 20, 44, 44, 10, 26, 18, 13, 46, 27, 45, 4, 46, 3, 12, 8, 34, 11, 1, 23, 26, 30, 13, 12, 12, 48, 9, 27, 39, 41, 46, 5, 12, 45, 14, 38, 42, 22, 36, 15, 20, 47, 22, 31, 3, 8, 49, 29, 17, 28, 44, 11, 39, 3, 28, 25, 0, 14, 20, 44, 27, 23, 11, 44, 17, 18, 1, 42, 27, 2, 20, 6, 11, 17, 42, 9, 43, 35, 49, 27, 29, 33, 30, 25, 34, 27, 2, 49, 10, 0, 12, 35, 5, 18, 8, 39, 31, 1, 11, 27, 0, 33, 3, 24, 7, 29, 6, 14, 46, 16, 25, 28, 45, 20, 44, 1, 36, 17, 12, 11, 1, 11, 14, 5, 44, 5, 8, 9, 26, 13, 41, 38, 28, 19, 38, 22, 24, 31, 21, 0, 0, 39, 38, 10, 39, 15, 1, 5, 45, 11, 22, 27, 13, 22, 11, 15, 29, 43, 0, 48, 31, 15, 15, 3, 14, 36, 10, 15, 14, 40, 10, 46]                                             |527           |277         |
|108      |[30, 44, 28, 45, 1, 7, 30, 13, 25, 5, 14, 1, 32, 14, 36, 9, 27, 34, 36, 27, 22, 0, 26, 25, 34, 49, 21, 6, 39, 43, 23, 15, 7, 22, 3, 16, 7, 35, 7, 5, 11, 1, 24, 35, 41, 39, 41, 7, 17, 1, 7, 43, 18, 9, 40, 44, 24, 45, 48, 30, 38, 17, 6, 10, 41, 3, 20, 39, 11, 12, 41, 13, 13, 10, 49, 18, 11, 36, 27, 37, 22, 43, 8, 15, 40, 27, 38, 15, 25, 13, 10, 29, 6, 10, 10, 29, 45, 37, 13, 49, 46, 22, 28, 35, 12, 18, 36, 46, 4, 30, 10, 30, 1, 10, 12, 17, 13, 44, 39, 5, 9, 36, 49, 46, 49, 29, 7, 41, 49, 48, 37, 29, 16, 7, 0, 47, 19, 9, 47, 47, 39, 0, 24, 38, 27, 39, 14, 12, 9, 21, 31, 18, 46, 43, 20, 27, 24, 28, 38, 44, 40, 11, 8, 8, 0, 43, 30, 38, 16, 45, 49, 21, 41, 35, 30, 22, 4, 37, 17, 12, 33, 21, 34, 38, 10, 9, 1, 16, 22, 6, 46, 46, 48, 46, 29, 15, 36, 3, 5, 26, 32, 44, 41, 17, 28, 45, 10, 39, 8, 17, 11, 24, 32, 44, 29, 36, 23, 23, 46, 11, 16, 1, 28, 6, 28, 39, 45, 19, 25, 28, 39, 12, 28, 16, 34, 14, 19, 30, 15, 24, 20, 4, 22, 19, 19, 29, 44, 31, 14, 14, 34, 20, 43, 48, 13, 16, 26, 3, 0, 6, 37, 6, 21, 0, 16, 22, 28, 3, 5, 10, 41, 34, 17, 35, 20, 34, 18, 27, 29, 49, 39, 17, 29, 13, 19, 34, 28, 15, 24, 49, 42, 11, 36, 38, 38, 40, 8, 17, 39, 28, 0, 32, 38, 39, 24, 16, 31, 34, 30, 13, 49, 7, 48, 5, 41, 2, 4, 13, 31, 41, 11, 28, 1, 24, 9, 46, 38, 12, 17, 29, 37, 14, 2, 42, 31, 49, 8, 38, 28, 38, 36, 11, 7, 36, 42, 31, 8, 23, 2, 39, 17, 34, 9, 28, 18, 28, 4, 5, 10, 13, 19, 35, 29, 14, 35, 36, 15, 36, 3, 16, 22, 14, 25, 39, 40, 12, 48, 41, 43, 17, 43, 12, 15, 1, 14, 28, 44, 6, 8, 39, 31, 14, 13, 16, 5, 16, 37, 41, 0, 3, 43, 40, 6, 34, 12, 9, 15, 13, 40, 42, 42, 25, 11, 46, 1, 12, 4, 45, 15, 3, 29, 34, 33, 44, 38, 0, 29, 31, 21, 22, 0, 18, 45, 16, 25, 42, 15, 10, 38, 10, 16, 34, 8, 23, 24, 22, 19, 6, 40, 6, 20, 41, 45, 6, 5, 28, 9, 21, 14, 21, 3, 33, 40, 25, 31, 2, 31, 12, 11, 45, 26, 38]                                                                                                                                                                                                                                           |486           |257         |
|133      |[33, 48, 48, 17, 36, 5, 3, 36, 19, 42, 21, 41, 6, 9, 45, 34, 44, 25, 25, 16, 13, 7, 12, 48, 5, 2, 38, 16, 24, 42, 39, 44, 20, 20, 8, 43, 45, 11, 37, 11, 16, 20, 37, 45, 19, 6, 39, 40, 2, 5, 31, 29, 17, 6, 35, 19, 31, 42, 37, 49, 6, 42, 19, 3, 24, 18, 34, 29, 3, 12, 26, 14, 23, 37, 24, 44, 2, 23, 19, 10, 23, 24, 17, 31, 30, 21, 30, 44, 6, 35, 13, 25, 35, 5, 41, 26, 48, 14, 4, 24, 12, 27, 36, 34, 48, 4, 39, 35, 39, 26, 41, 2, 23, 17, 1, 8, 6, 42, 13, 30, 30, 25, 29, 1, 14, 6, 33, 28, 32, 38, 22, 7, 24, 33, 20, 19, 39, 2, 24, 8, 5, 43, 34, 18, 7, 13, 37, 2, 49, 22, 11, 4, 41, 46, 31, 29, 9, 19, 10, 36, 8, 7, 24, 46, 7, 3, 35, 9, 0, 11, 44, 4, 45, 20, 34, 33, 29, 12, 3, 11, 40, 49, 17, 2, 2, 26, 6, 19, 43, 39, 33, 31, 31, 29, 0, 2, 9, 15, 39, 6, 7, 31, 6, 8, 31, 48, 13, 36, 30, 45, 45, 32, 15, 48, 3, 4, 34, 27, 21, 35, 7, 3, 29, 25, 19, 40, 38, 47, 8, 41, 41, 26, 10, 32, 47, 13, 20, 24, 27, 3, 24, 5, 37, 9, 32, 48, 23, 46, 28, 2, 2, 7, 3, 2, 25, 11, 1, 3, 0, 25, 19, 0, 7, 19, 42, 8, 7, 11, 44, 41, 19, 6, 30, 40, 8, 38, 35, 18, 32, 45, 41, 18, 45, 31, 42, 9, 21, 38, 13, 20, 49, 37, 25, 39, 33, 26, 21, 47, 22, 24, 17, 3, 36, 9, 9, 3, 39, 18, 17, 14, 41, 31, 36, 43, 30, 40, 45, 0, 12, 2, 22, 10, 6, 27, 37, 17, 1, 4, 32, 44, 26, 27, 30, 36, 6, 42, 4, 20, 9, 18, 30, 22, 13, 38, 11, 5, 12, 46, 34, 38, 41, 34, 24, 2, 21, 29, 33, 47, 40, 9, 19, 21, 37, 34, 27, 42, 9, 4, 37, 32, 9, 10, 26, 15, 46, 31, 12, 32, 8, 40, 3, 4, 47, 27, 29, 8, 49, 42, 22, 26, 4, 17, 22, 24, 30, 48, 12, 3, 2, 27, 30, 44, 31, 33, 22, 36, 14, 42, 2, 8, 0, 47, 28, 48, 34, 3, 21, 48, 24, 35, 3, 6, 23, 29, 18, 33, 44, 13, 14, 28, 41, 14, 5, 30, 49, 1, 37, 11, 49, 25, 15, 22, 1, 14, 10, 16, 37, 17, 41, 49, 22, 23, 42, 8, 2, 6, 10, 39, 35, 14, 41, 31, 36, 19, 2, 47, 18, 11, 23, 2, 16, 3, 22, 45]                                                                                                                                                                                                                                                             |498           |263         |
|137      |[10, 16, 14, 24, 48, 44, 4, 22, 20, 28, 29, 38, 22, 19, 39, 7, 42, 34, 9, 18, 27, 41, 2, 30, 0, 20, 25, 48, 27, 3, 22, 6, 13, 7, 24, 17, 13, 33, 23, 41, 36, 28, 26, 46, 24, 25, 29, 39, 20, 37, 21, 30, 45, 2, 47, 20, 45, 12, 31, 43, 39, 9, 23, 32, 27, 17, 9, 16, 26, 3, 49, 5, 3, 2, 3, 49, 30, 46, 1, 30, 36, 12, 7, 8, 46, 34, 43, 39, 34, 29, 9, 2, 16, 35, 28, 25, 1, 32, 6, 13, 13, 13, 8, 33, 24, 19, 1, 47, 2, 48, 30, 3, 5, 49, 47, 16, 7, 49, 8, 9, 15, 40, 9, 43, 42, 48, 3, 35, 6, 22, 48, 44, 9, 36, 35, 14, 41, 2, 33, 41, 37, 35, 5, 21, 34, 26, 12, 26, 23, 40, 24, 24, 23, 10, 23, 34, 13, 5, 38, 46, 1, 46, 11, 24, 32, 20, 34, 2, 48, 14, 13, 32, 40, 10, 17, 4, 49, 36, 33, 46, 38, 42, 21, 42, 5, 43, 44, 13, 23, 8, 9, 22, 43, 22, 7, 5, 12, 31, 20, 32, 24, 21, 27, 0, 43, 12, 30, 48, 32, 12, 32, 42, 0, 24, 11, 31, 12, 44, 6, 20, 24, 43, 10, 31, 32, 24, 27, 24, 20, 9, 20, 15, 23, 28, 15, 31, 18, 38, 45, 47, 46, 40, 9, 45, 27, 20, 7, 13, 38, 46, 6, 26, 24, 24, 32, 30, 2, 32, 24, 29, 48, 36, 8, 26, 45, 27, 5, 9, 45, 9, 28, 7, 4, 28, 8, 41, 41, 48, 35, 28, 35, 42, 10, 0, 27, 32, 16, 20, 29, 47, 42, 9, 31, 12, 3, 8, 43, 3, 8, 34, 38, 36, 27, 41, 39, 21, 28, 14, 15, 48, 5, 5, 27, 1, 42, 9, 10, 47, 25, 39, 39, 27, 14, 37, 42, 43, 47, 29, 49, 17, 32, 39, 8, 5, 14, 12, 47, 30, 27, 10, 13, 43, 33, 1, 8, 6, 28, 2, 24, 36, 34, 45, 32, 10, 46, 4, 38, 42, 48, 32, 11, 6, 9, 22, 49, 12, 39, 35, 2, 47, 11, 27, 26, 26, 17, 49, 37, 15, 36, 15, 30, 2, 2, 49, 15, 42, 2, 4, 25, 26, 14, 29, 39, 3, 10, 29, 40, 15, 42, 37, 9, 48, 24, 34, 15, 25, 10, 46, 19, 14, 18, 29, 25, 42, 47, 11, 39, 46, 27, 31, 14, 5, 34, 6, 45, 0, 40, 19, 36, 37, 44, 4, 35, 3, 15, 25, 11, 33, 27, 24, 20, 32, 34, 18, 37, 33, 28, 35, 1, 22, 17, 34, 44, 12, 49, 26, 16, 13, 6, 16, 12, 3, 34, 31, 48, 1, 35, 15, 36, 11, 11, 4, 8, 8, 27, 49]                                                                                                                                                                                                                                     |498           |263         |
|148      |[5, 47, 28, 1, 28, 30, 13, 29, 44, 13, 34, 13, 29, 15, 45, 20, 49, 36, 40, 18, 44, 35, 27, 32, 33, 29, 27, 21, 33, 4, 35, 46, 8, 39, 7, 45, 28, 24, 15, 17, 43, 37, 11, 30, 2, 27, 16, 8, 8, 43, 5, 24, 6, 9, 6, 32, 33, 30, 27, 15, 9, 24, 35, 20, 40, 38, 45, 14, 21, 11, 40, 0, 25, 39, 43, 32, 42, 24, 12, 28, 3, 10, 7, 4, 43, 31, 2, 2, 13, 24, 21, 4, 45, 20, 33, 17, 35, 0, 17, 40, 33, 5, 25, 21, 22, 11, 46, 29, 25, 45, 25, 34, 15, 5, 19, 13, 4, 20, 11, 3, 22, 37, 45, 19, 1, 47, 18, 35, 17, 44, 17, 38, 22, 42, 28, 47, 48, 48, 3, 7, 28, 27, 5, 15, 4, 10, 3, 8, 40, 8, 12, 34, 7, 18, 46, 9, 9, 17, 1, 31, 40, 3, 49, 27, 39, 28, 0, 9, 21, 16, 41, 3, 10, 16, 38, 3, 34, 0, 2, 9, 12, 42, 13, 40, 32, 2, 28, 16, 17, 2, 21, 26, 33, 6, 49, 20, 31, 28, 42, 24, 6, 30, 19, 29, 38, 17, 32, 11, 22, 26, 30, 29, 48, 36, 28, 23, 45, 33, 32, 27, 2, 42, 22, 19, 11, 4, 45, 45, 43, 1, 44, 13, 22, 22, 8, 32, 39, 22, 24, 16, 9, 33, 28, 24, 39, 37, 43, 29, 21, 38, 1, 20, 48, 39, 4, 32, 27, 9, 22, 35, 32, 27, 23, 49, 16, 24, 34, 49, 8, 16, 49, 25, 17, 13, 38, 40, 22, 31, 28, 42, 7, 13, 43, 4, 0, 16, 46, 10, 44, 22, 35, 38, 8, 48, 30, 49, 31, 5, 10, 34, 11, 49, 7, 48, 38, 11, 38, 31, 42, 0, 9, 28, 28, 17, 29, 11, 24, 48, 25, 3, 7, 10, 42, 1, 35, 34, 11, 15, 29, 43, 34, 7, 12, 40, 13, 10, 14, 27, 22, 28, 44, 27, 47, 9, 12, 0, 42, 46, 20, 35, 48, 34, 2, 13, 15, 21, 12, 19, 37, 25, 1, 24, 9, 18, 1, 35, 40, 38, 30, 9, 27, 18, 40, 38, 36, 42, 18, 42, 39, 27, 28, 3, 48, 21, 3, 35, 22, 49, 20, 48, 27, 20, 18, 28, 48, 49, 20, 47, 41, 32, 47, 24, 32, 37, 25, 14, 15, 28, 6, 20, 24, 7, 17, 5, 15, 10, 20, 36, 16, 29, 8, 6, 31, 3, 16, 19, 34, 12, 2, 17, 40, 2, 19, 17, 39, 6, 41, 25, 3, 11, 45, 10, 49, 42, 39, 43, 4, 9, 40, 4, 13, 0, 46, 36, 19, 46, 41, 21, 16, 35, 21, 0, 27, 22, 30, 46, 12, 28, 46, 23]                                                                                                                                                                                                                                                        |495           |261         |
|155      |[17, 28, 15, 44, 47, 7, 15, 7, 31, 39, 38, 36, 4, 48, 11, 19, 37, 31, 10, 33, 37, 32, 21, 0, 30, 27, 25, 37, 16, 13, 5, 37, 1, 20, 9, 46, 42, 38, 25, 13, 37, 26, 34, 48, 1, 24, 38, 49, 27, 35, 3, 28, 10, 6, 42, 17, 1, 38, 28, 17, 5, 27, 24, 15, 24, 18, 20, 49, 19, 14, 9, 3, 2, 13, 25, 25, 8, 46, 21, 40, 12, 2, 23, 27, 37, 44, 0, 8, 47, 22, 27, 9, 37, 47, 8, 8, 30, 3, 5, 14, 15, 44, 21, 18, 1, 33, 16, 33, 30, 5, 39, 8, 42, 2, 7, 15, 35, 46, 21, 33, 24, 35, 15, 41, 49, 37, 5, 16, 11, 3, 36, 31, 12, 18, 45, 24, 22, 26, 49, 49, 21, 8, 3, 9, 26, 14, 35, 16, 10, 14, 1, 9, 3, 30, 27, 3, 34, 38, 5, 22, 14, 46, 47, 12, 25, 47, 5, 24, 10, 9, 24, 46, 8, 22, 19, 19, 40, 5, 6, 20, 20, 29, 41, 35, 47, 16, 19, 43, 0, 46, 30, 14, 27, 33, 46, 45, 14, 36, 23, 16, 44, 36, 7, 42, 27, 11, 12, 4, 47, 5, 41, 23, 35, 9, 25, 21, 38, 23, 30, 1, 47, 22, 3, 11, 35, 44, 46, 4, 1, 23, 32, 7, 11, 10, 22, 45, 8, 19, 2, 5, 32, 4, 38, 38, 10, 29, 42, 14, 45, 19, 0, 49, 3, 40, 41, 12, 20, 43, 4, 28, 9, 33, 20, 32, 0, 4, 9, 37, 26, 11, 39, 37, 34, 16, 0, 10, 18, 25, 48, 48, 21, 32, 13, 47, 10, 44, 12, 38, 13, 7, 46, 0, 43, 28, 6, 23, 26, 24, 3, 5, 45, 22, 10, 3, 2, 40, 39, 14, 1, 46, 49, 45, 12, 8, 36, 31, 23, 48, 19, 49, 41, 35, 13, 8, 6, 2, 46, 23, 6, 40, 32, 39, 17, 40, 12, 36, 15, 23, 21, 49, 45, 31, 43, 8, 5, 35, 41, 45, 34, 4, 8, 32, 32, 19, 3, 41, 42, 2, 42, 47, 22, 36, 27, 26, 46, 21, 38, 37, 9, 4, 26, 33, 17, 39, 12, 49, 40, 41, 0, 15, 26, 15, 0, 6, 20, 18, 28, 11, 6, 36, 29, 48, 1, 40, 43, 6, 5, 7, 38, 20, 36, 11, 47, 47, 22, 49, 1, 28, 2, 13, 25, 2, 32, 38, 20, 1, 6, 11, 6, 12, 39, 18, 3, 13, 36, 28, 11, 21, 3, 41, 12, 7, 39, 17, 22, 33, 23, 25, 33, 48, 17, 5, 17, 13, 49, 38, 24, 39, 21, 14, 6, 36, 20, 36, 18, 26, 25, 49, 14, 18, 41, 11, 26, 38, 26, 27, 23, 16, 38, 9, 47, 34, 19, 47, 34, 45, 33, 12, 4, 27, 33, 3, 33, 25, 37, 26, 1, 38, 32, 19, 2, 3, 23, 43, 41, 34, 13, 48, 18, 20, 12, 37, 29, 22, 27, 30, 22, 24, 27, 28, 8, 24, 34, 7, 12, 24, 21, 12, 40, 25, 25, 7, 4, 15, 32, 19, 40, 9, 11]                                   |496           |262         |
|193      |[23, 17, 35, 6, 38, 3, 35, 21, 5, 37, 2, 47, 7, 49, 39, 40, 10, 33, 27, 27, 34, 29, 32, 43, 26, 31, 33, 23, 21, 46, 14, 49, 7, 38, 4, 46, 37, 30, 18, 1, 1, 18, 38, 32, 47, 8, 49, 24, 39, 40, 20, 10, 29, 32, 4, 6, 44, 39, 26, 26, 18, 24, 7, 33, 13, 46, 45, 47, 17, 31, 32, 27, 27, 19, 43, 42, 43, 27, 5, 13, 22, 21, 10, 42, 43, 3, 23, 32, 2, 40, 3, 25, 32, 37, 35, 22, 34, 24, 44, 6, 26, 47, 36, 28, 16, 12, 39, 31, 8, 38, 1, 24, 33, 22, 14, 39, 22, 4, 38, 49, 16, 24, 43, 4, 44, 17, 28, 46, 38, 0, 12, 9, 9, 44, 31, 17, 6, 47, 25, 45, 47, 35, 23, 43, 20, 12, 13, 9, 19, 8, 33, 45, 22, 15, 22, 20, 3, 11, 38, 38, 47, 21, 46, 33, 45, 29, 37, 12, 22, 14, 11, 41, 9, 29, 5, 11, 27, 38, 33, 30, 7, 35, 12, 30, 15, 43, 6, 4, 7, 37, 41, 41, 35, 20, 33, 38, 10, 22, 28, 18, 1, 0, 7, 32, 10, 38, 21, 11, 5, 39, 19, 44, 41, 46, 40, 27, 11, 47, 34, 11, 24, 24, 9, 4, 35, 17, 13, 28, 12, 32, 24, 47, 18, 26, 43, 22, 37, 44, 16, 4, 10, 14, 35, 7, 16, 5, 6, 24, 11, 36, 6, 34, 8, 1, 27, 37, 8, 33, 2, 8, 32, 14, 3, 19, 0, 4, 26, 12, 12, 3, 18, 40, 3, 47, 18, 41, 9, 10, 38, 44, 17, 29, 7, 30, 17, 5, 23, 22, 6, 21, 24, 2, 44, 44, 4, 29, 7, 37, 7, 33, 25, 1, 0, 3, 28, 49, 36, 30, 41, 32, 48, 35, 10, 25, 7, 16, 43, 36, 28, 17, 37, 13, 9, 6, 26, 4, 17, 29, 31, 7, 24, 49, 43, 14, 40, 22, 40, 13, 22, 29, 43, 41, 40, 15, 3, 36, 9, 45, 26, 49, 36, 8, 5, 11, 9, 48, 46, 49, 0, 2, 5, 44, 35, 24, 48, 21, 30, 14, 33, 26, 23, 39, 20, 37, 7, 30, 33, 6, 36, 10, 34, 25, 6, 38, 31, 34, 20, 10, 15, 39, 38, 23, 26, 10, 28, 17, 1, 48, 34, 5, 33, 46, 7, 48, 30, 12, 29, 37, 44, 46, 37, 6, 29, 43, 31, 47, 30, 3, 8, 38, 41, 11, 20, 36, 9, 22, 11, 5, 17, 19, 21, 8, 29, 24, 48, 28, 5, 13, 4, 7, 21, 19, 2, 21, 26, 6, 32, 8, 35, 26, 23, 38, 2, 28, 35, 44, 22, 23, 13, 33, 10, 30, 10, 1, 37, 12, 38, 35, 26, 42, 21, 19, 34, 41, 14, 30, 49, 49, 12, 30, 19, 36, 28, 8, 28, 14, 27, 43, 46, 40, 27, 17, 6, 28, 46, 49, 38, 18, 26, 24, 27, 16, 37, 17, 40, 26, 18, 11, 0, 25, 30, 35, 10, 34, 31, 4, 14, 3, 0, 45]                                                            |502           |265         |
|211      |[10, 23, 34, 35, 35, 38, 12, 1, 27, 34, 45, 5, 3, 19, 11, 46, 2, 21, 24, 4, 9, 44, 11, 43, 15, 41, 21, 44, 40, 1, 24, 36, 5, 42, 25, 47, 17, 37, 46, 22, 26, 30, 27, 11, 0, 2, 4, 28, 25, 19, 49, 0, 13, 42, 6, 12, 14, 9, 16, 15, 19, 9, 9, 23, 30, 0, 9, 27, 24, 11, 11, 34, 7, 14, 37, 34, 27, 40, 44, 5, 22, 29, 28, 37, 15, 42, 5, 15, 16, 48, 23, 13, 39, 27, 2, 31, 4, 49, 39, 21, 22, 29, 24, 24, 47, 34, 4, 46, 19, 7, 10, 23, 27, 32, 24, 20, 31, 49, 18, 29, 22, 9, 19, 45, 44, 7, 32, 37, 45, 16, 29, 27, 42, 24, 47, 20, 1, 42, 6, 14, 32, 5, 5, 43, 17, 9, 45, 27, 7, 15, 45, 6, 2, 29, 0, 5, 26, 30, 3, 0, 47, 8, 47, 32, 39, 43, 37, 49, 36, 30, 5, 45, 36, 20, 23, 15, 23, 12, 34, 21, 2, 14, 3, 49, 9, 29, 26, 6, 48, 13, 24, 15, 38, 23, 18, 25, 39, 20, 18, 39, 19, 17, 2, 9, 1, 5, 49, 34, 13, 0, 26, 17, 4, 41, 21, 14, 35, 37, 22, 36, 35, 19, 0, 23, 30, 12, 37, 31, 14, 12, 32, 7, 14, 38, 16, 12, 1, 13, 34, 0, 18, 23, 9, 2, 10, 41, 0, 41, 37, 38, 18, 4, 16, 28, 49, 28, 47, 19, 33, 1, 47, 33, 30, 17, 16, 32, 41, 41, 2, 38, 18, 0, 5, 25, 44, 44, 20, 8, 45, 19, 44, 18, 32, 4, 24, 31, 2, 10, 16, 42, 31, 40, 13, 0, 14, 17, 32, 16, 35, 36, 8, 11, 28, 2, 17, 7, 30, 49, 40, 48, 14, 37, 33, 14, 15, 10, 42, 21, 16, 36, 14, 35, 19, 32, 25, 45, 0, 13, 18, 17, 23, 14, 29, 36, 39, 6, 13, 39, 5, 19, 13, 13, 45, 32, 12, 8, 17, 17, 24, 20, 34, 7, 6, 41, 18, 9, 10, 30, 26, 27, 8, 32, 21, 31, 5, 31, 16, 29, 0, 16, 12, 43, 46, 25, 3, 5, 12, 41, 32, 2, 34, 28, 10, 2, 23, 27, 25, 45, 44, 21, 31, 47, 19, 35, 28, 30, 16, 20, 14, 29, 30, 22, 2, 35, 41, 1, 35, 5, 33, 16, 42, 25, 10, 42, 1, 8, 33, 3, 27, 0, 8, 48, 21, 28, 35, 27, 40, 10, 13, 15, 23, 23, 7, 41, 14, 30, 32, 32, 49, 27, 7, 34, 13, 8, 21, 11, 33, 18, 34, 42, 39, 39, 2, 0, 31, 38, 29, 7, 32, 37, 8, 33, 7, 17, 16, 38, 34, 40, 45, 2, 42, 42, 30, 16, 4, 48, 28, 34, 21, 43, 0, 37, 41, 17, 28, 24, 24, 27, 47, 30, 27, 44, 0, 18, 38, 36, 24, 49, 18, 45, 0, 40, 35]                                                                                                                               |499           |263         |
|243      |[27, 27, 33, 12, 34, 2, 39, 36, 25, 19, 44, 15, 4, 0, 18, 38, 25, 43, 39, 31, 7, 34, 16, 30, 16, 31, 21, 6, 41, 13, 26, 24, 12, 10, 12, 26, 5, 1, 4, 7, 35, 13, 34, 38, 1, 28, 31, 49, 36, 1, 28, 25, 44, 22, 15, 17, 3, 39, 41, 11, 45, 0, 22, 13, 48, 41, 19, 25, 47, 39, 13, 17, 38, 43, 45, 12, 5, 11, 39, 38, 34, 1, 0, 23, 0, 19, 45, 13, 37, 34, 30, 18, 44, 6, 25, 14, 25, 15, 21, 31, 49, 5, 41, 39, 15, 43, 45, 46, 18, 17, 29, 34, 35, 33, 35, 37, 0, 38, 40, 39, 2, 31, 9, 45, 45, 2, 4, 30, 19, 19, 4, 38, 41, 20, 36, 38, 2, 25, 1, 9, 42, 22, 28, 20, 3, 36, 12, 3, 39, 6, 32, 10, 35, 26, 47, 21, 11, 34, 15, 33, 26, 0, 22, 8, 16, 38, 9, 48, 43, 29, 31, 15, 41, 3, 9, 21, 42, 34, 16, 25, 36, 5, 21, 26, 16, 35, 44, 14, 0, 21, 34, 17, 44, 38, 5, 39, 11, 40, 3, 8, 6, 35, 41, 28, 31, 6, 48, 15, 37, 45, 4, 21, 11, 46, 27, 28, 2, 18, 10, 31, 0, 13, 47, 39, 48, 8, 4, 6, 16, 1, 14, 14, 28, 1, 32, 40, 43, 36, 1, 31, 21, 8, 33, 26, 18, 40, 26, 41, 47, 20, 46, 12, 14, 0, 49, 4, 12, 30, 12, 42, 45, 38, 34, 13, 38, 0, 1, 24, 14, 35, 20, 33, 26, 7, 12, 4, 24, 29, 38, 44, 9, 44, 15, 18, 5, 14, 3, 26, 9, 40, 12, 12, 20, 5, 29, 49, 14, 31, 25, 4, 7, 9, 22, 5, 30, 1, 33, 47, 13, 17, 28, 18, 18, 8, 47, 48, 6, 30, 35, 37, 44, 12, 14, 42, 37, 46, 33, 36, 14, 19, 2, 14, 11, 43, 40, 35, 22, 8, 4, 1, 4, 15, 15, 41, 0, 49, 6, 27, 42, 28, 17, 20, 38, 7, 14, 44, 7, 36, 21, 29, 8, 4, 5, 38, 17, 46, 16, 4, 29, 42, 28, 45, 10, 24, 1, 3, 25, 42, 35, 13, 21, 20, 12, 21, 19, 18, 48, 19, 48, 49, 29, 35, 5, 43, 49, 39, 30, 34, 2, 23, 11, 17, 22, 38, 34, 0, 33, 49, 1, 17, 19, 8, 5, 34, 2, 4, 7, 3, 25, 34, 24, 33, 14, 25, 2, 32, 14, 1, 43, 12, 48, 42, 33, 37, 7, 21, 4, 47, 22, 22, 22, 10, 21, 27, 43, 8, 17, 4, 6, 47, 26, 29, 33, 7, 10, 17, 36, 47, 3, 16, 37, 14, 23, 6, 16, 0, 21, 4, 36, 30, 36, 16, 5, 20, 12, 3, 39, 27, 10, 34, 44, 7, 7, 12, 17, 7, 39, 14, 48, 42, 20, 19, 4, 16, 47, 31, 29, 18, 9, 7, 21, 35, 33, 40, 4, 20, 40, 25, 26, 43, 20, 43, 20, 17, 6, 37, 36, 13, 29, 33, 15, 46, 22, 17, 12, 46, 10, 4, 45, 4]                                  |500           |263         |
|251      |[18, 44, 19, 24, 29, 1, 12, 42, 41, 19, 42, 42, 36, 8, 24, 15, 7, 49, 14, 23, 16, 8, 47, 10, 31, 34, 12, 46, 39, 31, 26, 44, 22, 37, 30, 44, 45, 11, 0, 19, 10, 16, 26, 22, 6, 21, 38, 46, 19, 19, 30, 9, 3, 4, 0, 2, 11, 9, 20, 46, 11, 34, 21, 22, 27, 3, 37, 49, 9, 45, 2, 23, 10, 16, 36, 2, 14, 6, 22, 12, 5, 23, 5, 23, 20, 6, 42, 24, 22, 1, 43, 17, 8, 33, 38, 3, 42, 24, 44, 9, 35, 16, 36, 16, 32, 38, 49, 49, 20, 16, 10, 37, 8, 38, 49, 15, 30, 22, 11, 17, 45, 26, 33, 4, 2, 45, 32, 30, 22, 28, 41, 24, 3, 31, 40, 11, 40, 40, 4, 23, 46, 32, 18, 47, 49, 22, 19, 7, 18, 34, 18, 7, 1, 25, 29, 44, 21, 48, 0, 38, 42, 31, 28, 18, 22, 27, 38, 35, 43, 44, 21, 26, 30, 14, 17, 20, 41, 26, 8, 21, 19, 17, 40, 37, 41, 42, 14, 32, 14, 44, 20, 25, 17, 19, 39, 46, 41, 19, 19, 16, 15, 25, 33, 34, 38, 8, 22, 48, 46, 29, 29, 31, 17, 34, 16, 33, 20, 33, 16, 25, 8, 43, 20, 30, 24, 11, 37, 0, 12, 2, 26, 27, 33, 21, 7, 40, 23, 36, 21, 35, 27, 11, 17, 19, 6, 47, 47, 9, 35, 9, 32, 21, 49, 38, 42, 33, 36, 47, 28, 7, 45, 23, 23, 34, 49, 38, 32, 38, 7, 29, 48, 10, 10, 42, 39, 7, 39, 21, 35, 16, 32, 46, 18, 12, 43, 39, 16, 11, 40, 24, 0, 32, 17, 27, 47, 37, 0, 33, 11, 8, 13, 17, 31, 46, 9, 49, 34, 30, 48, 43, 10, 6, 46, 29, 46, 37, 36, 14, 42, 35, 30, 45, 5, 36, 33, 27, 31, 46, 1, 42, 26, 35, 12, 39, 3, 17, 18, 18, 18, 11, 9, 27, 39, 9, 48, 29, 44, 30, 14, 27, 5, 15, 11, 29, 46, 9, 10, 23, 46, 42, 31, 25, 0, 7, 20, 9, 38, 20, 10, 17, 43, 4, 11, 43, 47, 2, 49, 26, 47, 21, 43, 17, 48, 1, 37, 19, 30, 27, 40, 11, 47, 31, 10, 10, 21, 36, 29, 18, 2, 48, 23, 11, 21, 15, 41, 1, 47, 32, 2, 23, 16, 43, 37, 16, 4, 49, 1, 42, 29, 30, 40, 38, 27, 31, 22, 1, 30, 12, 26, 47, 14, 1, 29, 26, 4, 49, 1, 19, 1, 18, 34, 5, 23, 47, 28, 12, 17, 2, 31, 49, 44, 44, 24, 14, 22, 28, 1, 0, 1, 15, 24, 3, 37, 35, 19, 29, 48, 0, 15, 24]                                                                                                                                                                                                                                          |482           |255         |
|255      |[30, 15, 37, 5, 41, 49, 39, 39, 1, 35, 29, 16, 23, 24, 36, 3, 25, 48, 11, 33, 9, 19, 48, 46, 26, 20, 11, 25, 31, 28, 19, 11, 21, 5, 2, 23, 29, 9, 21, 40, 25, 13, 39, 8, 15, 39, 34, 31, 37, 36, 16, 7, 38, 9, 31, 45, 39, 18, 40, 31, 33, 29, 14, 38, 33, 8, 21, 1, 29, 24, 16, 26, 1, 6, 45, 45, 21, 6, 46, 42, 14, 4, 40, 32, 39, 47, 33, 43, 9, 34, 9, 10, 7, 43, 41, 30, 2, 45, 6, 6, 0, 40, 10, 35, 44, 13, 17, 15, 8, 29, 25, 41, 7, 35, 37, 40, 4, 23, 27, 46, 46, 15, 47, 17, 25, 20, 45, 4, 34, 49, 47, 27, 10, 45, 5, 2, 25, 35, 36, 2, 5, 16, 40, 20, 1, 1, 14, 8, 16, 25, 42, 12, 15, 37, 17, 24, 1, 39, 12, 49, 41, 39, 22, 16, 46, 2, 45, 2, 37, 24, 19, 29, 43, 18, 4, 2, 16, 15, 7, 39, 9, 27, 21, 49, 43, 10, 2, 9, 29, 29, 19, 29, 28, 48, 40, 29, 12, 49, 23, 31, 31, 29, 47, 26, 45, 42, 27, 11, 41, 28, 43, 35, 49, 29, 21, 47, 30, 24, 30, 13, 49, 22, 44, 49, 13, 44, 42, 34, 41, 36, 20, 29, 23, 46, 28, 7, 0, 31, 5, 10, 44, 6, 8, 42, 48, 4, 0, 16, 39, 37, 42, 42, 15, 7, 31, 27, 37, 6, 33, 30, 7, 7, 2, 0, 15, 39, 31, 37, 31, 21, 27, 15, 14, 42, 47, 47, 49, 31, 4, 19, 37, 20, 44, 23, 31, 27, 40, 20, 16, 26, 26, 21, 28, 34, 6, 43, 12, 29, 45, 34, 32, 8, 46, 45, 0, 43, 36, 48, 18, 22, 25, 15, 37, 49, 10, 26, 25, 3, 49, 41, 4, 13, 2, 20, 37, 7, 0, 28, 6, 34, 19, 11, 13, 48, 33, 13, 13, 0, 17, 17, 15, 40, 31, 10, 42, 33, 13, 6, 6, 44, 19, 8, 30, 6, 37, 10, 1, 30, 9, 6, 43, 9, 2, 6, 36, 18, 48, 37, 4, 1, 46, 24, 28, 36, 30, 3, 22, 49, 44, 13, 39, 5, 12, 41, 24, 10, 1, 22, 22, 9, 33, 49, 1, 28, 4, 27, 26, 18, 21, 47, 47, 31, 37, 19, 9, 18, 41, 15, 38, 26, 22, 23, 26, 30, 16, 22, 12, 30, 16, 4, 29, 36, 43, 30, 15, 26, 19, 42, 20, 38, 38, 33, 13, 34, 5, 38, 21, 0, 21, 46, 16, 41, 46, 32, 30, 44, 23, 8, 4, 29, 9, 15, 5, 30, 45, 18, 49, 39, 10, 1, 22, 46, 43, 4, 37, 45, 31, 25, 36, 48, 12, 5, 36, 27, 12, 32, 42, 22, 42, 11, 25, 20, 29, 7, 10, 25, 25, 37, 4, 3, 6, 1, 15, 42, 37, 8, 45, 35, 4, 9, 35, 7, 21, 32]                                                                                                                           |486           |256         |
|296      |[0, 11, 16, 35, 18, 3, 16, 5, 23, 3, 29, 41, 1, 30, 21, 10, 33, 7, 45, 49, 0, 47, 24, 41, 32, 46, 31, 26, 42, 3, 26, 4, 48, 37, 10, 44, 6, 41, 23, 47, 0, 49, 43, 37, 9, 8, 15, 11, 48, 15, 30, 26, 42, 6, 14, 24, 14, 48, 18, 48, 37, 4, 1, 31, 25, 49, 0, 45, 24, 27, 21, 13, 7, 45, 46, 26, 46, 43, 20, 23, 47, 37, 14, 27, 26, 27, 7, 35, 29, 48, 45, 7, 10, 17, 1, 9, 16, 9, 7, 14, 46, 29, 36, 8, 43, 22, 10, 5, 39, 3, 44, 47, 41, 1, 20, 5, 37, 47, 29, 39, 26, 6, 33, 41, 39, 35, 26, 7, 3, 34, 11, 27, 21, 27, 30, 21, 6, 1, 42, 29, 27, 28, 3, 47, 30, 12, 3, 26, 49, 34, 5, 10, 6, 4, 5, 22, 10, 9, 24, 18, 27, 40, 8, 25, 30, 25, 6, 37, 39, 3, 16, 28, 25, 38, 32, 25, 5, 23, 8, 42, 33, 10, 13, 33, 20, 42, 20, 14, 2, 33, 14, 23, 35, 11, 32, 3, 18, 24, 47, 8, 38, 39, 29, 0, 2, 26, 43, 18, 2, 37, 8, 15, 25, 41, 3, 37, 38, 41, 13, 21, 19, 12, 12, 33, 26, 20, 30, 24, 44, 30, 6, 30, 20, 45, 25, 31, 15, 17, 17, 43, 23, 4, 25, 5, 2, 22, 48, 27, 25, 15, 34, 29, 17, 18, 9, 24, 4, 47, 24, 27, 29, 9, 40, 45, 15, 0, 13, 46, 30, 4, 46, 38, 4, 26, 11, 13, 42, 45, 3, 8, 28, 9, 30, 4, 41, 18, 18, 6, 38, 18, 33, 26, 18, 1, 25, 44, 16, 21, 28, 12, 29, 13, 28, 28, 28, 19, 13, 5, 43, 19, 19, 22, 11, 27, 14, 26, 20, 12, 11, 12, 29, 6, 3, 45, 7, 44, 38, 28, 36, 26, 33, 13, 27, 10, 34, 23, 28, 12, 0, 15, 41, 44, 48, 22, 31, 24, 44, 22, 20, 3, 15, 0, 4, 35, 31, 18, 45, 41, 1, 21, 43, 26, 6, 46, 35, 40, 45, 41, 31, 6, 25, 21, 0, 12, 31, 40, 11, 34, 19, 24, 20, 15, 25, 27, 25, 45, 29, 21, 14, 14, 6, 40, 20, 10, 13, 40, 12, 5, 30, 13, 36, 35, 5, 10, 11, 9, 44, 10, 44, 1, 44, 41, 2, 10, 40, 35, 20, 31, 33, 27, 43, 5, 9, 3, 28, 36, 35, 12, 0, 43, 49, 14, 24, 36, 47, 45, 41, 48, 31, 8, 42, 4, 9, 1, 17, 49, 1, 28, 41, 45, 15, 4, 26, 38, 12, 0, 0, 21, 46, 10, 0, 12, 7, 47, 45, 9, 49, 36, 30, 8, 35, 37, 34, 24, 29, 37, 2, 48, 29, 36, 13, 31, 25, 48, 1, 43, 21, 34, 40, 24, 44, 49, 46, 6, 35, 44, 22, 42, 33, 18, 39, 14, 15, 27, 40, 39, 23, 14]                                                                                                             |489           |258         |
|321      |[23, 16, 24, 9, 23, 23, 8, 7, 10, 39, 0, 21, 3, 6, 24, 43, 15, 16, 18, 13, 17, 39, 2, 38, 41, 2, 25, 14, 11, 8, 43, 31, 49, 34, 10, 35, 22, 7, 35, 7, 9, 11, 20, 44, 49, 34, 12, 4, 44, 27, 13, 32, 30, 5, 46, 0, 16, 5, 19, 41, 47, 45, 26, 47, 33, 47, 14, 26, 34, 26, 38, 3, 8, 10, 18, 39, 7, 4, 40, 19, 16, 42, 30, 36, 1, 46, 16, 49, 22, 47, 35, 30, 0, 27, 44, 2, 8, 6, 8, 9, 6, 15, 29, 40, 2, 23, 34, 10, 12, 12, 29, 21, 43, 37, 20, 27, 22, 27, 7, 42, 34, 5, 7, 30, 43, 22, 48, 14, 15, 40, 23, 48, 15, 14, 41, 9, 6, 5, 47, 32, 32, 1, 9, 27, 20, 10, 27, 35, 49, 40, 20, 40, 5, 10, 14, 4, 15, 14, 28, 15, 12, 5, 17, 30, 30, 15, 15, 13, 37, 4, 42, 29, 35, 1, 45, 23, 9, 7, 6, 40, 11, 7, 38, 40, 26, 6, 37, 20, 37, 47, 3, 0, 13, 2, 2, 40, 9, 33, 32, 48, 11, 25, 28, 30, 4, 32, 6, 8, 20, 2, 21, 20, 40, 43, 44, 39, 6, 16, 48, 2, 4, 38, 43, 34, 31, 16, 35, 8, 28, 43, 12, 21, 45, 34, 7, 25, 32, 46, 48, 29, 39, 20, 8, 33, 19, 0, 29, 22, 42, 19, 41, 40, 32, 39, 13, 33, 47, 30, 4, 48, 39, 22, 26, 27, 30, 5, 34, 24, 48, 18, 0, 37, 45, 41, 27, 8, 39, 5, 6, 10, 28, 27, 5, 49, 29, 2, 37, 39, 36, 14, 40, 44, 39, 31, 30, 23, 44, 41, 20, 1, 7, 31, 1, 12, 27, 4, 5, 3, 14, 4, 7, 3, 43, 46, 2, 0, 17, 25, 2, 25, 43, 40, 10, 28, 22, 32, 17, 25, 40, 14, 14, 43, 22, 30, 20, 1, 29, 7, 29, 29, 36, 2, 5, 16, 16, 5, 6, 6, 16, 17, 30, 28, 24, 14, 27, 14, 37, 7, 38, 43, 12, 10, 9, 47, 34, 6, 47, 8, 20, 30, 34, 17, 12, 34, 45, 2, 17, 32, 9, 28, 1, 18, 22, 26, 29, 48, 21, 32, 15, 42, 10, 42, 35, 3, 16, 12, 7, 23, 8, 3, 34, 1, 13, 46, 7, 25, 26, 31, 11, 29, 49, 9, 2, 39, 41, 25, 1, 19, 41, 2, 46, 44, 22, 27, 26, 12, 22, 29, 34, 42, 44, 40, 40, 0, 40, 28, 36, 13, 21, 48, 27, 17, 29, 11, 16, 26, 4, 24, 30, 15, 37, 11, 20, 46, 47, 20, 19, 10, 24, 4, 26, 1, 8, 36, 10, 39, 33, 45, 11, 17, 11, 25, 24, 10, 2]                                                                                                                                                                                                                                                       |495           |261         |
|322      |[0, 31, 12, 3, 18, 32, 18, 40, 20, 22, 11, 46, 13, 31, 13, 16, 28, 42, 29, 7, 45, 32, 47, 49, 26, 38, 6, 16, 28, 12, 23, 43, 12, 45, 10, 32, 19, 45, 30, 1, 37, 12, 2, 25, 16, 11, 40, 33, 31, 28, 2, 46, 21, 45, 42, 47, 22, 6, 33, 22, 4, 22, 8, 42, 48, 11, 21, 6, 32, 8, 29, 37, 16, 35, 40, 6, 5, 45, 48, 6, 1, 21, 33, 8, 19, 31, 22, 39, 23, 17, 43, 38, 17, 41, 0, 4, 26, 19, 44, 15, 42, 33, 47, 20, 20, 1, 8, 19, 32, 33, 19, 14, 12, 42, 36, 25, 9, 8, 18, 12, 7, 5, 41, 8, 40, 18, 5, 42, 46, 34, 34, 30, 24, 44, 20, 12, 39, 39, 31, 42, 12, 16, 24, 21, 42, 34, 6, 43, 23, 13, 32, 44, 27, 4, 48, 35, 22, 2, 0, 37, 31, 36, 16, 9, 45, 30, 45, 3, 20, 37, 15, 18, 8, 27, 43, 20, 22, 36, 40, 11, 1, 30, 30, 26, 35, 36, 45, 17, 22, 42, 19, 18, 7, 8, 2, 1, 42, 19, 0, 29, 30, 42, 29, 2, 19, 33, 42, 23, 36, 33, 4, 34, 43, 40, 46, 22, 49, 42, 31, 41, 29, 15, 20, 5, 23, 31, 46, 41, 5, 15, 25, 36, 17, 6, 28, 48, 26, 15, 32, 24, 27, 14, 13, 36, 32, 46, 21, 47, 18, 42, 8, 1, 47, 49, 10, 4, 11, 21, 5, 42, 22, 0, 46, 18, 0, 28, 12, 37, 29, 5, 48, 19, 45, 23, 20, 20, 25, 5, 16, 32, 4, 2, 7, 19, 9, 32, 39, 25, 8, 35, 38, 15, 0, 34, 0, 32, 43, 13, 44, 35, 0, 33, 43, 35, 27, 39, 43, 33, 23, 44, 6, 48, 20, 14, 42, 46, 2, 25, 35, 28, 14, 37, 25, 33, 21, 35, 19, 37, 12, 44, 49, 29, 0, 9, 26, 8, 45, 48, 22, 26, 17, 17, 24, 4, 19, 23, 38, 15, 25, 37, 26, 27, 43, 34, 29, 8, 34, 28, 35, 36, 38, 4, 2, 10, 2, 2, 3, 36, 24, 34, 8, 8, 7, 32, 17, 27, 38, 29, 43, 0, 0, 47, 11, 48, 5, 19, 23, 36, 38, 2, 5, 19, 35, 39, 19, 18, 43, 48, 29, 4, 18, 13, 15, 37, 7, 48, 6, 27, 1, 46, 49, 49, 3, 28, 24, 47, 13, 47, 1, 34, 18, 41, 10, 9, 18, 28, 31, 3, 46, 32, 29, 6, 16, 25, 37, 2, 30, 42, 20, 39, 19, 40, 21, 0, 44, 49, 49, 2, 25, 30, 10, 29, 43, 10, 21, 33, 42, 17, 48, 48, 46, 17, 47, 22]                                                                                                                                                                                                                                                                               |506           |266         |
|362      |[16, 42, 36, 11, 4, 14, 45, 6, 44, 21, 48, 44, 49, 45, 25, 19, 21, 39, 6, 2, 34, 29, 28, 26, 9, 37, 11, 17, 26, 2, 38, 46, 4, 32, 39, 26, 31, 39, 20, 15, 31, 19, 46, 14, 31, 0, 13, 25, 0, 5, 21, 19, 23, 1, 5, 12, 31, 24, 44, 6, 39, 39, 38, 32, 37, 24, 30, 27, 11, 36, 10, 48, 46, 21, 18, 41, 44, 46, 0, 16, 24, 25, 25, 14, 15, 15, 46, 7, 8, 47, 31, 2, 41, 31, 37, 36, 49, 21, 7, 42, 1, 44, 25, 27, 29, 46, 46, 37, 17, 25, 7, 5, 10, 20, 42, 8, 0, 28, 10, 46, 41, 38, 13, 6, 48, 0, 31, 19, 23, 33, 0, 40, 13, 33, 41, 28, 49, 29, 9, 39, 7, 1, 29, 36, 20, 45, 37, 13, 18, 13, 38, 46, 6, 24, 47, 27, 47, 19, 17, 31, 10, 49, 7, 31, 27, 40, 0, 5, 33, 46, 17, 8, 36, 16, 6, 19, 0, 33, 23, 18, 8, 12, 34, 7, 29, 15, 23, 23, 16, 16, 41, 37, 31, 34, 43, 16, 9, 46, 45, 42, 24, 28, 46, 43, 25, 44, 0, 4, 43, 31, 12, 49, 30, 31, 29, 9, 29, 23, 17, 41, 27, 47, 4, 31, 44, 9, 30, 11, 32, 13, 16, 44, 16, 12, 16, 49, 37, 4, 36, 22, 37, 48, 41, 38, 27, 19, 4, 24, 7, 28, 10, 27, 15, 25, 35, 49, 48, 21, 38, 45, 38, 16, 21, 23, 34, 32, 47, 10, 11, 3, 42, 3, 1, 42, 0, 11, 22, 40, 6, 37, 9, 6, 15, 32, 45, 0, 45, 11, 31, 23, 10, 42, 3, 31, 21, 14, 32, 42, 14, 42, 43, 44, 32, 34, 33, 41, 0, 7, 13, 42, 6, 34, 44, 20, 18, 7, 11, 46, 1, 40, 36, 1, 14, 43, 23, 8, 1, 16, 16, 4, 38, 41, 15, 28, 31, 22, 9, 21, 15, 19, 34, 35, 4, 14, 45, 31, 3, 5, 13, 14, 17, 11, 23, 34, 34, 25, 18, 39, 23, 15, 29, 17, 7, 24, 14, 22, 9, 37, 17, 12, 20, 22, 16, 7, 22, 16, 20, 39, 35, 31, 27, 40, 20, 10, 32, 44, 28, 19, 36, 44, 8, 13, 9, 46, 24, 7, 10, 45, 39, 1, 41, 20, 39, 42, 35, 35, 10, 15, 48, 27, 28, 42, 2, 30, 46, 46, 16, 49, 3, 48, 14, 17, 24, 7, 11, 4, 48, 29, 34, 18, 46, 41, 45, 29, 37, 18, 38, 10, 42, 21, 49, 26, 15, 18, 5, 40, 13, 46, 1, 18, 18, 41, 42, 7, 27, 6, 9, 3, 29, 36, 34, 29, 18, 1, 2, 34, 28, 30, 13, 34, 47, 42, 4, 6, 15, 2, 36, 34, 49, 16, 49, 3, 21, 14, 23, 5, 23, 43, 31, 4, 37, 23, 10, 17, 39, 22, 7, 19, 31, 5, 28, 15, 29, 46, 22, 22, 24, 24, 35, 11]                                                                                          |470           |249         |
+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+------------+
only showing top 20 rows
"""
```

### mapInPandas
The final approach to distributing custom Pandas functions is mapInPandas. In our mapInPandas function, we can return many rows for each input row, meaning it operates in an opposite manner to applyInPandas. We’ll use a Python Iterator for this, allowing us to be flexible in how many rows we yield. In our simple example below, we’ll convert the granularity of the DataFrame back to one row per combination of trip_id and device_id. Note that this example is illustrative - we could simply use Spark’s native explode() function and get the same result but more performant. For a more realistic use of this approach, read the blog post referenced above which describes how to use mapInPandas to process uncommon file types. 

```python
from collections.abc import Iterator


def renormalize(itr: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for pdf in itr:
        # Unpack the list of values from the trip_id column into their own rows
        pdf = pdf.explode('trip_id')
        yield pdf

expected_schema = 'device_id int, trip_id int, sensor_reading long, sqrt_reading long'
df = df.mapInPandas(renormalize, schema=expected_schema)
df.orderBy("device_id").display()
df.filter("device_id = 26").display()
```


## Find duplicates and non-duplicates

### (1) `exceptAll()` what is pyspark "exceptAll()" function? Explain it with example

The `exceptAll()` function in PySpark is used to return a new DataFrame containing rows in the first DataFrame but not in another DataFrame, while preserving duplicates. This is equivalent to `EXCEPT ALL` in SQL. As standard in SQL, this function resolves columns by position (not by name)¹.

Here's an example:

```python
# Create two DataFrames
df1 = spark.createDataFrame(
    [("a", 1), ("a", 1), ("a", 1), ("a", 2), ("b", 3), ("c", 4)], ["C1", "C2"]
)
df2 = spark.createDataFrame([("a", 1), ("b", 3)], ["C1", "C2"])

# Use exceptAll() function
result = df1.exceptAll(df2)

# Show the result
result.show()
```

Output:

```
+---+---+
| C1| C2|
+---+---+
|  a|  1|
|  a|  1|
|  a|  2|
|  c|  4|
+---+---+
```

In this example, the `exceptAll()` function returns all rows from `df1` that are not in `df2`, while preserving duplicates¹. So, even though the value `("a", 1)` appears three times in `df1` and once in `df2`, two instances of it are still present in the resulting DataFrame. This is because `exceptAll()` preserves duplicates.

Source: Conversation with Bing, 18/04/2024
(1) pyspark.sql.DataFrame.exceptAll — PySpark master documentation. https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.exceptAll.html.
(2) pyspark.sql.DataFrame.exceptAll — PySpark master documentation. https://api-docs.databricks.com/python/pyspark/latest/pyspark.sql/api/pyspark.sql.DataFrame.exceptAll.html.
(3) PySpark: Get Rows Which Are Not in Another DataFrame. https://www.statology.org/pyspark-get-rows-not-in-another-dataframe/.
(4) github.com. https://github.com/PandaGege/big_data/tree/eb96d4439611337f4d95764b65ccbd13a270b899/spark%2Fsql_dataframe_guide.py.
(5) github.com. https://github.com/bhishanpdl/Big_Data_Analysis/tree/b4aec3a0e285de5cac02ad7390712635a73a24db/a01_PySpark%2Fa01_Pyspark_Basics%2FREADME.md.
(6) github.com. https://github.com/Sapphirine/Esport-Game-Prediction/tree/243a729cd395a4fb63a800ac88a13b55bdbd3989/dashboard%2Fvenv%2Flib%2Fpython2.7%2Fsite-packages%2Fpyspark%2Fsql%2Fdataframe.py.


### (2) `subtract()` - Is this the same as "subtract()"? Can I use join (but which type of join) to achieve this?

The `subtract()` function in PySpark is similar to `exceptAll()`, but there's a key difference: `subtract()` is equivalent to `EXCEPT DISTINCT` in SQL¹. This means that `subtract()` removes all duplicates. If a row appears in both DataFrames, it will be removed entirely from the first DataFrame¹.

Here's an example:

```python
# Create two DataFrames
df1 = spark.createDataFrame(
    [("a", 1), ("a", 1), ("b", 3), ("c", 4)], ["C1", "C2"]
)
df2 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3)], ["C1", "C2"])

# Use subtract() function
result = df1.subtract(df2)

# Show the result
result.show()
```

Output:

```
+---+---+
| C1| C2|
+---+---+
|  c|  4|
+---+---+
```

In this example, the `subtract()` function removes all instances of `("a", 1)` from `df1` because it appears in `df2`¹.

As for using a join operation to achieve the same result as `exceptAll()`, you can use a `LEFT ANTI JOIN`. This type of join returns only the rows from the left DataFrame that are not present in the right DataFrame². Here's how you can do it:

```python
# Use left anti join
result = df1.join(df2, ["C1", "C2"], "left_anti")

# Show the result
result.show()
```

This will give you the same result as `df1.exceptAll(df2)`. However, please note that this method requires the join to be performed on all columns. If you want to perform the operation based on specific columns, you would need to adjust the join condition accordingly².

Source: Conversation with Bing, 18/04/2024
(1) pyspark.sql.DataFrame.subtract — PySpark master documentation. https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.subtract.html.
(2) Spark: subtract two DataFrames - Stack Overflow. https://stackoverflow.com/questions/29537564/spark-subtract-two-dataframes.
(3) python - Subtract in pyspark dataframe - Stack Overflow. https://stackoverflow.com/questions/57483536/subtract-in-pyspark-dataframe.
(4) How to subtract two columns of pyspark dataframe and also divide?. https://stackoverflow.com/questions/57105403/how-to-subtract-two-columns-of-pyspark-dataframe-and-also-divide.
(5) pyspark.sql.DataFrame.exceptAll — PySpark master documentation. https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.exceptAll.html.
(6) Keeping identifier in exceptAll in PySpark - Stack Overflow. https://stackoverflow.com/questions/59762501/keeping-identifier-in-exceptall-in-pyspark.
(7) pyspark.sql.DataFrame.exceptAll — PySpark master documentation. https://api-docs.databricks.com/python/pyspark/latest/pyspark.sql/api/pyspark.sql.DataFrame.exceptAll.html.
(8) github.com. https://github.com/intel-analytics/BigDL/tree/11a0600676f8c400fa2686f5a52d70dcc754b728/ppml%2Ftrusted-big-data-ml%2Fpython%2Fdocker-graphene%2Fexamples%2Fsql_example.py.

### `left anti join` - how to get the duplicates and non-duplicates if I use this "left anti" join?

To get the duplicates and non-duplicates using a "left anti" join in PySpark, you can follow these steps:

1. **Non-Duplicates**: The result of a "left anti" join itself gives you the non-duplicate records. These are the records that are present in the left DataFrame but not in the right DataFrame².

```python
non_duplicates = df1.join(df2, ["C1", "C2"], "left_anti")
non_duplicates.show()
```

2. **Duplicates**: To get the duplicate records, you can first find all records in the left DataFrame, then subtract the non-duplicates from it³.

```python
all_records = df1
duplicates = all_records.subtract(non_duplicates)
duplicates.show()
```

In this example, `all_records` is a DataFrame that contains all records from `df1`. `non_duplicates` is the DataFrame we obtained from the "left anti" join, which contains all records from `df1` that are not in `df2`. The `subtract()` function then gives us all records from `all_records` that are not in `non_duplicates`, which are the duplicate records³.

Please note that this method considers all columns for identifying duplicates. If you want to consider specific columns, you would need to adjust the join and subtract conditions accordingly³. Also, be aware that null values are treated as distinct in PySpark, so you might need to handle them separately if your data contains null values¹.

Source: Conversation with Bing, 18/04/2024
(1) PySpark SQL Left Anti Join with Example - Spark By Examples. https://sparkbyexamples.com/pyspark/pyspark-sql-left-anti-join-with-example/.
(2) Solved: How to get all occurrences of duplicate records in .... https://community.databricks.com/t5/data-engineering/how-to-get-all-occurrences-of-duplicate-records-in-a-pyspark/td-p/19818.
(3) pyspark - Left Anti join not consider null as duplicate values in Spark .... https://stackoverflow.com/questions/62423032/left-anti-join-not-consider-null-as-duplicate-values-in-spark.
(4) Spark SQL Left Anti Join with Example - Spark By {Examples}. https://sparkbyexamples.com/spark/spark-sql-left-anti-join-with-example/.
(5) undefined. https://spark.apache.org/docs/3.0.0-preview/sql-ref-null-semantics.html.


# Databricks

## Connect to Azure Data Lake Storage Gen2 and Blob Storage

* https://docs.databricks.com/en/connect/storage/azure-storage.html
* https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes

Need to store secrets (=key-value pair) in Azure Key Vault first, then create a secret scope backed by Azure Key Vault in Databricks.

Because the Azure Key Vault-backed secret scope is a __read-only__ interface to the Key Vault, the PutSecret and DeleteSecret the Secrets API operations are not allowed.

```python
service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")
```

Replace
* <secret-scope> with the Databricks secret scope name.
* <service-credential-key> with the name of the key containing the client secret.
* <storage-account> with the name of the Azure storage account.
* <application-id> with the Application (client) ID for the Microsoft Entra ID application.
* <directory-id> with the Directory (tenant) ID for the Microsoft Entra ID application.

### Access Files in Azure Storage Account

```python
spark.read.load("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")

dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")

### from mount point
import pandas as pd
pd.read_csv("file:/dbfs/mnt/ext-folder/UAT/logo/log/tmp.csv")
```

## Write string to a single .txt file

```python
# Define the file path and string content
file_path = "/mnt/databricks-mount/your_file_name.txt"
content = "Your string content"

# Save the string content as a .txt file in Azure Blob Storage
dbutils.fs.put(file_path, content, overwrite=True)

print("File uploaded successfully.")
```

## Asynchronous logic from Databricks

### Async download images to local then upload to Azure Blob Storage

* https://poe.com/s/vcyaLuQpk49VG5A4tIBa
* https://poe.com/s/38ETD8ZpgOlhHYYht3nX

```python
import nest_asyncio     ### need to add if using async in Databricks
nest_asyncio.apply()    ### https://community.databricks.com/t5/data-engineering/asynchronous-api-calls-from-databricks/td-p/4691

################################

import aiohttp
import asyncio
from azure.storage.blob.aio import BlobServiceClient

async def download_image(session, url, destination):
    async with session.get(url) as response:
        with open(destination, 'wb') as file:
            while True:
                chunk = await response.content.read(8192)
                if not chunk:
                    break
                file.write(chunk)

async def upload_image(blob_client, source, destination):
    with open(source, "rb") as data:
        await blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True, blob_name=destination)

async def download_images_and_upload_to_azure(urls, destination_folder, connection_string, container_name):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for i, url in enumerate(urls):
            destination = f"{destination_folder}/image_{i + 1}.jpg"
            tasks.append(download_image(session, url, destination))

        await asyncio.gather(*tasks)

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    for i, url in enumerate(urls):
        source = f"{destination_folder}/image_{i + 1}.jpg"
        destination = f"images/image_{i + 1}.jpg"
        await upload_image(container_client.get_blob_client(destination), source, destination)

async def main():
    # Define the list of image URLs
    image_urls = [
        "https://commons.wikimedia.org/wiki/File:ChatGPT_logo.svg#/media/File:ChatGPT_logo.svg",
        "https://commons.wikimedia.org/wiki/File:Chatgpt_idwiktionary.jpg#/media/File:Chatgpt_idwiktionary.jpg",
        "https://upload.wikimedia.org/wikipedia/commons/4/4d/OpenAI_Logo.svg"
    ]

    # Define the destination folder to save the downloaded images
    destination_folder = "downloaded_images"

    # Define your Azure Storage connection string and container name
    connection_string = "<your_connection_string>"
    container_name = "<your_container_name>"

    # Start the download and upload process
    await download_images_and_upload_to_azure(image_urls, destination_folder, connection_string, container_name)
    print("Images downloaded and uploaded successfully.")

# Run the asyncio event loop
asyncio.run(main())
```


# Graph, edge, vertice, Graphframe

Credit to [link](https://github.com/cenzwong/tech/tree/master/Note/Spark#graphframe)

## `GraphFrame(v, e)`, Create GraphFrame

```python
# Vertics DataFrame
v = spark.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 37),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 38),
  ("g", "Gabby", 60)
], ["id", "name", "age"])

# Edges DataFrame
e = spark.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"), # b and c follow each other
  ("c", "b", "follow"), #
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend"),
  ("g", "e", "follow")
], ["src", "dst", "relationship"])

# Create a GraphFrame
g = GraphFrame(v, e)

g.vertices.show()
g.edges.show()
```

```python
# Vertics DataFrame
v = spark.createDataFrame([
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 37),
    ("d", "David", 29),
    ("e", "Esther", 32),
    ("f", "Fanny", 38),
    ("g", "Gabby", 60)
], ["id", "name", "age"])
# Edges DataFrame
e = spark.createDataFrame([
    ("a", "b", "follow"),
    ("c", "a", "friend"),
    ("b", "c", "follow"),
    ("d", "a", "follow"),
    ("f", "c", "follow"),
    ("f", "d", "follow"),
    ("f", "b", "follow"),
    ("c", "d", "follow"),
    ("g", "a", "friend"),
    ("g", "d", "friend"),
    ("g", "c", "friend"),
    ("e", "a", "follow"),
    ("e", "d", "follow")
], ["src", "dst", "relationship"])

# Create a GraphFrame
g = GraphFrame(v, e)
```

## Explore `GraphFrame`

Credit to [link](https://github.com/cenzwong/tech/tree/master/Note/Spark#graphframe)

```python
g.triplets.show() # display all
g.vertices.show() # display vertices
g.edges.show()    # display edges
g.degrees.show()
g.inDegrees.show()
g.outDegrees.show()
```

## Filter, `g.filterVerices()` `g.filterEdges()`

Returns `GraphFrame`, not `DataFrame`.

Credit to [link](https://github.com/cenzwong/tech/tree/master/Note/Spark#graphframe)

```python
g.filterVerices("columnName > 30")
g.filterEdges("columnName = 30")
g.dropIsolatedVertices() #Drop isolated vertices (users) which are not contained in any edges (relationships).
                         #Vertices without incoming / outgoing edges
```

## `.find("(a)-[e]->(b)")`, Motif finding

Find the edges `e` from vertex `a` to vertex `b`.

P.S. `.find()` returns `sparkDF` DataFrame.

Credit to [link](https://github.com/cenzwong/tech/tree/master/Note/Spark#graphframe)

```python
g.find("(a)-[]->(b);(b)-[]->(a)").filter("a.id < b.id") # A and B follow/friend each other; 
                                                        # .filter() out "B follows/friends back A" rows, 
                                                        # just keeps "A follows/friends B" rows
g.find("(a)-[]->(b); !(b)-[]->(a)").filter("a.id < b.id") # jsut A follows B, B not follows A
g.find("!()-[]->(a)") # find vertices without incoming edges
g.find("(a)-[e]->(b)").filter("e.relationship = 'follow'") # find A follows B,
```

## Subgraphs

Credit to `msbd5003`.

```python
# Build subgraph based on conditions, i.e. subgraph contains (v,e)
# Select subgraph of users older than 30, and relationships of type "friend".
# Drop isolated vertices (users) which are not contained in any edges (relationships).
g1 = g.filterVertices("age > 30").filterEdges("relationship = 'friend'")\
      .dropIsolatedVertices()
g1.vertices.show()
g1.edges.show()
```
Output:
```
+---+------+---+
| id|  name|age|
+---+------+---+
|  e|Esther| 32|
|  b|   Bob| 36|
|  a| Alice| 34|
+---+------+---+
+---+---+------------+
|src|dst|relationship|
+---+---+------------+
|  a|  e|      friend|
|  a|  b|      friend|
+---+---+------------+
```

# Database Connection

https://medium.com/@uzzaman.ahmed/pyspark-dataframe-api-read-and-write-data-from-databases-5b58548d1baa

Reading and writing to SQL Server:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("database-read-write") \
    .getOrCreate()

url = "jdbc:sqlserver://<hostname>:<port>;database=<database_name>"
properties = {
    "user": "<username>",
    "password": "<password>",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

table_name = "<table_name>"
df = spark.read.jdbc(url=url, table=table_name, properties=properties)

mode = "overwrite"
df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)
```

## Read Database Concurrently

https://stackoverflow.com/questions/53404288/only-single-thread-executes-parallel-sql-query-with-pyspark-using-multiprocessin

Setup:
```python
driverPath = r'C:\src\NetSuiteJDBC\NQjc.jar'
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-class-path '{0}' --jars '{0}' --master local[*] --conf 'spark.scheduler.mode=FAIR' --conf 'spark.scheduler.allocation.file=C:\\src\\PySparkConfigs\\fairscheduler.xml' pyspark-shell".format(driverPath)
)

import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Column, Row, SQLContext
from pyspark.sql.functions import col, split, regexp_replace, when
from pyspark.sql.types import ArrayType, IntegerType, StringType

spark = SparkSession.builder.appName("sparkNetsuite").getOrCreate()
spark.sparkContext.setLogLevel("INFO")
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
sc = SparkContext.getOrCreate()
```

JDBC connection logic:
```python
# In sparkMethods.py file:
def getAndSaveTableInPySpark(tableName):
    import os
    import os.path
    from pyspark.sql import SparkSession, SQLContext
    spark = SparkSession.builder.appName("sparkNetsuite").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")

    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", "OURCONNECTIONURL;") \
        .option("driver", "com.netsuite.jdbc.openaccess.OpenAccessDriver") \
        .option("dbtable", tableName) \
        .option("user", "USERNAME") \
        .option("password", "PASSWORD") \
        .load()

    filePath = "C:\\src\\NetsuiteSparkProject\\" + tableName + "\\" + tableName + ".parquet"
    jdbcDF.write.parquet(filePath)
    fileExists = os.path.exists(filePath)
    if(fileExists):
        return (filePath + " exists!")
    else:
        return (filePath + " could not be written!")
```

Multi-thread read Database:
```python
from multiprocessing.pool import ThreadPool
pool = ThreadPool(5)
results = pool.map(sparkMethods.getAndSaveTableInPySpark, top5Tables)
pool.close() 
pool.join() 
print(*results, sep='\n')
```

Output:
```shell
C:\src\NetsuiteSparkProject\SALES_TERRITORY_PLAN_PARTNER\SALES_TERRITORY_PLAN_PARTNER.parquet exists!
C:\src\NetsuiteSparkProject\WORK_ORDER_SCHOOLS_TO_INSTALL_MAP\WORK_ORDER_SCHOOLS_TO_INSTALL_MAP.parquet exists!
C:\src\NetsuiteSparkProject\ITEM_ACCOUNT_MAP\ITEM_ACCOUNT_MAP.parquet exists!
C:\src\NetsuiteSparkProject\PRODUCT_TRIAL_STATUS\PRODUCT_TRIAL_STATUS.parquet exists!
C:\src\NetsuiteSparkProject\ACCOUNT_PERIOD_ACTIVITY\ACCOUNT_PERIOD_ACTIVITY.parquet exists!
```


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

#### Method 1 - Setting in Code / Notebook

Set MongoDB connection when create Spark session in Python code.

```python
URL = "mongodb://{USER_NAME}:{PASSWORD}@127.0.0.1:{PORT}/test.application_test?readPreference=primaryPreferred?authMechanism={SCRAM-SHA-1}"
spark = (
  SparkSession.builder.master("local[*]")
            .appName("TASK_NAME")
            .config("spark.mongodb.input.uri", URI)
            .config("spark.mongodb.output.uri", URI)
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
            .config("spark.jars.packages", "org.mongodb:mongo-java-driver:x.x.x")
            .getOrCreate()
            )
```

#### Method 2 - Setting in Terminal

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
from graphframes import *

spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = spark.sparkContext

sc
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