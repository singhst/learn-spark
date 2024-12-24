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
    - [Read csv use double quotation as escape character](#read-csv-use-double-quotation-as-escape-character)
  - [Optimization](#optimization)
    - [\[ing\] Speed Up Reading .csv/.json with schema](#ing-speed-up-reading-csvjson-with-schema)
    - [Multithread submits spark job](#multithread-submits-spark-job)
      - [Sequential Approach](#sequential-approach)
      - [Multithreading Approach](#multithreading-approach)
      - [Potential Use Cases for Multithreading with Spark](#potential-use-cases-for-multithreading-with-spark)
  - [convert Map, Array, or Struct Type Columns into JSON Strings](#convert-map-array-or-struct-type-columns-into-json-strings)
    - [Exaplme Data](#exaplme-data)
    - [`Map` / `MapType` Column to JSON StringType](#map--maptype-column-to-json-stringtype)
    - [`List of MapType` column into JSON StringType](#list-of-maptype-column-into-json-stringtype)
    - [`ArrayType` column into JSON StringType](#arraytype-column-into-json-stringtype)
  - [Change Column Type in a StructType](#change-column-type-in-a-structtype)
  - [Sort Array of Struct by a Field Inside that Struct](#sort-array-of-struct-by-a-field-inside-that-struct)
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
    - [Normal Pyspark UDF `udf()`](#normal-pyspark-udf-udf)
    - [`pandas_udf()`](#pandas_udf)
    - [`applyInPandas()`](#applyinpandas)
    - [`mapInPandas(self_func(Iterator[]))`](#mapinpandasself_funciterator)
      - [Data example of `Iterator[pd.DataFrame]`](#data-example-of-iteratorpddataframe)
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

### Read csv use double quotation as escape character


  <details>
      <summary>example data .csv</summary>
    
  ```
  Organization Name,Organization Name URL,Industries,Headquarters Location,Description,CB Rank (Company),Founded Date,Founded Date Precision,Operating Status,Website,Twitter,Facebook,LinkedIn,Actively Hiring,Number of Sub-Orgs,Full Description,Phone Number,Contact Email,Number of Articles,Hub Tags,Number of Employees,Founders,Last Leadership Hiring Date,Last Layoff Mention Date,Number of Events,Trend Score (7 Days),Trend Score (30 Days),Trend Score (90 Days),Similar Companies,SEMrush - Monthly Visits,SEMrush - Average Visits (6 months),SEMrush - Monthly Visits Growth,SEMrush - Visit Duration,SEMrush - Visit Duration Growth,SEMrush - Page Views / Visit,SEMrush - Page Views / Visit Growth,SEMrush - Bounce Rate Growth,SEMrush - Bounce Rate,SEMrush - Global Traffic Rank,SEMrush - Monthly Rank Change (#),SEMrush - Monthly Rank Growth,Apptopia - Number of Apps,Apptopia - Downloads Last 30 Days,IPO Date,IPO Status,Stock Symbol,Stock Symbol URL,Stock Exchange,Valuation at IPO,Valuation at IPO Currency,Valuation at IPO Currency (in USD),Money Raised at IPO,Money Raised at IPO Currency,Money Raised at IPO Currency (in USD)
  Dunelm,https://www.crunchbase.com/organization/dunelm,"Consumer Goods, E-Commerce, Furniture, Home Decor, Manufacturing, Marketing, Retail","Leicester, Leicester, United Kingdom","Dunelm is the UK's home furnishing retailers. Shop for bedding, curtains, furniture, beds, and mattresses.","126,848",1979-01-01,year,Active,http://www.dunelm.com/,https://twitter.com/DunelmUK?ref_src=twsrc%5Egoogle%7Ctwcamp%5Eserp%7Ctwgr%5Eauthor,https://www.facebook.com/DunelmUK/,https://www.linkedin.com/company/dunelm-soft-furnishings-ltd,,,"Dunelm, the UK's leading home furnishing retailers. Shop for bedding, curtains, furniture, beds and mattresses. It all started 37 years ago in Leicester, when Bill and Jean Adderley opened a home textiles business offering a wide range of products at affordable prices. Their first shops were small high street units, based in a few towns across the East Midlands. But thanks to their philosophy of ""simply value for money""​, the business began to grow and grow. Today, there are now over 150 Dunelm stores right across the UK and Northern Ireland.",084-5165-6565,corporate@dunelm.com,163,,5001-10000,William Adderley,,,,-0.6,-1.3,-0.1,11,"12,937,119","9,698,912.67",22.52%,651,5.85%,1.87,1.85%,-7.64%,63.97%,"8,560","-1,148",-11.83%,1,,2014-11-17,Public,DNLM,https://www.crunchbase.com/ipo/dunelm-ipo--60f5c179,LSE - London Stock Exchange,,,,,,
  Tractor Supply Company,https://www.crunchbase.com/organization/tractor-supply-company,"Agriculture, Retail, Shopping","Brentwood, Tennessee, United States","Tractor Supply Company helps customers find everything they need to maintain their farms, ranches, homes and animals.","113,024",1938-01-01,year,Active,http://www.tractorsupply.com/,https://twitter.com/tractorsupply,https://www.facebook.com/TractorSupplyCo,https://www.linkedin.com/company/tractor-supply-company,,,"Tractor Supply Company is the largest operator of rural lifestyle retail stores in the United States. The company operates over over 2000 retail stores in 49 states, employs more than 45,000 team members and is headquartered in Brentwood, Tenn. Its stock is traded on the NASDAQ exchange under the symbol ""TSCO"".  The company was founded in 1938 as a mail order catalog business offering tractor parts to America's family farmers. Today Tractor Supply is a leading edge retailer with annual revenues of nearly $10 billion.  Tractor Supply stores are located primarily in towns outlying major metropolitan markets and in rural communities. The typical Tractor Supply store has about 16,000 square feet of selling space inside, with a similar amount of outside space.",(877)718-6750,,221,,10001+,Charles E. Schmidt,2019-12-20,,,0,0.5,2.5,8,"18,662,480","19,024,232",17.47%,383,4.64%,2.73,-5.99%,-1.33%,48.04%,"6,212",-419,-6.32%,4,"152,803",1994-02-25,Public,TSCO,https://www.crunchbase.com/ipo/tractor-supply-company-ipo--40d66ee8,NASDAQ,,,,,,
  TRANSSION,https://www.crunchbase.com/organization/transsion,"Manufacturing, Mobile","Shenzhen, Guangdong, China","TRANSSION is a high-tech company specializing in the R&D, production, sale and service of mobile communication products.","105,340",2013-01-01,year,Active,http://transsion.com/,,https://www.facebook.com/Transsion-Holdings-1063184517071454/,https://www.linkedin.com/company/%E4%BC%A0%E9%9F%B3%E6%8E%A7%E8%82%A1%E9%9B%86%E5%9B%A2/,,,"TRANSSION HOLDINGS, the company formerly known as TECNO TELECOM LIMITED, established in July 2006, is a high-tech company specializing in the R&D, production, sale and service of mobile communication products. After years of development, TRANSSION has become an important part of the mobile phone industry and one of the major mobile phone manufacturers in the world. Currently, it has full ownership of three famous mobile phone brands TECNO, itel and Infinix, and an after-sales service brand Carlcare. The company has set up offices in many countries and regions, such as Dubai, Nigeria, Kenya, Tanzania, Cameroon and Bengal, and even has built a factory in Ethiopia, which has provided great support for all its brands.",86-755-33979200,hello@transsion.com,42,,10001+,,2022-01-13,,,-0.6,-0.9,-0.3,10,"67,288","103,266.5",-59.33%,"1,656",12.5%,2.72,-18.55%,21.34%,51.85%,"548,929","274,683",100.16%,2,70,2019-09-25,Public,688036,https://www.crunchbase.com/ipo/transsion-ipo--f0897048,SZSE - Shenzhen Stock Exchange,3950000000,USD,3950000000,400000000,USD,400000000
  ```
  </details>

Code,
```python
import os
df = (
    spark.read
    .option("header", True)
    .option("quote", "\"")
    .option("escape", "\"")
    .option("inferSchema", True)
    .csv(f"file:{os.getcwd()}/csv.csv")
)
df.display()


### output
# +--------------------+---------------------+--------------------+---------------------+--------------------+-----------------+------------+----------------------+----------------+--------------------+--------------------+--------------------+--------------------+---------------+------------------+--------------------+---------------+--------------------+------------------+--------+-------------------+------------------+---------------------------+------------------------+----------------+--------------------+---------------------+---------------------+-----------------+------------------------+-----------------------------------+-------------------------------+------------------------+-------------------------------+----------------------------+-----------------------------------+----------------------------+---------------------+-----------------------------+---------------------------------+-----------------------------+-------------------------+---------------------------------+----------+----------+------------+--------------------+--------------------+----------------+-------------------------+----------------------------------+-------------------+----------------------------+-------------------------------------+
# |   Organization Name|Organization Name URL|          Industries|Headquarters Location|         Description|CB Rank (Company)|Founded Date|Founded Date Precision|Operating Status|             Website|             Twitter|            Facebook|            LinkedIn|Actively Hiring|Number of Sub-Orgs|    Full Description|   Phone Number|       Contact Email|Number of Articles|Hub Tags|Number of Employees|          Founders|Last Leadership Hiring Date|Last Layoff Mention Date|Number of Events|Trend Score (7 Days)|Trend Score (30 Days)|Trend Score (90 Days)|Similar Companies|SEMrush - Monthly Visits|SEMrush - Average Visits (6 months)|SEMrush - Monthly Visits Growth|SEMrush - Visit Duration|SEMrush - Visit Duration Growth|SEMrush - Page Views / Visit|SEMrush - Page Views / Visit Growth|SEMrush - Bounce Rate Growth|SEMrush - Bounce Rate|SEMrush - Global Traffic Rank|SEMrush - Monthly Rank Change (#)|SEMrush - Monthly Rank Growth|Apptopia - Number of Apps|Apptopia - Downloads Last 30 Days|  IPO Date|IPO Status|Stock Symbol|    Stock Symbol URL|      Stock Exchange|Valuation at IPO|Valuation at IPO Currency|Valuation at IPO Currency (in USD)|Money Raised at IPO|Money Raised at IPO Currency|Money Raised at IPO Currency (in USD)|
# +--------------------+---------------------+--------------------+---------------------+--------------------+-----------------+------------+----------------------+----------------+--------------------+--------------------+--------------------+--------------------+---------------+------------------+--------------------+---------------+--------------------+------------------+--------+-------------------+------------------+---------------------------+------------------------+----------------+--------------------+---------------------+---------------------+-----------------+------------------------+-----------------------------------+-------------------------------+------------------------+-------------------------------+----------------------------+-----------------------------------+----------------------------+---------------------+-----------------------------+---------------------------------+-----------------------------+-------------------------+---------------------------------+----------+----------+------------+--------------------+--------------------+----------------+-------------------------+----------------------------------+-------------------+----------------------------+-------------------------------------+
# |              Dunelm| https://www.crunc...|Consumer Goods, E...| Leicester, Leices...|Dunelm is the UK'...|          126,848|  1979-01-01|                  year|          Active|http://www.dunelm...|https://twitter.c...|https://www.faceb...|https://www.linke...|           NULL|              NULL|Dunelm, the UK's ...|  084-5165-6565|corporate@dunelm.com|               163|    NULL|         5001-10000|  William Adderley|                       NULL|                    NULL|            NULL|                -0.6|                 -1.3|                 -0.1|               11|              12,937,119|                       9,698,912.67|                         22.52%|                     651|                          5.85%|                        1.87|                              1.85%|                      -7.64%|               63.97%|                        8,560|                           -1,148|                      -11.83%|                        1|                             NULL|2014-11-17|    Public|        DNLM|https://www.crunc...|LSE - London Stoc...|            NULL|                     NULL|                              NULL|               NULL|                        NULL|                                 NULL|
# |Tractor Supply Co...| https://www.crunc...|Agriculture, Reta...| Brentwood, Tennes...|Tractor Supply Co...|          113,024|  1938-01-01|                  year|          Active|http://www.tracto...|https://twitter.c...|https://www.faceb...|https://www.linke...|           NULL|              NULL|Tractor Supply Co...|  (877)718-6750|                NULL|               221|    NULL|             10001+|Charles E. Schmidt|                 2019-12-20|                    NULL|            NULL|                 0.0|                  0.5|                  2.5|                8|              18,662,480|                         19,024,232|                         17.47%|                     383|                          4.64%|                        2.73|                             -5.99%|                      -1.33%|               48.04%|                        6,212|                             -419|                       -6.32%|                        4|                          152,803|1994-02-25|    Public|        TSCO|https://www.crunc...|              NASDAQ|            NULL|                     NULL|                              NULL|               NULL|                        NULL|                                 NULL|
# |           TRANSSION| https://www.crunc...|Manufacturing, Mo...| Shenzhen, Guangdo...|TRANSSION is a hi...|          105,340|  2013-01-01|                  year|          Active|http://transsion....|                NULL|https://www.faceb...|https://www.linke...|           NULL|              NULL|TRANSSION HOLDING...|86-755-33979200| hello@transsion.com|                42|    NULL|             10001+|              NULL|                 2022-01-13|                    NULL|            NULL|                -0.6|                 -0.9|                 -0.3|               10|                  67,288|                          103,266.5|                        -59.33%|                   1,656|                          12.5%|                        2.72|                            -18.55%|                      21.34%|               51.85%|                      548,929|                          274,683|                      100.16%|                        2|                               70|2019-09-25|    Public|      688036|https://www.crunc...|SZSE - Shenzhen S...|      3950000000|                      USD|                        3950000000|          400000000|                         USD|                            400000000|
# +--------------------+---------------------+--------------------+---------------------+--------------------+-----------------+------------+----------------------+----------------+--------------------+--------------------+--------------------+--------------------+---------------+------------------+--------------------+---------------+--------------------+------------------+--------+-------------------+------------------+---------------------------+------------------------+----------------+--------------------+---------------------+---------------------+-----------------+------------------------+-----------------------------------+-------------------------------+------------------------+-------------------------------+----------------------------+-----------------------------------+----------------------------+---------------------+-----------------------------+---------------------------------+-----------------------------+-------------------------+---------------------------------+----------+----------+------------+--------------------+--------------------+----------------+-------------------------+----------------------------------+-------------------+----------------------------+-------------------------------------+
```

## Optimization

### [ing] Speed Up Reading .csv/.json with schema

Reading .csv/.json by a pre-defined schema can speed up data import, because Spark doesn't need to scan values in each column/attribute to auto-build the schema based on data.

* [Using schemas to speed up reading into Spark DataFrames](https://t-redactyl.io/blog/2020/08/using-schemas-to-speed-up-reading-into-spark-dataframes.html)
* [Spark read JSON with or without schema](https://sparkbyexamples.com/spark/spark-read-json-with-schema/)


### Multithread submits spark job

#### Sequential Approach

* [Spark — Beyond Basics: Multithreading in Spark using Python](https://towardsdev.com/spark-beyond-basics-multithreading-in-spark-using-python-92ae8befc5ed)

  ```python line_numbers
  # List of tables
  TABLES = ["db1.table1", "db1.table2", "db2.table3", "db2.table4", "db3.table5", "db3.table6"]

  # List to keep the dictionary of table_name and respective count
  table_count = []

  # function to get the table records count.
  def get_count(table: str) -> dict:
      count_dict = {}
      count_dict["table_name"] = table
      try:
          count = spark.read.table(table).count()
          count_dict["count"] = count
      except Exception:
          count_dict["count"] = 0
      return count_dict

  def main():
      for table in TABLES:
          table_count.append(get_count(table))


  if __name__ == "__main__":
      main()
      # Creating dataframe from list
      count_df = spark.createDataFrame(table_count).withColumn(
          "date", datetime.now().date()
      )

      # writing into the table
      count_df.coalesce(1).write.insertInto("control_db.counts_table")
  ```

  Spark UI:

  <img src="img/spark-multithreading-sparkui-sequential.webp" style="zoom:80%;" />


#### Multithreading Approach

* [Enhancing Spark Job Performance with Multithreading](https://medium.com/@geekfrosty/enhancing-spark-job-performance-with-multithreading-31118b8ace76)

Only the job submission is happening in parallel here, how long does a job take to complete still depends on the Spark executor configurations provided during spark-submit

  ```python line_numbers
  from concurrent.futures import ThreadPoolExecutor

  # List of tables
  TABLES = ["db1.table1", "db1.table2", "db2.table3", "db2.table4", "db3.table5", "db3.table6"]

  # function to get the table records count.
  def get_count(table: str) -> dict:
      count_dict = {}
      count_dict["table_name"] = table
      try:
          count = spark.read.table(table).count()
          count_dict["count"] = count
      except Exception:
          count_dict["count"] = 0
      return count_dict

  # Code implementation using ThreadPoolExecutor
  def main(TABLES: list) -> None:
      """Main method to submit count jobs in parallel.

      Args:
          TABLES (list): list of table name.

      Raises:
          e: Exception in case of any failures
      """
      with ThreadPoolExecutor(max_workers=6) as executor:
          to_do_map = {}
          for table in TABLES:
              # Submitting jobs in parallel
              future = executor.submit(get_count, table)
              print(f"scheduled for {table}: {future}")
              to_do_map[future] = table
          done_iter = as_completed(to_do_map)

          for future in done_iter:
              try:
                  count = future.result()
                  print("result: ", count)
                  count_list.append(count)
              except Exception as e:
                  raise e

  if __name__ == "__main__":
      # call main method for getting counts in parallel
      main(TABLES)
      
      # count_list -> list of dict containing table_name and count
      print(count_list)
      # Create dataframe
      table_count_df = spark.createDataFrame(count_list)
      table_count_df = table_count_df.withColumn("insert_ts", lit(datetime.now()))

      # Write dataframe into a control table.
      col_order = spark.read.table(CNT_TABLE).limit(1).columns
      table_count_df.select(*col_order).coalesce(1).write.insertInto(
          CNT_TABLE, overwrite=True
      )
  ```

  Explanation:
  * `executor.submit` schedules the `get_count()` to be executed, and returns a `future` representing the pending operation.
  * `concurrent.futures.Future` is an instance of `Future` class represents a deferred computation that may or may not have been completed.
  * `as_complete` takes an iterable of futures and returns an iterator that yields futures as they are completed.
  * `future.result()` get the results of the completed future. *This where exceptions needs to be handled properly for better debugging in case of errors.*

  Spark UI:

  <img src="img/spark-multithreading-sparkui-multithreading.webp" style="zoom:80%;" />

#### Potential Use Cases for Multithreading with Spark
  
  1. Parallel Data Processing:

      When you need to perform independent operations on multiple datasets or tables, multithreading can help speed up the process by utilizing multiple threads to handle different tasks concurrently.
  
  2. Concurrent Data Loading:

      Loading data from multiple sources or tables into Spark DataFrames can be done in parallel, reducing the overall data loading time.
  
  3. Asynchronous Operations:

      For tasks that involve waiting for I/O operations (e.g., reading from databases, writing to storage), multithreading can help keep the CPU busy by performing other tasks while waiting for I/O operations to complete.


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
## Sort Array of Struct by a Field Inside that Struct

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_sort

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Sample data
data = [
    (1, [{"date": "2023-01-12 12:00:00", "value": 10}, {"date": "2023-01-02 13:00:00", "value": 20}]),
    (2, [{"date": "2022-01-03 14:00:00", "value": 30}, {"date": "2023-05-04 15:00:00", "value": 40}, {"date": "2023-02-03 14:00:00", "value": 30}, {"date": "2023-10-04 15:00:00", "value": 40}]),
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "prediction"])
df.show(truncate=False)
df.display()

# Sort the array of structs by the 'date' field
df_sorted = df.withColumn(
    "prediction",
    F.expr("array_sort(prediction, (left, right) -> CASE WHEN left.date < right.date THEN -1 WHEN left.date > right.date THEN 1 ELSE 0 END)")
)

# Show the result
df_sorted.show(truncate=False)
df_sorted.display()

"""output
+---+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|id |prediction                                                                                                                                                                      |
+---+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|1  |[{date -> 2023-01-12 12:00:00, value -> 10}, {date -> 2023-01-02 13:00:00, value -> 20}]                                                                                        |
|2  |[{date -> 2022-01-03 14:00:00, value -> 30}, {date -> 2023-05-04 15:00:00, value -> 40}, {date -> 2023-02-03 14:00:00, value -> 30}, {date -> 2023-10-04 15:00:00, value -> 40}]|
+---+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+


+---+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|id |prediction                                                                                                                                                                      |
+---+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|1  |[{date -> 2023-01-02 13:00:00, value -> 20}, {date -> 2023-01-12 12:00:00, value -> 10}]                                                                                        |
|2  |[{date -> 2022-01-03 14:00:00, value -> 30}, {date -> 2023-02-03 14:00:00, value -> 30}, {date -> 2023-05-04 15:00:00, value -> 40}, {date -> 2023-10-04 15:00:00, value -> 40}]|
+---+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
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
        .withColumn("device_id", (rand() * num_devices).cast("int"))
        .withColumn("trip_id", (rand() * num_trips).cast("int"))
        .withColumn("sensor_reading", (rand() * 1000))
        .drop("id")
    )

df = generate_initial_df(5000000, 10000, 50)
df = df.filter("device_id in (0,1,12,26)")
df.orderBy("device_id").show(truncate=False)

""" output
+---------+-------+------------------+
|device_id|trip_id|sensor_reading    |
+---------+-------+------------------+
|0        |25     |343.2447748562299 |
|0        |45     |872.8454455822704 |
|0        |35     |734.3414815129069 |
|0        |49     |64.48873158490986 |
|0        |20     |971.8224400017041 |
|0        |28     |406.57632335348717|
|0        |46     |132.39089817477546|
|0        |15     |654.8580194368938 |
|0        |25     |299.33117911360654|
|0        |6      |68.94179394270827 |
|0        |36     |926.3480907714563 |
|0        |12     |608.9780533114772 |
|0        |1      |554.6065427978697 |
|0        |39     |145.9549539028847 |
|0        |32     |478.52902498205486|
|0        |13     |345.6236454722018 |
|0        |38     |241.18865584676652|
|0        |14     |797.7643265975955 |
|0        |35     |573.9094429129616 |
|0        |15     |592.4690974466193 |
+---------+-------+------------------+
only showing top 20 rows
"""
```

### Normal Pyspark UDF `udf()`

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
|1        |43     |530.7896784379321 |283.41982282875625|
|12       |29     |860.0978137772574 |437.615866197445  |
|12       |20     |232.33699467935554|134.7430986996961 |
|12       |4      |986.8659204465876 |495.2589545057588 |
|26       |35     |251.14227779589214|144.51996442365015|
|26       |1      |309.73943001795067|174.54075303471242|
|12       |48     |93.73357753079259 |59.52585933755739 |
|0        |46     |868.6442327100272 |441.5274880310181 |
|26       |10     |213.72341326792267|124.98758919556596|
|1        |44     |612.0595219855699 |322.1916588634277 |
|26       |32     |210.56012149186688|123.32141813880153|
|26       |2      |364.20864840263755|201.93668788900223|
|12       |21     |968.7261169748709 |487.0582541109475 |
|26       |42     |109.40400632521141|68.41162451136303 |
|12       |23     |639.5374448859094 |335.1809902769203 |
|26       |8      |807.889310536298  |413.6344246613312 |
|26       |11     |73.26594689652165 |47.68831199640936 |
|26       |14     |75.48443523383153 |48.98596513383387 |
|1        |15     |956.479842753875  |481.5132472039963 |
|0        |0      |896.5671037710001 |454.28094442414834|
+---------+-------+------------------+------------------+
only showing top 20 rows
"""
```

### `pandas_udf()`

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
|1        |43     |530.7896784379321 |283.41982282875625|
|12       |29     |860.0978137772574 |437.615866197445  |
|12       |20     |232.33699467935554|134.7430986996961 |
|12       |4      |986.8659204465876 |495.2589545057588 |
|26       |35     |251.14227779589214|144.51996442365015|
|26       |1      |309.73943001795067|174.54075303471242|
|12       |48     |93.73357753079259 |59.52585933755739 |
|0        |46     |868.6442327100272 |441.5274880310181 |
|26       |10     |213.72341326792267|124.98758919556596|
|1        |44     |612.0595219855699 |322.1916588634277 |
|26       |32     |210.56012149186688|123.32141813880153|
|26       |2      |364.20864840263755|201.93668788900223|
|12       |21     |968.7261169748709 |487.0582541109475 |
|26       |42     |109.40400632521141|68.41162451136303 |
|12       |23     |639.5374448859094 |335.1809902769203 |
|26       |8      |807.889310536298  |413.6344246613312 |
|26       |11     |73.26594689652165 |47.68831199640936 |
|26       |14     |75.48443523383153 |48.98596513383387 |
|1        |15     |956.479842753875  |481.5132472039963 |
|0        |0      |896.5671037710001 |454.28094442414834|
+---------+-------+------------------+------------------+
only showing top 20 rows
"""
```

### `applyInPandas()`

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
```

```
+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+------------+
|device_id|trip_id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |sensor_reading|sqrt_reading|
+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+------------+
|0        |[46, 0, 17, 22, 32, 48, 45, 10, 37, 20, 45, 27, 46, 45, 1, 7, 43, 22, 9, 3, 26, 37, 24, 24, 18, 14, 3, 6, 6, 35, 10, 7, 32, 40, 35, 42, 15, 28, 25, 24, 24, 13, 14, 13, 24, 15, 48, 13, 31, 3, 38, 26, 12, 34, 38, 1, 33, 31, 3, 35, 43, 36, 29, 42, 32, 3, 25, 44, 31, 43, 24, 2, 1, 6, 7, 45, 27, 43, 24, 23, 33, 47, 15, 45, 39, 9, 41, 9, 33, 9, 43, 45, 26, 3, 17, 9, 20, 45, 29, 11, 4, 13, 2, 7, 37, 12, 33, 18, 19, 21, 6, 34, 36, 4, 11, 16, 45, 44, 16, 25, 29, 21, 3, 12, 41, 26, 44, 14, 29, 13, 23, 44, 2, 20, 47, 47, 38, 8, 20, 46, 4, 40, 44, 25, 25, 34, 24, 5, 32, 12, 20, 20, 0, 27, 9, 11, 4, 14, 9, 17, 11, 4, 34, 30, 30, 8, 42, 23, 37, 19, 17, 29, 9, 31, 26, 43, 28, 2, 29, 9, 30, 9, 21, 23, 26, 18, 32, 47, 24, 35, 40, 44, 20, 30, 28, 18, 41, 42, 41, 43, 45, 38, 3, 19, 39, 43, 3, 4, 28, 49, 36, 48, 43, 29, 4, 21, 3, 6, 2, 5, 5, 40, 20, 35, 28, 24, 31, 6, 7, 10, 20, 7, 20, 0, 29, 30, 38, 11, 42, 26, 10, 10, 0, 14, 38, 32, 33, 32, 1, 5, 11, 1, 19, 38, 46, 19, 14, 44, 2, 46, 41, 36, 42, 13, 20, 35, 3, 11, 11, 39, 38, 22, 24, 42, 42, 24, 42, 3, 43, 45, 31, 13, 15, 16, 44, 34, 3, 12, 19, 6, 6, 40, 1, 24, 6, 10, 40, 26, 10, 19, 20, 9, 13, 24, 25, 18, 11, 29, 11, 28, 44, 8, 14, 6, 20, 11, 31, 16, 13, 11, 10, 33, 44, 20, 42, 7, 4, 43, 34, 34, 8, 39, 40, 25, 22, 39, 38, 14, 29, 7, 7, 35, 40, 24, 47, 26, 44, 36, 23, 5, 22, 0, 3, 34, 15, 49, 43, 34, 35, 3, 44, 40, 12, 7, 13, 23, 34, 19, 38, 22, 27, 30, 27, 43, 30, 2, 46, 34, 31, 32, 30, 14, 6, 45, 48, 42, 11, 2, 38, 12, 3, 29, 37, 18, 0, 17, 26, 0, 9, 27, 37, 10, 4, 23, 36, 29, 36, 46, 26, 36, 40, 31, 44, 40, 41, 35, 3, 25, 40, 1, 44, 21, 11, 38, 45, 1, 2, 22, 45, 20, 11, 20, 9, 5, 18, 3, 11, 34, 48, 45, 24, 38, 24, 23, 5, 32, 8, 37, 37, 5, 24, 39, 35, 40, 28, 21, 40, 34, 29, 19, 36, 17, 30, 16, 1, 36, 25, 34, 34, 16, 28, 0, 44, 46, 35, 36, 34, 28, 39, 49, 39, 31, 33, 30, 3, 24, 8, 39, 19, 39, 4, 4, 5, 13, 27, 39, 0, 9, 37, 44, 12, 25, 12, 33, 48, 30, 41, 10, 30, 36, 21, 34, 6, 23, 3, 25, 22, 6, 2, 20, 12, 6, 19, 16, 24, 17, 7, 10, 16]|516           |272         |
|1        |[43, 44, 15, 10, 14, 27, 15, 5, 23, 13, 7, 2, 40, 10, 26, 4, 35, 22, 33, 5, 44, 45, 48, 3, 30, 2, 11, 22, 45, 37, 49, 13, 25, 21, 21, 21, 31, 20, 13, 14, 42, 48, 31, 12, 38, 47, 45, 21, 36, 18, 23, 1, 47, 22, 15, 8, 34, 0, 7, 9, 10, 8, 15, 10, 0, 15, 26, 49, 23, 49, 46, 2, 39, 33, 3, 27, 45, 11, 25, 17, 33, 49, 4, 5, 39, 27, 2, 27, 8, 7, 40, 11, 35, 2, 5, 2, 11, 6, 1, 41, 13, 23, 39, 38, 17, 0, 45, 10, 48, 34, 1, 9, 23, 47, 23, 23, 47, 35, 4, 0, 45, 31, 45, 30, 26, 12, 42, 49, 4, 12, 42, 49, 37, 46, 29, 17, 43, 33, 48, 19, 18, 4, 46, 35, 8, 45, 32, 16, 20, 6, 34, 40, 37, 14, 40, 9, 9, 2, 28, 4, 11, 27, 6, 13, 36, 30, 37, 11, 45, 35, 46, 42, 32, 47, 30, 27, 4, 42, 49, 28, 21, 35, 40, 28, 26, 24, 2, 39, 44, 9, 32, 17, 9, 44, 33, 39, 24, 49, 49, 46, 1, 24, 18, 19, 4, 46, 5, 21, 38, 17, 17, 36, 40, 16, 7, 20, 2, 7, 21, 16, 23, 12, 32, 47, 19, 5, 30, 28, 9, 22, 6, 8, 49, 26, 15, 8, 7, 29, 8, 1, 2, 46, 28, 7, 38, 2, 22, 8, 39, 16, 38, 28, 2, 24, 35, 38, 17, 22, 10, 28, 17, 7, 0, 38, 28, 32, 44, 13, 7, 35, 48, 43, 23, 20, 7, 8, 37, 47, 39, 45, 38, 9, 19, 4, 35, 46, 7, 32, 47, 8, 7, 43, 35, 48, 22, 17, 16, 49, 45, 20, 7, 0, 16, 21, 21, 11, 30, 46, 49, 15, 39, 27, 14, 26, 37, 13, 3, 32, 49, 36, 7, 12, 40, 41, 29, 37, 31, 0, 47, 11, 18, 29, 4, 28, 14, 12, 23, 19, 45, 18, 15, 16, 48, 1, 38, 48, 30, 47, 26, 1, 43, 29, 49, 10, 24, 7, 1, 34, 41, 26, 49, 35, 48, 32, 3, 40, 40, 17, 49, 29, 24, 42, 39, 43, 9, 21, 31, 38, 3, 33, 18, 20, 8, 7, 35, 15, 44, 12, 17, 20, 18, 10, 42, 0, 3, 35, 5, 6, 46, 40, 38, 28, 3, 3, 28, 2, 14, 29, 33, 47, 1, 8, 34, 22, 34, 40, 47, 38, 1, 27, 33, 9, 46, 27, 20, 20, 25, 40, 25, 37, 48, 4, 9, 33, 1, 11, 39, 14, 19, 45, 6, 31, 40, 31, 15, 1, 13, 49, 5, 46, 44, 37, 43, 24, 11, 32, 39, 47, 14, 8, 26, 25, 43, 44, 14, 16, 32, 11, 6, 12, 31, 7, 17, 45, 17, 45, 1, 4, 29, 42, 39, 18, 13, 5, 26, 45, 14, 15, 4, 32, 40, 30, 21, 11, 22, 9, 7, 46, 46, 42, 40, 22, 25, 44, 28, 8, 13]                                                                                          |507           |266         |
|12       |[29, 20, 4, 48, 21, 23, 13, 7, 24, 40, 25, 38, 5, 31, 0, 11, 0, 35, 21, 32, 11, 17, 16, 47, 13, 34, 29, 29, 21, 20, 31, 35, 5, 12, 26, 14, 34, 25, 14, 33, 27, 9, 32, 36, 20, 25, 46, 2, 18, 29, 7, 14, 27, 15, 42, 48, 12, 42, 9, 38, 1, 49, 19, 42, 49, 35, 26, 18, 22, 37, 28, 41, 16, 35, 28, 39, 46, 49, 13, 0, 41, 20, 28, 13, 48, 1, 25, 20, 49, 40, 2, 16, 21, 37, 46, 25, 35, 47, 48, 27, 28, 4, 28, 32, 33, 35, 35, 48, 11, 6, 21, 19, 42, 12, 37, 5, 37, 28, 36, 3, 9, 14, 41, 48, 8, 26, 2, 32, 14, 15, 9, 3, 42, 22, 3, 44, 23, 3, 24, 12, 28, 2, 7, 21, 47, 0, 7, 31, 19, 9, 41, 21, 4, 42, 45, 4, 38, 28, 15, 47, 23, 8, 45, 18, 32, 32, 1, 3, 43, 33, 19, 24, 8, 4, 27, 17, 28, 23, 11, 9, 11, 1, 47, 41, 44, 15, 45, 16, 29, 13, 21, 42, 23, 46, 24, 30, 32, 38, 39, 21, 19, 16, 7, 3, 26, 5, 45, 25, 29, 49, 1, 13, 27, 36, 46, 49, 13, 37, 33, 39, 8, 39, 31, 25, 11, 19, 31, 35, 42, 41, 34, 22, 6, 47, 10, 10, 45, 28, 38, 24, 11, 6, 47, 47, 29, 12, 47, 5, 45, 24, 12, 15, 34, 26, 4, 46, 38, 5, 48, 3, 45, 9, 18, 46, 20, 7, 23, 0, 24, 14, 33, 11, 38, 49, 46, 10, 31, 33, 48, 37, 26, 8, 39, 2, 30, 41, 46, 11, 39, 11, 31, 10, 20, 23, 11, 44, 35, 17, 17, 24, 18, 17, 5, 48, 7, 30, 39, 25, 22, 22, 41, 13, 27, 34, 0, 42, 40, 36, 32, 21, 37, 1, 46, 28, 8, 40, 2, 2, 31, 27, 1, 29, 1, 37, 17, 1, 30, 8, 5, 6, 17, 41, 17, 29, 0, 15, 12, 14, 16, 18, 40, 21, 25, 49, 41, 45, 37, 32, 43, 27, 10, 8, 21, 22, 7, 43, 0, 49, 17, 21, 6, 6, 6, 39, 10, 5, 15, 12, 43, 40, 2, 1, 42, 43, 17, 27, 30, 32, 10, 30, 21, 10, 48, 19, 48, 46, 8, 35, 3, 13, 32, 6, 49, 37, 8, 27, 9, 37, 5, 25, 49, 36, 26, 13, 0, 34, 40, 34, 21, 48, 48, 39, 31, 13, 28, 2, 21, 38, 4, 2, 43, 29, 35, 19, 9, 21, 16, 2, 17, 22, 40, 40, 21, 41, 7, 43, 44, 49, 20, 8, 13, 30, 4, 31, 35, 41, 36, 26, 7, 22, 49, 36, 43, 21, 35, 29, 24, 35, 28, 19, 30, 26, 25, 49, 31, 22, 33, 46, 45, 31, 5]                                                                                                                                                                              |517           |272         |
|26       |[35, 1, 10, 32, 2, 42, 8, 11, 14, 25, 17, 2, 3, 22, 16, 25, 34, 9, 40, 36, 0, 34, 13, 3, 25, 1, 47, 36, 29, 14, 14, 14, 9, 41, 44, 42, 20, 13, 34, 46, 10, 18, 22, 18, 5, 3, 11, 6, 8, 40, 35, 49, 29, 45, 43, 15, 19, 19, 37, 15, 29, 36, 47, 12, 28, 33, 17, 2, 17, 42, 29, 33, 25, 49, 34, 11, 48, 22, 17, 48, 20, 10, 49, 42, 22, 8, 19, 24, 25, 23, 18, 21, 4, 34, 34, 19, 3, 25, 45, 13, 44, 30, 24, 27, 35, 42, 15, 5, 5, 5, 45, 8, 0, 44, 30, 41, 3, 13, 41, 28, 8, 36, 35, 23, 8, 25, 31, 17, 1, 13, 4, 5, 34, 45, 18, 20, 20, 45, 29, 19, 16, 46, 18, 9, 3, 30, 15, 47, 39, 36, 36, 1, 28, 13, 9, 13, 25, 9, 16, 25, 8, 16, 35, 34, 38, 4, 39, 44, 1, 5, 20, 36, 19, 19, 32, 32, 40, 13, 26, 25, 38, 17, 3, 12, 9, 9, 48, 23, 46, 47, 24, 32, 10, 38, 16, 5, 26, 39, 29, 30, 41, 31, 29, 2, 21, 27, 3, 44, 20, 38, 2, 39, 38, 44, 47, 40, 15, 22, 34, 15, 20, 40, 17, 0, 25, 32, 11, 35, 33, 14, 28, 35, 6, 0, 12, 40, 32, 18, 7, 11, 46, 20, 8, 5, 4, 17, 18, 40, 26, 35, 3, 22, 17, 5, 22, 41, 35, 31, 22, 5, 27, 12, 8, 25, 22, 28, 35, 46, 37, 7, 40, 37, 48, 18, 45, 7, 31, 31, 36, 37, 37, 22, 29, 42, 20, 27, 14, 0, 15, 38, 48, 40, 42, 12, 27, 30, 25, 47, 33, 16, 23, 4, 10, 2, 26, 3, 34, 2, 42, 15, 36, 0, 18, 16, 2, 27, 40, 38, 10, 38, 16, 0, 26, 16, 6, 32, 39, 14, 34, 12, 11, 14, 39, 40, 35, 24, 22, 12, 22, 30, 41, 36, 5, 18, 43, 34, 4, 2, 35, 3, 44, 49, 18, 24, 36, 37, 12, 35, 43, 3, 17, 47, 27, 6, 44, 42, 4, 0, 35, 18, 9, 46, 42, 36, 28, 37, 38, 49, 12, 13, 32, 9, 40, 49, 39, 6, 30, 44, 37, 41, 49, 2, 6, 13, 36, 8, 42, 20, 37, 34, 13, 18, 45, 43, 7, 18, 36, 10, 28, 43, 31, 49, 26, 41, 49, 42, 30, 4, 41, 32, 42, 46, 22, 14, 49, 39, 8, 46, 29, 24, 5, 42, 26, 40, 43, 36, 45, 20, 35, 38, 8, 11, 47, 31, 47, 0, 3, 3, 12, 42, 14, 44, 3, 26, 16, 14, 23, 19, 24, 41, 36, 36, 31, 36, 41, 5, 37, 2, 19, 37, 18, 17, 2, 20, 0, 39, 4, 45, 19, 16, 41, 21, 10, 35, 23, 48, 29, 43, 7, 42, 15, 39, 49, 13, 20, 14, 27, 44, 24, 24, 41, 40, 8, 2, 29]                                                                                 |501           |264         |
+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+------------+
```

### `mapInPandas(self_func(Iterator[]))`
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
df.orderBy("device_id").show(truncate=False)

"""
+---------+-------+--------------+------------+
|device_id|trip_id|sensor_reading|sqrt_reading|
+---------+-------+--------------+------------+
|0        |46     |516           |272         |
|0        |0      |516           |272         |
|0        |17     |516           |272         |
|0        |22     |516           |272         |
|0        |32     |516           |272         |
|0        |48     |516           |272         |
|0        |45     |516           |272         |
|0        |10     |516           |272         |
|0        |37     |516           |272         |
|0        |20     |516           |272         |
|0        |45     |516           |272         |
|0        |27     |516           |272         |
|0        |46     |516           |272         |
|0        |45     |516           |272         |
|0        |1      |516           |272         |
|0        |7      |516           |272         |
|0        |43     |516           |272         |
|0        |22     |516           |272         |
|0        |9      |516           |272         |
|0        |3      |516           |272         |
+---------+-------+--------------+------------+
only showing top 20 rows
"""
```

#### Data example of `Iterator[pd.DataFrame]`

Example of how you might use an `Iterator[pd.DataFrame]` in practice. Let's say you have a list of pandas DataFrames, each representing a chunk of data. You can create an iterator over these DataFrames and process them one by one.

```python
import pandas as pd

# Create sample DataFrames
df1 = pd.DataFrame({
    'device_id': [1, 2],
    'trip_id': [[101, 102], [103, 104]],
    'sensor_reading': [100, 200],
    'sqrt_reading': [10, 14]
})
df1.display()

df2 = pd.DataFrame({
    'device_id': [3, 4],
    'trip_id': [[105, 106], [107, 108]],
    'sensor_reading': [300, 400],
    'sqrt_reading': [17, 20]
})
df2.display()

# Create an iterator over the DataFrames
dataframes = iter([df1, df2])
for df in dataframes:
    print(df)
dataframes = iter([df1, df2])

"""
df1:
   device_id     trip_id  sensor_reading  sqrt_reading
0          1  [101, 102]             100            10
1          2  [103, 104]             200            14

df2:
   device_id     trip_id  sensor_reading  sqrt_reading
0          3  [105, 106]             300            17
1          4  [107, 108]             400            20
"""
```

Use the `renormalize` function you provided to process these DataFrames:

```python
from collections.abc import Iterator

def renormalize(itr: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for pdf in itr:
        # Unpack the list of values from the trip_id column into their own rows
        pdf = pdf.explode('trip_id')
        yield pdf

# Apply the renormalize function
processed_dfs = renormalize(dataframes)

# Iterate over the processed DataFrames and print them
for df in processed_dfs:
    print(df)

"""
The output will be:
   device_id  trip_id  sensor_reading  sqrt_reading
0          1      101             100            10
0          1      102             100            10
1          2      103             200            14
1          2      104             200            14
   device_id  trip_id  sensor_reading  sqrt_reading
0          3      105             300            17
0          3      106             300            17
1          4      107             400            20
1          4      108             400            20
"""
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