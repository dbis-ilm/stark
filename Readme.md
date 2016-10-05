# STARK - **S**patio-**T**emporal Data Analytics on Sp**ark**

STARK is a framework that tightly integrates with [Apache Spark](https://spark.apache.org) and add support for spatial and temporal data types and operations.

## Installation

Using STARK is very simple. Just clone the repository and build the assembly file with
```
sbt assembly
```

The resulting assembly file can be found in `$STARKDIR/target/scala-2.11/stark.jar`. Just add this library to your classpath and STARK is ready to use.

## Usage

STARK uses the `STObject` class as its main data structure. This class can hold your spatio-temporal data. STARK also has [_implicit_ conversion ](http://docs.scala-lang.org/tutorials/tour/implicit-conversions) methods that add spatio-temporal operations to your RDD of `STObject` .

To be able to use the spatio-temporal operations, you have to have an Pair-RDD where the `STObject` is in first position:

```Scala
import dbis.stark._
import dbis.stark.spatial.SpatialRDD._

val countries = sc.textFile("/data/countries") // assume Schema ID;Name;WKT String
    .map(line => line.split(';'))
    .map(arr => (STObject(arr(2)), (arr(0).toInt, arr(1)) ) // ( STObject, (Int, String) )

// find all geometries that contain the given point    
val filtered = countries.contains(STObject("POINT( 50.681898, 10.938838 )"))
```

## Features

### Spatial Partitioning
  * __TODO__


### Indexing
  * __TODO__

### Operators
  * __TODO__
