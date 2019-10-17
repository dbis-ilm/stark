package dbis.stark

import java.io.File
import java.nio.file.{Files, Path}
import java.time.LocalDate

import dbis.stark.spatial.partitioner.{BSPStrategy, PartitionerFactory}
import org.apache.spark.SpatialRDD._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.Tag

import scala.collection.JavaConverters._

object Fix extends Tag("dbis.stark.Fix")

object StarkTestUtils {
  def createIntervalRDD(
                         sc: SparkContext,
                         file: String = "src/test/resources/intervaltest.csv",
                         sep: Char = ';',
                         numParts: Int = 8,
                         distinct: Boolean = false) = {

    val rdd = sc.textFile(file, if(distinct) 1 else numParts) // let's start with only one partition and repartition later
      .map { line => line.split(sep) }
      .map { arr =>
        (arr(0), STObject(arr(1),Interval(arr(2).toInt, arr(3).toInt))) }
      .keyBy( _._2)

    if(distinct)
      StarkTestUtils.distinct(rdd).repartition(numParts)
    else
      rdd
  }

  case class FileOperationError(msg: String) extends RuntimeException(msg)

  def rmrf(root: String): Unit = rmrf(new File(root))

  def rmrf(root: File): Unit = {
    if (root.isFile) root.delete()
    else if (root.exists) {
      root.listFiles.foreach(rmrf)
      root.delete()
    }
  }

  def rm(file: String): Unit = rm(new File(file))

  def rm(file: File): Unit =
    if (!file.delete) throw FileOperationError(s"Deleting $file failed!")

  def mkdir(path: String): Unit = new File(path).mkdirs

  def toHuman(bytes: Long): String = {
    val names = Array("Bytes", "KiB", "MiB", "GiB", "TB")
    var idx = 0
    var b = bytes
    while(b >= 1024 && idx < names.length) {
      b /= 1024
      idx += 1
    }

    s"$b ${names(idx)}"
  }

  def du(path: Path): Long =
    Files.walk(path).iterator().asScala.map(Files.size).sum

  def createSparkContext(name: String, parallel: Int = 4) = {
    val conf = new SparkConf().setMaster(s"local[$parallel]").setAppName(name)
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[StarkKryoRegistrator].getName)
    new SparkContext(conf)
  }

  def load(sc: SparkContext, path: String, delim: String = ";") = sc.textFile(path).map(_.split(delim)).map{ arr => (STObject(arr(1)), arr(0))}



  def createRDD(
      sc: SparkContext, 
      file: String = "src/test/resources/new_eventful_flat_1000.csv", 
      sep: Char = ',', 
      numParts: Int = 4, 
      distinct: Boolean = false) = {
    
    val rdd = sc.textFile(file, if(distinct) 1 else numParts) // let's start with only one partition and repartition later
      .map { line => line.split(sep) }
      .map { arr => 
        val ts = makeTimeStamp(arr(1).toInt, arr(2).toInt, arr(3).toInt)
        // we don't add the ts directly to STObject to allow tests without a temporal component
        (arr(0), ts , s"${arr.slice(1, 4).mkString("-")}", STObject(arr(7))) } 
      .keyBy( _._4)
      
    if(distinct)
      StarkTestUtils.distinct(rdd).repartition(numParts)
    else
      rdd
  }

  def createPointRDD(
                 sc: SparkContext,
                 file: String = "src/test/resources/points.csv",
                 sep: Char = ';') = {

    val rdd = sc.textFile(file) // let's start with only one partition and repartition later
      .map { line => line.split(sep) }
      .map { arr =>
        (STObject(arr(0).toDouble, arr(1).toDouble, 0), arr(0).toDouble) }

      rdd
  }

  def createPolyRDD(
                     sc: SparkContext,
                     file: String = "src/test/resources/poly.csv",
                     sep: Char = ';') = {

    val rdd = sc.textFile(file) // let's start with only one partition and repartition later
      .map { line => line.split(sep) }
      .map { arr =>
        (STObject(arr(0), 0), 0) }

    rdd
  }
  
  def makeTimeStamp(year: Int, month: Int, day: Int) = LocalDate.of(year, month, day).toEpochDay
  
  
  def createIndexedRDD(
      sc: SparkContext, 
      distinct: Boolean = false, 
      cost: Int = 10, 
      cellSize: Double = 1,
      file: String = "src/test/resources/new_eventful_flat_1000.csv", 
      sep: Char = ',', 
      numParts: Int = 4,
      order: Int = 10) = {
    
    val rdd = StarkTestUtils.createRDD(sc, file, sep, numParts, distinct)
    val pc = BSPStrategy(cellSize,cost,pointsOnly = true)
    val parti = PartitionerFactory.get(pc, rdd).get
    rdd.index(parti, order)
  }

  def timing[R](name: String)(block: => R) = {
    val start = System.currentTimeMillis()
    val res = block
    val end = System.currentTimeMillis()
    println(s"$name took ${end - start} ms")
    res
  }
  
  def distinct[V](rdd: RDD[(STObject, V)]) = {
    
    val set = scala.collection.mutable.Set.empty[STObject]
    
    rdd.filter{ case (st, _) =>
      val contains = set.contains(st)
      if(contains)
        false
      else {
        set += st
        true
      }
    }
  }
}
