package dbis.stark

import java.io.File
import java.time.LocalDate

import dbis.stark.spatial.SpatialRDD._
import dbis.stark.spatial.partitioner.BSPartitioner
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object TestUtils {
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

  def createSparkContext(name: String, parallel: Int = 4) = {
    val conf = new SparkConf().setMaster(s"local[$parallel]").setAppName(name)
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
      TestUtils.distinct(rdd).repartition(numParts)
    else
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
    
    val rdd = TestUtils.createRDD(sc, file, sep, numParts, distinct)
    rdd.index(new BSPartitioner(rdd, cellSize, cost), order)
  } 
  
  def distinct[V](rdd: RDD[(STObject, V)]) = {
    
    val set = scala.collection.mutable.Set.empty[STObject]
    
    rdd.filter{ case (st, value) => 
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