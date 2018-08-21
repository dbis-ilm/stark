package dbis.stark.spatial

import java.nio.file.Paths

import dbis.stark.{Instant, Interval, STObject}
import org.apache.spark.SpatialRDD._
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object STSparkContext {
  final val PARTITIONINFO_FILE = "partition_info"
  final val PARTITIONINFO_DELIM = ";"
}

/**
  * A spatio-temporal aware context
  *
  * @param sc The original [[org.apache.spark.SparkContext]]
  */
class STSparkContext(private val sc: SparkContext) {

  // a representation of the filesystem
  private val dfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)

  /**
    * Load a text file and decide which partitions to load based on the given query object
    *
    * If the given path is a directory which contains an info file, we will use this info file
    * to decide which partition we have to load. If this file is not present, all partitions are loaded
    * @param path The base path
    * @param qry The query object used to decide which partition to load
    * @return Returns the [[org.apache.spark.rdd.RDD]]
    */
  def textFile(path: String, qry: STObject): RDD[String] = {
    val partitionsToLoad: String = getPartitionsToLoad(path, qry)

    if(partitionsToLoad.isEmpty)
      sc.emptyRDD[String]
    else
      sc.textFile(partitionsToLoad)
  }

  def objectFile[T : ClassTag](path: String, qry: STObject): RDD[T] = {
    val partitionstoLoad = getPartitionsToLoad(path, qry)

    if(partitionstoLoad.isEmpty)
      sc.emptyRDD[T]
    else
      sc.objectFile(partitionstoLoad)
  }

  private[stark] def getPartitionsToLoad(path: String, qry: STObject) = {
    val p = Paths.get(path)

    // the path to the info file
    val infoFile = p.resolve(STSparkContext.PARTITIONINFO_FILE).toString

    val partitionsToLoad = if (dfs.isDirectory(new Path(path)) && dfs.exists(new Path(infoFile))) {
      sc.textFile(infoFile) // load info file
        .map(_.split(STSparkContext.PARTITIONINFO_DELIM))
        .map { arr =>
          val stobj = {
            val interval = if (arr(1).isEmpty)
              None
            else {
              if (arr(2).isEmpty)
                Some(Interval(Instant(arr(1).toLong), None))
              else
                Some(Interval(Instant(arr(1).toLong), Instant(arr(2).toLong)))
            }

            if (interval.isDefined)
              STObject(arr(0), interval.get)
            else
              STObject(arr(0))
          }

          (stobj, arr(3))
        } // (STObject, path)
        .intersects(qry) // find relevant partitions
        .map { case (_, file) => Paths.get(path, file).toString } // only path
        .collect() // fetch into single array
        .mkString(",") // make comma separated string for SparkContext#textFile

    } else // if it's not possible to load info file, load everything under this path
      path

    partitionsToLoad
  }
}
