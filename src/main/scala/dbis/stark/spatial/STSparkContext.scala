package dbis.stark.spatial

import java.awt.image.BufferedImage
import java.nio.ByteBuffer
import java.nio.file.Paths

import dbis.stark.raster.{RasterRDD, RasterUtils, SMA, Tile}
import dbis.stark.{Instant, Interval, STObject}
import javax.imageio.ImageIO
import org.apache.hadoop.fs.Path
import org.apache.spark.SpatialRDD._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag


object STSparkContext {
  final val PARTITIONINFO_FILE = "partition_info"
  final val PARTITIONINFO_DELIM = ";"
}

/**
  * A spatio-temporal aware context
  *
  */
class STSparkContext(conf: SparkConf) extends SparkContext(conf) {

  def this() = this(new SparkConf())

  // a representation of the filesystem
  private val dfs = org.apache.hadoop.fs.FileSystem.get(hadoopConfiguration)

  /**
    * Load a text file and decide which partitions to load based on the given query object
    *
    * If the given path is a directory which contains an info file, we will use this info file
    * to decide which partition we have to load. If this file is not present, all partitions are loaded
    * @param path The base path
    * @param qry The query object used to decide which partition to load
    * @return Returns the [[org.apache.spark.rdd.RDD]]
    */
  def textFile(path: String, qry: STObject, minPartitions: Int): RDD[String] = {
    val partitionsToLoad: String = getPartitionsToLoad(path, qry)

    if(partitionsToLoad.isEmpty)
      this.emptyRDD[String]
    else
      super.textFile(partitionsToLoad, minPartitions)
  }

  def objectFile[T : ClassTag](path: String, qry: STObject, minPartitions: Int): RDD[T] = {

    val partitionstoLoad = getPartitionsToLoad(path, qry)

    if(partitionstoLoad.isEmpty)
      emptyRDD[T]
    else
      super.objectFile[T](partitionstoLoad, minPartitions)
  }

  def tileFile(file: String, partitions: Int, qry: Option[STObject]): RasterRDD[Double] = {

    val raw = qry match {
      case None =>
        super.textFile(file, partitions)
      case Some(so) =>
        this.textFile(file, so, partitions)
    }

    raw.map{ line =>
      val arr = line.split(',')
      val ulx = arr(0).toDouble
      val uly = arr(1).toDouble

      val tWidth = arr(2).toInt
      val tHeight = arr(3).toInt

      val pw = arr(4).toDouble
//      require(tWidth * tHeight == arr.length - 4, s"w*h = ${tWidth * tHeight} != ${arr.length - 4}")

      val data = new Array[Double](arr.length - 5)

//      Array.copy(arr, 4, data, 0, arr.length - 4)

      var i = 0
      while(i < data.length) {
        data(i) = arr(i+5).toDouble
        i += 1
      }

      Tile[Double](ulx, uly, tWidth, tHeight, data, pw)
    }

//    val dir = new File(folderPath)
//    var fileList = List[File]()
//    if(dir.exists() && dir.isDirectory) {
//      fileList = dir.listFiles.filter(_.isFile).toList
//    }
//
//    var uplx = 0
//    var uply = 0
//    var tileList = List[Tile[Double]]()
//
//    for(f <- fileList) {
//      if(f.getName.startsWith(filePrefix)) {
//        val data = Source.fromFile(f)
//          .getLines().flatMap(_.split(",").map(_.toDouble))
//          .toArray
//
//        tileList ::= Tile[Double](uplx, uply + tileHeight, tileWidth, tileHeight, data)
//        if(uplx + tileWidth >= totalWidth) {//for now tiles will be loaded row by row
//          uplx = 0
//          uply += tileHeight
//        } else {
//          uplx += tileWidth
//        }
//      }
//    }
//
//    sc.parallelize(tileList)
  }

  def objectTiles(file: String, partitions: Int = 1024, query: Option[STObject] = None): RasterRDD[Double] = {
    val byteRDD = query match {
      case None =>
        super.objectFile[(STObject, Array[Byte])](file, partitions)
      case Some(so) =>
        this.objectFile[(STObject, Array[Byte])](file,so,partitions)
    }

    byteRDD.map{ case (_, arr) =>
      val buf = ByteBuffer.wrap(arr)
      val ulx = buf.getDouble
      val uly = buf.getDouble
      val width = buf.getInt
      val height = buf.getInt
      val pw = buf.getDouble

      val numValues = buf.getInt
      val values = new Array[Double](numValues)
      var i = 0
      while(i < numValues) {
        values(i) = buf.getDouble
        i += 1
      }

      Tile(ulx, uly, width, height, values, pw)
    }
  }

  /**
    * Loads a raster data set where each tile is stored in its own file and parameters are encoded in file name
    * @param folderPath The folder containing all tile files
    * @param imageReader A function converting the image of a tile into a data array (grey scale, RGB, ...).E.g. [[dbis.stark.raster.RasterUtils.greyScaleImgToUnsignedByteArray]]
    * @return Returns a [[RasterRDD[Byte]] containing all tiles
    */
  def loadNanoFiles(folderPath: String, imageReader: BufferedImage => Array[Array[Int]] = RasterUtils.greyScaleImgToUnsignedByteArray): RasterRDD[Byte] =

    super.binaryFiles(folderPath)
      .map{case(name, data) => (name.split("_"),data)}
      .filter(_._1.length == 10)
      .map{case(nameEncodedData, binaryData) =>
        val str = binaryData.open()
        val img = ImageIO.read(str)
        str.close()

        val data = imageReader(img)


        val xCount = nameEncodedData(0)//Counter of Image x-axes
        val yCount = nameEncodedData(1)
//        val zCount = nameEncodedData(2)
//        val xCoord = nameEncodedData(3)//x-coordinate of image creation position (ul?)
//        val yCoord = nameEncodedData(4)
//        val zCoord = nameEncodedData(5)
//        val pixelDeltaX = nameEncodedData(6)//distance between pixels in x-coordinate
//        val pixelDeltaY = nameEncodedData(7)
//        val pixelDeltaZ = nameEncodedData(8)

        val flatData = data.flatten
        val sma = SMA(flatData(0).toByte, flatData(0).toByte, 0.0)
        var i = 1

        val byteData = new Array[Byte](flatData.length)
        byteData(0) = flatData(0).toByte

        while(i < flatData.length) {
          val b = flatData(i).toByte
          if(b < sma.min)
            sma.min = b
          else if(b > sma.max)
            sma.max = b

          byteData(i) = b
          i += 1
        }

        // TODO what is -100 here? make it generic
        Tile[Byte]((xCount.toDouble - 100) * img.getWidth(),
          ((yCount.toDouble - 100) + 1) * img.getHeight(),
          img.getWidth(), img.getHeight(), byteData, sma = Some(sma))//this converts to signed again
      }

  private[stark] def getPartitionsToLoad(path: String, qry: STObject) = {
    val p = Paths.get(path)
    // the path to the info file
    val infoFile = p.resolve(STSparkContext.PARTITIONINFO_FILE).toString

    val partitionsToLoad = if (dfs.isDirectory(new Path(path)) && dfs.exists(new Path(infoFile))) {
      super.textFile(infoFile) // load info file
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
