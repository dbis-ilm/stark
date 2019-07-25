package dbis.stark.raster

import java.nio.file.{Files, Paths}

import dbis.stark.{STObject, StarkTestUtils}
import dbis.stark.StarkTestUtils.makeTimeStamp
import dbis.stark.spatial.STSparkContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.raster.{BucketUDT, TileUDT}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable

class RasterTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  private var sc: SparkContext = _

  override def beforeAll() {
    sc = StarkTestUtils.createSparkContext("spatialrasterrddtestcase")
  }

  override def afterAll() {
    if(sc != null)
      sc.stop()
  }

  ignore should "load files into a tile RDD" in {
    val spark = dbis.stark.sql.STARKSession.builder()
      .appName("STARK Web UI")
      .master("local")
      .getOrCreate()

    val rowRDD = spark.read.option("header", false.toString).option("delimiter", ";").csv(s"file:///home/timo/Tmp/Raster_Tile/raster_1_5.csv").rdd.map{row =>
      val ulx = row.getString(0).toDouble
      val uly = row.getString(1).toDouble
      val w = row.getString(2).toInt
      val h = row.getString(3).toInt
      val pw = row.getString(4).toDouble

      val rawData = row.getString(5).split(",")
      val data = new Array[Double](rawData.length)
      var i = 0
      while(i < data.length){
        data(i) = rawData(i).toDouble

        i += 1
      }

      Row(Tile[Double](ulx,uly,w,h,data,pw))
    }

    val df = spark.createDataFrame(rowRDD, StructType(Seq(StructField("tile", TileUDT, nullable = false))))
    df.createOrReplaceTempView("raster")

    //spark.sql("SELECT rasterHistogram(*) from raster").show()
    //val rows = spark.sql("SELECT histogram(tile, 10) from raster").collect()
    val rows = spark.sql("SELECT rasterHistogram(*) from raster").collect()
    rows.foreach(row => {
      val arr = row.getAs[mutable.WrappedArray[Bucket[Any]]](0)
      arr.foreach(el => {
        println(el)
      })
    })
  }
}