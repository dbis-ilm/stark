package dbis.stark.raster

import java.nio.file.{Files, Paths}

import dbis.stark.{STObject, StarkTestUtils}
import dbis.stark.StarkTestUtils.makeTimeStamp
import dbis.stark.spatial.STSparkContext
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class RasterTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  private var sc: SparkContext = _

  override def beforeAll() {
    sc = StarkTestUtils.createSparkContext("spatialrasterrddtestcase")
  }

  override def afterAll() {
    if(sc != null)
      sc.stop()
  }

  it should "load files into a tile RDD" in {
//    val spc = new STSparkContext(sc)
//
//    val rdd = spc.tileFiles("C:/Users/Timo/Desktop/CreateTile/Tiles"/*, "prefix", 60, 60, 720*/)
//
////    for(t <- rdd) {
////      println(t)
////    }
//
//    val tl = rdd.map(_.data.length).sum().toInt
//    println(s"Total tile data size: $tl")
  }
}